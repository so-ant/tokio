#![warn(rust_2018_idioms)]
#![cfg(all(feature = "full", target_os = "linux"))]

//! A message on a socket's error queue makes epoll report `EPOLLERR`. With
//! `EPOLLOUT` that is reported as write-closed, which must not stop a writer
//! from sleeping when the socket buffer is full, and must not stop it from
//! being woken by a real close.

use tokio::io::{AsyncReadExt, Interest};
use tokio::net::unix::pipe;
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::time::{sleep, timeout};

use std::io;
use std::os::fd::AsRawFd;
use std::time::Duration;

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, server) = tokio::join!(TcpStream::connect(addr), listener.accept());
    (client.unwrap(), server.unwrap().0)
}

/// Puts one message on `client`'s error queue: the completion of a
/// `MSG_ZEROCOPY` send. `false` if this kernel or process cannot do that.
async fn queue_a_completion(client: &TcpStream, server: &mut TcpStream) -> bool {
    let one: libc::c_int = 1;
    let ret = unsafe {
        libc::setsockopt(
            client.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ZEROCOPY,
            (&one as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret != 0 {
        // Linux before 4.14.
        return false;
    }

    let payload = vec![1u8; 64 * 1024];
    let mut sent = 0;
    while sent < payload.len() {
        client.writable().await.unwrap();
        let res = client.try_io(Interest::WRITABLE, || {
            socket2::SockRef::from(client)
                .send_with_flags(&payload[sent..], libc::MSG_ZEROCOPY | libc::MSG_NOSIGNAL)
        });
        match res {
            Ok(n) => sent += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            // The locked-memory limit is too low to pin the buffer.
            Err(e) if e.raw_os_error() == Some(libc::ENOBUFS) => return false,
            Err(e) => panic!("{e}"),
        }
    }
    let mut got = vec![0u8; payload.len()];
    server.read_exact(&mut got).await.unwrap();

    // The kernel still reads `payload` until the completion is queued.
    client.ready(Interest::ERROR).await.unwrap();
    true
}

fn read_error_queue(stream: &TcpStream) {
    let mut control = [0u64; 16];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = std::mem::size_of_val(&control) as _;
    let n = unsafe {
        libc::recvmsg(
            stream.as_raw_fd(),
            &mut msg,
            libc::MSG_ERRQUEUE | libc::MSG_DONTWAIT,
        )
    };
    assert!(n >= 0, "{}", io::Error::last_os_error());
}

/// Writes until the socket buffer is full.
fn fill(stream: &TcpStream) {
    let chunk = vec![0u8; 256 * 1024];
    loop {
        match stream.try_write(&chunk) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
            Err(e) => panic!("{e}"),
        }
    }
}

/// How often `writable()` resolves in 300 ms while the send buffer is full
/// and the peer reads nothing. The loop is bounded because a writer that
/// never sleeps never lets the timeout run either.
async fn wakeups_with_a_full_buffer(stream: &TcpStream) -> usize {
    let mut wakeups = 0;
    let _ = timeout(Duration::from_millis(300), async {
        while wakeups < 10_000 {
            stream.writable().await.unwrap();
            match stream.try_write(&[0u8; 1024]) {
                // The peer's window can still open a little.
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => wakeups += 1,
                Err(e) => panic!("{e}"),
            }
        }
    })
    .await;
    wakeups
}

#[tokio::test]
async fn writer_sleeps_after_an_error_queue_message() {
    let (client, mut server) = tcp_pair().await;
    if !queue_a_completion(&client, &mut server).await {
        return;
    }
    read_error_queue(&client);
    fill(&client);
    let wakeups = wakeups_with_a_full_buffer(&client).await;
    assert!(wakeups <= 2, "writable() resolved {wakeups} times");
}

/// The same with the message left on the queue, so that `EPOLLERR` stays
/// asserted.
#[tokio::test]
async fn writer_sleeps_with_an_unread_error_queue_message() {
    let (client, mut server) = tcp_pair().await;
    if !queue_a_completion(&client, &mut server).await {
        return;
    }
    fill(&client);
    let wakeups = wakeups_with_a_full_buffer(&client).await;
    assert!(wakeups <= 2, "writable() resolved {wakeups} times");
}

/// `async_io` does not yield while its closure returns `WouldBlock` and the
/// resource stays ready, so here the bug is a task that never yields.
#[tokio::test]
async fn async_io_sleeps_after_an_error_queue_message() {
    let (client, mut server) = tcp_pair().await;
    if !queue_a_completion(&client, &mut server).await {
        return;
    }
    read_error_queue(&client);
    fill(&client);

    let mut attempts = 0;
    let wrote = timeout(
        Duration::from_millis(300),
        client.async_io(Interest::WRITABLE, || {
            attempts += 1;
            assert!(attempts < 10_000, "the write was retried without sleeping");
            socket2::SockRef::from(&client).send(&[0u8; 1024])
        }),
    )
    .await;
    // The peer's window can still open a little.
    assert!(wrote.is_err() || attempts <= 2);
    assert!(attempts <= 2, "the write was attempted {attempts} times");
}

/// A writer that went to sleep after `WRITE_CLOSED` was cleared is still
/// woken by a real close. The delay decides whether the reset lands before
/// or after the writer sleeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sleeping_writer_sees_a_reset() {
    for i in 0..40u64 {
        let (client, mut server) = tcp_pair().await;
        if !queue_a_completion(&client, &mut server).await {
            return;
        }
        read_error_queue(&client);

        let chunk = vec![0u8; 256 * 1024];
        let delay_us = if i % 2 == 0 { 20_000 } else { (i * 53) % 2000 };
        let wrote = timeout(Duration::from_secs(5), async {
            let reset = async {
                sleep(Duration::from_micros(delay_us)).await;
                server.set_zero_linger().unwrap();
                drop(server);
            };
            let write = async {
                let mut full = 0;
                while full < 100_000 {
                    client.writable().await.unwrap();
                    match client.try_write(&chunk) {
                        Ok(_) => {}
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => full += 1,
                        Err(e) => return Some(e),
                    }
                }
                None
            };
            tokio::join!(reset, write).1
        })
        .await;
        match wrote {
            Ok(Some(_)) => {}
            Ok(None) => panic!("round {i}: the writer spun instead of sleeping"),
            Err(_) => panic!("round {i}: the writer was not woken"),
        }
    }
}

/// A pipe with no reader is the case the write-closed report exists for: a
/// writer asleep on a full pipe is woken when the reader goes away.
#[tokio::test]
async fn sleeping_pipe_writer_sees_the_reader_close() {
    let (tx, rx) = pipe::pipe().unwrap();
    let chunk = vec![0u8; 64 * 1024];
    loop {
        match tx.try_write(&chunk) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("{e}"),
        }
    }
    let wrote = timeout(Duration::from_secs(5), async {
        let close = async {
            sleep(Duration::from_millis(20)).await;
            drop(rx);
        };
        let write = async {
            loop {
                tx.writable().await.unwrap();
                match tx.try_write(&chunk) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return e,
                }
            }
        };
        tokio::join!(close, write).1
    })
    .await
    .expect("the writer is woken");
    assert_eq!(wrote.kind(), io::ErrorKind::BrokenPipe);
}

/// The same for a Unix stream socket whose peer goes away.
#[tokio::test]
async fn sleeping_unix_writer_sees_the_peer_close() {
    let (a, b) = UnixStream::pair().unwrap();
    let chunk = vec![0u8; 64 * 1024];
    loop {
        match a.try_write(&chunk) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("{e}"),
        }
    }
    let wrote = timeout(Duration::from_secs(5), async {
        let close = async {
            sleep(Duration::from_millis(20)).await;
            drop(b);
        };
        let write = async {
            loop {
                a.writable().await.unwrap();
                match a.try_write(&chunk) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return e,
                }
            }
        };
        tokio::join!(close, write).1
    })
    .await
    .expect("the writer is woken");
    assert!(matches!(
        wrote.kind(),
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
    ));
}
