//! Создание memfd, mmap и FD-passing через SCM_RIGHTS.

use anyhow::Result;
use nix::sys::memfd::{memfd_create, MemFdCreateFlag};
use nix::sys::mman::{mmap, MapFlags, ProtFlags};
use nix::sys::socket::{recvmsg, sendmsg, ControlMessage, ControlMessageOwned, MsgFlags};
use std::ffi::CString;
use std::io::{self, IoSlice, IoSliceMut};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use tokio::io::Interest;
use tokio::net::UnixStream;

/// Создать анонимный memfd нужного размера. CLOEXEC по умолчанию.
pub fn create_memfd(size: usize) -> Result<OwnedFd> {
    let name = CString::new("shm-mpsc-bench")?;
    let fd = memfd_create(&name, MemFdCreateFlag::MFD_CLOEXEC)?;
    nix::unistd::ftruncate(&fd, size as i64)?;
    Ok(fd)
}

/// Размер файла за fd через fstat. Используется продюсером, чтобы знать,
/// сколько байт mmap'ить, ещё не зная геометрии кольца.
pub fn file_size(fd: BorrowedFd<'_>) -> Result<usize> {
    let st = nix::sys::stat::fstat(fd.as_raw_fd())?;
    Ok(st.st_size as usize)
}

/// Замапить fd в адресное пространство как RW SHARED.
///
/// # Safety
/// `fd` должен ссылаться на корректный файл размера >= `size`.
/// Возвращаемый указатель остаётся валидным, пока сегмент не будет размаплен.
pub unsafe fn map(fd: BorrowedFd<'_>, size: usize) -> Result<*mut u8> {
    let p = mmap(
        None,
        NonZeroUsize::new(size).unwrap(),
        ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
        MapFlags::MAP_SHARED,
        fd,
        0,
    )?;
    Ok(p.as_ptr() as *mut u8)
}

/// Послать `marker` + `fd` в сокет через sendmsg + SCM_RIGHTS.
///
/// Tokio держит сокет в non-blocking режиме, поэтому ждём writable
/// и повторяем sendmsg, пока он не уйдёт без EAGAIN.
pub async fn send_fd(
    stream: &UnixStream,
    fd: BorrowedFd<'_>,
    marker: &[u8],
) -> Result<()> {
    let raw = stream.as_raw_fd();
    let fd_raw = fd.as_raw_fd();
    stream
        .async_io(Interest::WRITABLE, || {
            let iov = [IoSlice::new(marker)];
            let cmsg = [ControlMessage::ScmRights(std::slice::from_ref(&fd_raw))];
            sendmsg::<()>(raw, &iov, &cmsg, MsgFlags::empty(), None)
                .map(|_| ())
                .map_err(errno_to_io)
        })
        .await?;
    Ok(())
}

/// Получить байты + fd через recvmsg. Аналогично — async по readable.
pub async fn recv_fd(stream: &UnixStream) -> Result<(Vec<u8>, OwnedFd)> {
    let raw = stream.as_raw_fd();
    let result = stream
        .async_io(Interest::READABLE, || {
            let mut buf = vec![0u8; 64];
            let mut iov = [IoSliceMut::new(&mut buf)];
            let mut cspace = nix::cmsg_space!([RawFd; 1]);
            let msg = recvmsg::<()>(raw, &mut iov, Some(&mut cspace), MsgFlags::empty())
                .map_err(errno_to_io)?;
            let fd = msg
                .cmsgs()
                .map_err(errno_to_io)?
                .find_map(|c| match c {
                    ControlMessageOwned::ScmRights(fds) => fds.first().copied(),
                    _ => None,
                })
                .ok_or_else(|| io::Error::other("no fd in cmsg"))?;
            let n = msg.bytes;
            buf.truncate(n);
            Ok((buf, fd))
        })
        .await?;
    let (buf, fd) = result;
    Ok((buf, unsafe { OwnedFd::from_raw_fd(fd) }))
}

fn errno_to_io(e: nix::errno::Errno) -> io::Error {
    if e == nix::errno::Errno::EAGAIN || e == nix::errno::Errno::EWOULDBLOCK {
        io::ErrorKind::WouldBlock.into()
    } else {
        io::Error::from_raw_os_error(e as i32)
    }
}
