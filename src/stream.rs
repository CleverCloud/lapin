use crate::{tcp, Error, Result};
use std::{
    convert::TryFrom,
    fmt,
    io::{self, IoSlice, IoSliceMut, Read, Write},
};

pub struct TcpStream(Inner);

enum Inner {
    Connected(tcp::TcpStream),
    Handshaking(Option<tcp::MidHandshakeTlsStream>),
}

impl TryFrom<tcp::HandshakeResult> for TcpStream {
    type Error = Error;

    fn try_from(result: tcp::HandshakeResult) -> Result<Self> {
        Ok(Self(match result {
            Ok(stream) if stream.is_connected() => Inner::Connected(stream),
            Ok(stream) => Inner::Handshaking(Some(stream.into())),
            Err(handshaker) => {
                Inner::Handshaking(Some(handshaker.into_mid_handshake_tls_stream()?))
            }
        }))
    }
}

impl TcpStream {
    pub(crate) fn inner(&self) -> &tcp::TcpStream {
        match self.0 {
            Inner::Connected(ref stream) => stream,
            Inner::Handshaking(ref handshaker) => handshaker.as_ref().unwrap().get_ref(),
        }
    }

    pub(crate) fn inner_mut(&mut self) -> &mut tcp::TcpStream {
        match self.0 {
            Inner::Connected(ref mut stream) => stream,
            Inner::Handshaking(ref mut handshaker) => handshaker.as_mut().unwrap().get_mut(),
        }
    }

    pub(crate) fn is_handshaking(&self) -> bool {
        if let Inner::Handshaking(_) = self.0 {
            true
        } else {
            false
        }
    }

    pub(crate) fn handshake(&mut self) -> Result<()> {
        if let Inner::Handshaking(ref mut handshaker) = self.0 {
            match handshaker.take().unwrap().handshake() {
                Ok(stream) => self.0 = Inner::Connected(stream),
                Err(error) => {
                    self.0 = Inner::Handshaking(Some(error.into_mid_handshake_tls_stream()?))
                }
            }
        }
        Ok(())
    }
}

macro_rules! fwd_impl {
    ($self:ident, $method:ident, $($args:expr),*) => {
        match $self.0 {
            Inner::Connected(ref mut inner) => inner.$method($($args),*),
            Inner::Handshaking(ref mut inner) => inner.as_mut().unwrap().get_mut().$method($($args),*),
        }
    };
}

impl Read for TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        fwd_impl!(self, read, buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        fwd_impl!(self, read_vectored, bufs)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        fwd_impl!(self, read_to_end, buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        fwd_impl!(self, read_to_string, buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        fwd_impl!(self, read_exact, buf)
    }
}

impl Write for TcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        fwd_impl!(self, write, buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        fwd_impl!(self, flush,)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        fwd_impl!(self, write_vectored, bufs)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        fwd_impl!(self, write_all, buf)
    }

    fn write_fmt(&mut self, fmt: fmt::Arguments<'_>) -> io::Result<()> {
        fwd_impl!(self, write_fmt, fmt)
    }
}
