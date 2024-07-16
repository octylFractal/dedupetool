use std::io::{IoSliceMut, Read};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use pin_project_lite::pin_project;

pin_project! {
    pub struct TokioFuturesIo {
        #[pin]
        file: tokio::fs::File,
        std_file: std::fs::File,
    }
}

impl TokioFuturesIo {
    pub async fn new(file: tokio::fs::File) -> Self {
        let std_file = file
            .try_clone()
            .await
            .expect("failed to clone file")
            .into_std()
            .await;
        Self { file, std_file }
    }
}

impl futures::io::AsyncRead for TokioFuturesIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        // delegate to tokio
        let mut buf = tokio::io::ReadBuf::new(buf);
        ready!(tokio::io::AsyncRead::poll_read(
            self.project().file,
            cx,
            &mut buf
        ))?;
        Poll::Ready(Ok(buf.filled().len()))
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<std::io::Result<usize>> {
        // not as good as it could be, but it works
        // this API simply doesn't work for async read vectoring...
        Poll::Ready(tokio::task::block_in_place(move || {
            self.project().std_file.read_vectored(bufs)
        }))
    }
}
