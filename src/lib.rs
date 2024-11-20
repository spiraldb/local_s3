use std::io;
use std::mem::ManuallyDrop;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;

use bytes::Bytes;
use futures::channel::oneshot;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use s3s::auth::SimpleAuth;
use s3s::dto::{CreateBucketInput, PutObjectInput, StreamingBlob};
use s3s::service::{S3ServiceBuilder, SharedS3Service};
use s3s::{S3Request, S3};
use s3s_fs::FileSystem;
use tokio::net::TcpListener;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

pub struct LocalS3 {
    // spawn a one-off runtime to run async config requests in blocking fashion.
    config_rt: Runtime,
    path: PathBuf,
    port: u16,
    key_id: String,
    secret_key: String,
}

pub const DEFAULT_KEY_ID: &str = "test-key-id";
pub const DEFAULT_SECRET_KEY: &str = "test-secret-key";

impl LocalS3 {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            config_rt: RuntimeBuilder::new_current_thread().build().unwrap(),
            path: path.as_ref().into(),
            port: 3030,
            key_id: DEFAULT_KEY_ID.into(),
            secret_key: DEFAULT_SECRET_KEY.into(),
        }
    }

    pub fn create_bucket(&self, name: &str) {
        let fs = FileSystem::new(&self.path).unwrap();

        let request = S3Request::new(
            CreateBucketInput::builder()
                .bucket(name.into())
                .build()
                .unwrap(),
        );

        self.config_rt.block_on(async move {
            fs.create_bucket(request).await.unwrap();
        });
    }

    pub fn put_object(&self, bucket: &str, object: &str, contents: Bytes) {
        let fs = FileSystem::new(&self.path).unwrap();
        let request = S3Request::new(
            PutObjectInput::builder()
                .bucket(bucket.into())
                .key(object.into())
                .body(Some(StreamingBlob::wrap(futures::stream::once(
                    async move { Ok::<Bytes, io::Error>(contents) },
                ))))
                .build()
                .unwrap(),
        );

        self.config_rt
            .block_on(async move { fs.put_object(request).await.unwrap() });
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_credentials(mut self, key_id: &str, secret_key: &str) -> Self {
        self.key_id = key_id.into();
        self.secret_key = secret_key.into();
        self
    }

    // Start in the background, returns a shutdown handle.
    #[must_use]
    pub fn start(self) -> S3Handle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let port = self.port;
        let path = self.path;
        let key_id = self.key_id;
        let secret_key = self.secret_key;

        // Spawn a background thread to execute the S3 handle.
        let handle = std::thread::spawn(move || {
            let runtime = Runtime::new().unwrap();
            let filesystem = FileSystem::new(&path).unwrap();
            let mut shared_s3 = S3ServiceBuilder::new(filesystem);
            shared_s3.set_auth(SimpleAuth::from_single(key_id, secret_key));
            let shared_s3 = shared_s3.build().into_shared();

            runtime
                .block_on(run_server(port, shared_s3, shutdown_rx))
                .unwrap();
        });

        S3Handle {
            handle,
            sender: ManuallyDrop::new(shutdown_tx),
        }
    }
}

/// Main S3 server entrypoint.
async fn run_server(
    port: u16,
    service: SharedS3Service,
    mut shutdown: oneshot::Receiver<()>,
) -> io::Result<()> {
    println!("server starting on port {port}...");
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;

    loop {
        if shutdown.try_recv() == Ok(Some(())) {
            println!("server received shutdown signal");
            break;
        }

        let (conn, addr) = listener.accept().await?;
        println!("server accepted connection from {addr}");
        let io = TokioIo::new(conn);
        let service = service.clone();
        tokio::spawn(async move {
            if let Err(err) = Builder::new(TokioExecutor::new())
                .http1_only()
                .serve_connection(io, service)
                .await
            {
                eprintln!("server error: {}", err);
            }
        });
    }

    Ok(())
}

pub struct S3Handle {
    handle: JoinHandle<()>,
    sender: ManuallyDrop<oneshot::Sender<()>>,
}

impl S3Handle {
    /// Detach the shutdown handle.
    ///
    /// Afterward this, the lifecycle of the S3 service will be tied to the current process.
    pub fn detach(self) {
        // Nothing happens.
    }

    /// Block on graceful shutdown of the S3 service.
    pub fn shutdown(mut self) {
        // SAFETY: this is the only code path which drops the sender.
        let sender = unsafe { ManuallyDrop::take(&mut self.sender) };
        sender.send(()).unwrap();
        self.handle.join().unwrap();
    }
}
