
use anyhow::Result;
use simple_redis::{Backend, network};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "0.0.0.0:6379";
    info!("Simple-Redis-Server is listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    let backend = Backend::new();
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("New connection from {}", addr);
        let cloned_backend = backend.clone();
        tokio::spawn(async move {
            if let Err(e) = network::stream_handler(stream, cloned_backend).await {
                warn!("Error handling connection from {}: {}", addr, e);
            }
        });
    }
}
