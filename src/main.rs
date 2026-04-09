use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;

use postgresql_trino_gateway::config::Config;
use postgresql_trino_gateway::handler::GatewayHandlerFactory;
use postgresql_trino_gateway::query_simple::GatewayQueryHandler;
use postgresql_trino_gateway::startup::GatewayStartupHandler;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Arc::new(Config::parse());

    let factory = Arc::new(GatewayHandlerFactory {
        startup: Arc::new(GatewayStartupHandler {
            config: config.clone(),
        }),
        query: Arc::new(GatewayQueryHandler),
    });

    let listener = TcpListener::bind(&config.listen_addr).await?;
    tracing::info!(addr = %config.listen_addr, "listening for PostgreSQL connections");

    loop {
        let (socket, _addr) = listener.accept().await?;
        let factory = factory.clone();

        tokio::spawn(async move {
            if let Err(e) = pgwire::tokio::process_socket(socket, None, factory).await {
                tracing::error!(error = %e, "connection error");
            }
        });
    }
}
