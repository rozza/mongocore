pub mod buffer_pool;
pub mod codec;
pub mod connection;
pub mod dispatch;
pub mod frame;
pub mod prefetch;

use std::path::Path;

use tokio::net::UnixListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::connection::pool::ConnectionPool;
use crate::operations::Operations;
use crate::transport::buffer_pool::{BufferPool, BufferPoolConfig};
use crate::transport::connection::{handle_connection, ConnectionConfig};

/// Handle returned from starting the binary transport.
///
/// Contains the task handle and a shutdown sender. Sending `true` on the
/// shutdown channel triggers graceful shutdown: each connected client receives
/// a shutdown frame and is given a drain period before the socket is closed.
pub struct BinaryTransportHandle {
    pub task: JoinHandle<()>,
    pub shutdown_tx: watch::Sender<bool>,
}

/// Start the binary UDS transport server.
///
/// Spawns a background task that listens for connections on the configured
/// Unix domain socket and handles each one with the binary frame protocol.
///
/// Returns a [`BinaryTransportHandle`] containing both the spawned task and
/// a shutdown sender. Send `true` on the sender to initiate graceful shutdown.
pub fn start_binary_transport(
    config: &Config,
    pool: ConnectionPool,
    operations: Operations,
) -> BinaryTransportHandle {
    let socket_path = config.binary_socket_path.clone();
    let socket_permissions = config.binary_socket_permissions;
    let max_frame_size = config.binary_max_frame_size as u32;
    let max_concurrent = config.binary_max_concurrent as u32;

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let task = tokio::spawn(async move {
        // Remove stale socket file if it exists
        let path = Path::new(&socket_path);
        if path.exists() {
            if let Err(e) = std::fs::remove_file(path) {
                error!("Failed to remove stale binary socket {}: {}", socket_path, e);
                return;
            }
        }

        // Bind UnixListener to the socket path
        let listener = match UnixListener::bind(&socket_path) {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind binary transport to {}: {}", socket_path, e);
                return;
            }
        };

        // Set socket permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(socket_permissions);
            if let Err(e) = std::fs::set_permissions(&socket_path, perms) {
                warn!("Failed to set binary socket permissions: {}", e);
            }
        }

        // Create buffer pool with default config
        let buffer_pool = BufferPool::new(BufferPoolConfig::default());

        info!("Binary transport listening on {}", socket_path);

        // Register interface metadata
        pool.append_interface_metadata("binary");

        // Accept loop — exits when shutdown is signaled
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, _addr)) => {
                            let ops = operations.clone();
                            let p = pool.clone();
                            let conn_config = ConnectionConfig {
                                max_frame_size,
                                max_concurrent,
                            };
                            let bp = buffer_pool.clone();
                            let conn_shutdown_rx = shutdown_rx.clone();
                            tokio::spawn(async move {
                                handle_connection(stream, ops, p, conn_config, bp, conn_shutdown_rx).await;
                            });
                        }
                        Err(e) => {
                            warn!("Failed to accept binary transport connection: {}", e);
                        }
                    }
                }
                result = shutdown_rx.changed() => {
                    if result.is_ok() && *shutdown_rx.borrow() {
                        info!("Binary transport accept loop shutting down");
                        break;
                    }
                }
            }
        }
    });

    BinaryTransportHandle { task, shutdown_tx }
}
