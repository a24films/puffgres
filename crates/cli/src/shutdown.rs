use tokio_util::sync::CancellationToken;

/// Manages graceful shutdown via OS signals.
///
/// On the first `SIGINT`/`SIGTERM`, the cancellation token is set and
/// pipeline phases can drain their current batch. On the second signal
/// the process exits immediately.
pub struct ShutdownController {
    token: CancellationToken,
}

impl ShutdownController {
    /// Create a new controller and spawn a background task that listens
    /// for OS signals. Returns the controller (which owns the token).
    pub fn new() -> Self {
        let token = CancellationToken::new();
        let t = token.clone();

        tokio::spawn(async move {
            wait_for_signal().await;
            tracing::info!("received shutdown signal, draining current batch…");
            t.cancel();

            // Second signal → force exit
            wait_for_signal().await;
            tracing::warn!("received second signal, forcing shutdown");
            std::process::exit(1);
        });

        Self { token }
    }

    /// Get a clone of the cancellation token to pass into pipeline phases.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = sigint.recv() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to register ctrl-c handler");
}
