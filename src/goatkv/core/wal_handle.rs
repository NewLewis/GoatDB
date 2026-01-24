use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;

use crate::goatkv::utils::cleanup_task::CleanupTask;

#[derive(Debug)]
pub struct WalHandle {
    log_number: u64,
    cleanup_sender: Sender<CleanupTask>,
    cleanup_enabled: Arc<AtomicBool>,
}

impl WalHandle {
    pub fn new(
        log_number: u64,
        cleanup_sender: Sender<CleanupTask>,
        cleanup_enabled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            log_number,
            cleanup_sender,
            cleanup_enabled,
        }
    }

    pub fn log_number(&self) -> u64 {
        self.log_number
    }
}

impl Drop for WalHandle {
    fn drop(&mut self) {
        if self.log_number == 0 {
            return;
        }
        if !self.cleanup_enabled.load(Ordering::SeqCst) {
            return;
        }
        let _ = self.cleanup_sender.send(CleanupTask::Wal(self.log_number));
    }
}
