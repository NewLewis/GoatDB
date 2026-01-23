use std::sync::mpsc::Sender;

use crate::goatkv::utils::cleanup_task::CleanupTask;

#[derive(Debug)]
pub struct WalHandle {
    log_number: u64,
    cleanup_sender: Sender<CleanupTask>,
}

impl WalHandle {
    pub fn new(log_number: u64, cleanup_sender: Sender<CleanupTask>) -> Self {
        Self {
            log_number,
            cleanup_sender,
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
        let _ = self.cleanup_sender.send(CleanupTask::Wal(self.log_number));
    }
}
