use anyhow::{Result, anyhow};
use elasticsearch::{Elasticsearch, OpenPointInTimeParts};
use sonic_rs::JsonValueTrait;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::retrieval_task::{DumpCancelled, RetryMode, RetryPolicy, send_checked_with_retry};

fn parse_pit_open_id(body: &[u8]) -> Result<String> {
    let json: sonic_rs::Value = sonic_rs::from_slice(body)?;
    json["id"]
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("No PIT ID found in response"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PitLease {
    pub generation: u64,
    pub id: String,
}

#[derive(Debug)]
struct PitState {
    current_id: String,
    latest_observed_id: String,
    generation: u64,
    active_slices: usize,
    completed_in_generation: usize,
    finished_in_generation: usize,
    pending_next_id: Option<String>,
    aborted: Option<String>,
}

#[derive(Clone)]
pub struct SharedPitCoordinator {
    inner: Arc<Mutex<PitState>>,
    notify: Arc<Notify>,
    cancel: CancellationToken,
}

impl SharedPitCoordinator {
    pub async fn open(
        client: &Elasticsearch,
        index: &str,
        keep_alive: &str,
        active_slices: usize,
        retry: RetryPolicy,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let indices = [index];
        let body =
            send_checked_with_retry(retry, &cancel, RetryMode::Idempotent, 0, "PIT open", || {
                client
                    .open_point_in_time(OpenPointInTimeParts::Index(&indices))
                    .keep_alive(keep_alive)
                    .send()
            })
            .await?;
        let id = parse_pit_open_id(&body)?;

        Ok(Self::new_for_test(id, active_slices, cancel))
    }

    pub fn new_for_test(
        current_id: String,
        active_slices: usize,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PitState {
                latest_observed_id: current_id.clone(),
                current_id,
                generation: 0,
                active_slices,
                completed_in_generation: 0,
                finished_in_generation: 0,
                pending_next_id: None,
                aborted: None,
            })),
            notify: Arc::new(Notify::new()),
            cancel,
        }
    }

    pub fn acquire(&self) -> PitLease {
        let state = self.inner.lock().unwrap();
        PitLease {
            generation: state.generation,
            id: state.current_id.clone(),
        }
    }

    pub async fn wait_for_generation(&self, generation: u64) -> Result<PitLease> {
        loop {
            // Cancellation takes precedence over an abort message: once the
            // pipeline token fires, guard-driven aborts are cascade noise and
            // every waiter must report the cancellation sentinel instead.
            if self.cancel.is_cancelled() {
                return Err(anyhow::Error::new(DumpCancelled));
            }

            let notified = self.notify.notified();
            let maybe_lease = {
                let state = self.inner.lock().unwrap();
                if let Some(error) = &state.aborted {
                    return Err(anyhow!(error.clone()));
                }

                if state.generation >= generation {
                    Some(PitLease {
                        generation: state.generation,
                        id: state.current_id.clone(),
                    })
                } else {
                    None
                }
            };

            if let Some(lease) = maybe_lease {
                return Ok(lease);
            }

            tokio::select! {
                _ = notified => {}
                _ = self.cancel.cancelled() => return Err(anyhow::Error::new(DumpCancelled)),
            }
        }
    }

    pub fn complete_round(
        &self,
        generation: u64,
        returned_id: Option<String>,
        slice_finished: bool,
    ) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(anyhow::Error::new(DumpCancelled));
        }

        let mut state = self.inner.lock().unwrap();
        if let Some(error) = &state.aborted {
            return Err(anyhow!(error.clone()));
        }

        if generation != state.generation {
            return Ok(());
        }

        let participants = state.active_slices;
        if participants == 0 {
            return Ok(());
        }

        if let Some(id) = returned_id {
            state.latest_observed_id = id.clone();
            state.pending_next_id = Some(id);
        }
        state.completed_in_generation += 1;
        if slice_finished {
            state.finished_in_generation += 1;
        }

        if state.completed_in_generation == participants {
            if let Some(next_id) = state.pending_next_id.take() {
                state.current_id = next_id;
            }
            state.generation += 1;
            state.active_slices = state
                .active_slices
                .saturating_sub(state.finished_in_generation);
            state.completed_in_generation = 0;
            state.finished_in_generation = 0;
            self.notify.notify_waiters();
        }

        Ok(())
    }

    pub fn abort(&self, error: String) {
        let mut state = self.inner.lock().unwrap();
        if state.aborted.is_none() {
            state.aborted = Some(error);
            self.notify.notify_waiters();
        }
    }

    pub fn observe_returned_id(&self, returned_id: Option<&str>) {
        let Some(returned_id) = returned_id else {
            return;
        };
        let mut state = self.inner.lock().unwrap();
        state.latest_observed_id = returned_id.to_string();
    }

    pub fn latest_id(&self) -> String {
        self.inner.lock().unwrap().latest_observed_id.clone()
    }
}

/// Aborts the shared PIT coordinator when dropped while still armed, so a
/// panicking or otherwise abruptly terminated slice can never strand its
/// siblings parked in `wait_for_generation`. Defuse before returning success;
/// error returns may leave it armed because `abort` is idempotent and the
/// first (more specific) abort message wins.
pub(crate) struct PitAbortGuard {
    coordinator: SharedPitCoordinator,
    armed: bool,
}

impl PitAbortGuard {
    pub(crate) fn new(coordinator: SharedPitCoordinator) -> Self {
        Self {
            coordinator,
            armed: true,
        }
    }

    pub(crate) fn defuse(mut self) {
        self.armed = false;
    }
}

impl Drop for PitAbortGuard {
    fn drop(&mut self) {
        if self.armed {
            self.coordinator
                .abort("PIT slice terminated unexpectedly (panic or cancellation)".into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_pit_open_id;
    use std::time::Duration;

    #[test]
    fn parse_pit_open_id_reads_id_from_sonic_response() {
        let body = br#"{"id":"pit-123","creation_time":12345}"#;

        assert_eq!(parse_pit_open_id(body).unwrap(), "pit-123");
    }

    #[test]
    fn parse_pit_open_id_rejects_missing_id() {
        let body = br#"{"creation_time":12345}"#;

        let error = parse_pit_open_id(body).unwrap_err().to_string();
        assert!(error.contains("No PIT ID found in response"));
    }

    #[tokio::test]
    async fn shared_pit_advances_generation_after_all_active_slices_report() {
        let shared = super::SharedPitCoordinator::new_for_test(
            "pit-0".into(),
            2,
            tokio_util::sync::CancellationToken::new(),
        );

        let lease_a = shared.acquire();
        let lease_b = shared.acquire();

        assert_eq!(lease_a.generation, 0);
        assert_eq!(lease_b.generation, 0);
        assert_eq!(lease_a.id, "pit-0");
        assert_eq!(lease_b.id, "pit-0");

        shared
            .complete_round(lease_a.generation, Some("pit-a-next".into()), false)
            .unwrap();

        let still_blocked =
            tokio::time::timeout(Duration::from_millis(50), shared.wait_for_generation(1)).await;
        assert!(still_blocked.is_err(), "generation must not advance early");

        shared
            .complete_round(lease_b.generation, Some("pit-b-next".into()), false)
            .unwrap();

        let lease_next = shared.wait_for_generation(1).await.unwrap();
        assert_eq!(lease_next.generation, 1);
        assert_eq!(lease_next.id, "pit-b-next");
    }

    #[tokio::test]
    async fn shared_pit_drops_finished_slices_from_future_generations() {
        let shared = super::SharedPitCoordinator::new_for_test(
            "pit-0".into(),
            2,
            tokio_util::sync::CancellationToken::new(),
        );

        let lease_a = shared.acquire();
        let lease_b = shared.acquire();

        shared
            .complete_round(lease_a.generation, Some("pit-a-next".into()), true)
            .unwrap();
        shared
            .complete_round(lease_b.generation, Some("pit-b-next".into()), false)
            .unwrap();

        let lease_next = shared.wait_for_generation(1).await.unwrap();
        assert_eq!(lease_next.id, "pit-b-next");

        shared
            .complete_round(lease_next.generation, Some("pit-b-next-2".into()), false)
            .unwrap();

        let lease_final = shared.wait_for_generation(2).await.unwrap();
        assert_eq!(lease_final.generation, 2);
        assert_eq!(lease_final.id, "pit-b-next-2");
    }

    #[tokio::test]
    async fn wait_for_generation_returns_error_when_cancelled() {
        let cancel = tokio_util::sync::CancellationToken::new();
        let shared = super::SharedPitCoordinator::new_for_test("pit-0".into(), 2, cancel.clone());

        let waiter = tokio::spawn({
            let shared = shared.clone();
            async move { shared.wait_for_generation(1).await }
        });

        // Let the waiter park before firing the cancellation.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        let result = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancelled waiter must return promptly")
            .expect("waiter task must not panic");
        let error = result.expect_err("cancellation must surface as an error");
        assert!(
            crate::retrieval::retrieval_task::is_cancelled(&error),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn abort_guard_aborts_coordinator_on_drop() {
        let shared = super::SharedPitCoordinator::new_for_test(
            "pit-0".into(),
            2,
            tokio_util::sync::CancellationToken::new(),
        );

        // A defused guard must not abort the coordinator.
        let guard = super::PitAbortGuard::new(shared.clone());
        guard.defuse();
        let lease = shared
            .wait_for_generation(0)
            .await
            .expect("defused guard must not abort the coordinator");
        assert_eq!(lease.generation, 0);

        let waiter = tokio::spawn({
            let shared = shared.clone();
            async move { shared.wait_for_generation(1).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        drop(super::PitAbortGuard::new(shared.clone()));

        let result = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("aborted waiter must return promptly")
            .expect("waiter task must not panic");
        let error = result.expect_err("armed guard drop must abort the coordinator");
        assert!(
            error
                .to_string()
                .contains("PIT slice terminated unexpectedly"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn shared_pit_tracks_latest_observed_id_for_final_cleanup() {
        let shared = super::SharedPitCoordinator::new_for_test(
            "pit-0".into(),
            2,
            tokio_util::sync::CancellationToken::new(),
        );

        shared.observe_returned_id(Some("pit-1"));

        let lease = shared.acquire();
        assert_eq!(lease.id, "pit-0");
        assert_eq!(shared.latest_id(), "pit-1");
    }
}
