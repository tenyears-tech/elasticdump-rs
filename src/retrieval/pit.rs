use anyhow::{Result, anyhow};
use elasticsearch::{Elasticsearch, OpenPointInTimeParts};
use sonic_rs::JsonValueTrait;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

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
}

impl SharedPitCoordinator {
    pub async fn open(
        client: &Elasticsearch,
        index: &str,
        keep_alive: &str,
        active_slices: usize,
    ) -> Result<Self> {
        let response = client
            .open_point_in_time(OpenPointInTimeParts::Index(&[index]))
            .keep_alive(keep_alive)
            .send()
            .await?;
        let body =
            super::retrieval_task::read_checked_response_bytes(response, 0, "PIT open").await?;
        let id = parse_pit_open_id(&body)?;

        Ok(Self::new_for_test(id, active_slices))
    }

    pub fn new_for_test(current_id: String, active_slices: usize) -> Self {
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
        }
    }

    pub async fn acquire(&self) -> PitLease {
        let state = self.inner.lock().await;
        PitLease {
            generation: state.generation,
            id: state.current_id.clone(),
        }
    }

    pub async fn wait_for_generation(&self, generation: u64) -> Result<PitLease> {
        loop {
            let notified = self.notify.notified();
            let maybe_lease = {
                let state = self.inner.lock().await;
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

            notified.await;
        }
    }

    pub async fn complete_round(
        &self,
        generation: u64,
        returned_id: Option<String>,
        slice_finished: bool,
    ) -> Result<()> {
        let mut state = self.inner.lock().await;
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

    pub async fn abort(&self, error: String) {
        let mut state = self.inner.lock().await;
        if state.aborted.is_none() {
            state.aborted = Some(error);
            self.notify.notify_waiters();
        }
    }

    pub async fn observe_returned_id(&self, returned_id: Option<&str>) {
        let Some(returned_id) = returned_id else {
            return;
        };
        let mut state = self.inner.lock().await;
        state.latest_observed_id = returned_id.to_string();
    }

    pub async fn latest_id(&self) -> String {
        self.inner.lock().await.latest_observed_id.clone()
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
        let shared = super::SharedPitCoordinator::new_for_test("pit-0".into(), 2);

        let lease_a = shared.acquire().await;
        let lease_b = shared.acquire().await;

        assert_eq!(lease_a.generation, 0);
        assert_eq!(lease_b.generation, 0);
        assert_eq!(lease_a.id, "pit-0");
        assert_eq!(lease_b.id, "pit-0");

        shared
            .complete_round(lease_a.generation, Some("pit-a-next".into()), false)
            .await
            .unwrap();

        let still_blocked =
            tokio::time::timeout(Duration::from_millis(50), shared.wait_for_generation(1)).await;
        assert!(still_blocked.is_err(), "generation must not advance early");

        shared
            .complete_round(lease_b.generation, Some("pit-b-next".into()), false)
            .await
            .unwrap();

        let lease_next = shared.wait_for_generation(1).await.unwrap();
        assert_eq!(lease_next.generation, 1);
        assert_eq!(lease_next.id, "pit-b-next");
    }

    #[tokio::test]
    async fn shared_pit_drops_finished_slices_from_future_generations() {
        let shared = super::SharedPitCoordinator::new_for_test("pit-0".into(), 2);

        let lease_a = shared.acquire().await;
        let lease_b = shared.acquire().await;

        shared
            .complete_round(lease_a.generation, Some("pit-a-next".into()), true)
            .await
            .unwrap();
        shared
            .complete_round(lease_b.generation, Some("pit-b-next".into()), false)
            .await
            .unwrap();

        let lease_next = shared.wait_for_generation(1).await.unwrap();
        assert_eq!(lease_next.id, "pit-b-next");

        shared
            .complete_round(lease_next.generation, Some("pit-b-next-2".into()), false)
            .await
            .unwrap();

        let lease_final = shared.wait_for_generation(2).await.unwrap();
        assert_eq!(lease_final.generation, 2);
        assert_eq!(lease_final.id, "pit-b-next-2");
    }

    #[tokio::test]
    async fn shared_pit_tracks_latest_observed_id_for_final_cleanup() {
        let shared = super::SharedPitCoordinator::new_for_test("pit-0".into(), 2);

        shared.observe_returned_id(Some("pit-1")).await;

        let lease = shared.acquire().await;
        assert_eq!(lease.id, "pit-0");
        assert_eq!(shared.latest_id().await, "pit-1");
    }
}
