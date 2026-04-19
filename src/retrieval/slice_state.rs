use sonic_rs::{JsonValueTrait, Value};

pub(crate) struct SliceState {
    pub(crate) slice_id: usize,
    pub(crate) search_body: Value,
    pub(crate) current_id: Option<String>,
    pub(crate) pit_generation: Option<u64>,
    pub(crate) search_after: Option<Value>,
    pub(crate) retrieved_hits: u64,
    next_worker: usize,
}

impl SliceState {
    pub(crate) fn new(slice_id: usize, initial_worker: usize, search_body: Value) -> Self {
        Self {
            slice_id,
            search_body,
            current_id: None,
            pit_generation: None,
            search_after: None,
            retrieved_hits: 0,
            next_worker: initial_worker,
        }
    }

    pub(crate) fn dispatch_worker(&mut self, worker_count: usize) -> usize {
        let worker = self.next_worker;
        self.next_worker = (self.next_worker + 1) % worker_count;
        worker
    }

    pub(crate) fn update_search_after_from_hits(&mut self, hits: Option<&[Value]>) {
        self.search_after = hits
            .and_then(|items| items.last())
            .and_then(|last_hit| last_hit.get("sort").cloned());
    }
}

#[cfg(test)]
mod tests {
    use super::SliceState;
    use sonic_rs::json;

    #[test]
    fn next_worker_wraps_around_worker_count() {
        let mut state = SliceState::new(2, 1, json!({"size": 10}));

        assert_eq!(state.dispatch_worker(4), 1);
        assert_eq!(state.dispatch_worker(4), 2);
        assert_eq!(state.dispatch_worker(4), 3);
        assert_eq!(state.dispatch_worker(4), 0);
    }

    #[test]
    fn update_search_after_from_last_hit_tracks_last_sort_values() {
        let mut state = SliceState::new(0, 0, json!({"size": 10}));
        let hits = vec![json!({"sort": [1, "a"]}), json!({"sort": [2, "b"]})];

        state.update_search_after_from_hits(Some(hits.as_slice()));

        assert_eq!(state.search_after.as_ref(), Some(&json!([2, "b"])));
    }

    #[test]
    fn update_search_after_from_last_hit_ignores_missing_sort() {
        let mut state = SliceState::new(0, 0, json!({"size": 10}));
        let hits = vec![json!({"_id": "no-sort"})];

        state.update_search_after_from_hits(Some(hits.as_slice()));

        assert!(state.search_after.is_none());
    }
}
