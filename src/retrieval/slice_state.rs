use sonic_rs::Value;

pub(crate) struct SliceState {
    pub(crate) slice_id: usize,
    pub(crate) search_body: Value,
    pub(crate) current_id: Option<String>,
    pub(crate) pit_generation: Option<u64>,
    pub(crate) search_after: Option<Vec<u8>>,
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

    pub(crate) fn update_search_after_from_raw(&mut self, raw_sort: Option<&[u8]>) {
        self.search_after = raw_sort.map(|value| value.to_vec());
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
    fn update_search_after_from_raw_stores_new_value() {
        let mut state = SliceState::new(0, 0, json!({"size": 10}));
        let sort = br#"[2,"b"]"#;

        state.update_search_after_from_raw(Some(sort));

        assert_eq!(state.search_after.as_deref(), Some(sort.as_slice()));
    }

    #[test]
    fn update_search_after_from_raw_clears_value_when_missing() {
        let mut state = SliceState::new(0, 0, json!({"size": 10}));
        state.update_search_after_from_raw(Some(br#"[1,"a"]"#));

        state.update_search_after_from_raw(None);

        assert!(state.search_after.is_none());
    }

    #[test]
    fn update_search_after_from_raw_replaces_previous_value() {
        let mut state = SliceState::new(0, 0, json!({"size": 10}));
        let first = br#"[1,"a"]"#;
        let second = br#"[2,{"nested":true}]"#;

        state.update_search_after_from_raw(Some(first));
        state.update_search_after_from_raw(Some(second));

        assert_eq!(state.search_after.as_deref(), Some(second.as_slice()));
    }
}
