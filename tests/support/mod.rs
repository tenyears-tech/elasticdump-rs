//! Shared helpers for the integration test target.
//!
//! This module lives in a `support/` subdirectory so cargo does not compile it
//! as its own test crate: only `.rs` files directly under `tests/` become test
//! targets. It is pulled into `integration_test.rs` via `mod support;`.

pub mod mock_es;
