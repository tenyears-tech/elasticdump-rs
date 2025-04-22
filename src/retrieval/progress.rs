use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

/// Set up progress bars for input and output
pub fn setup_progress_bars(mp: &MultiProgress) -> (Option<ProgressBar>, Option<ProgressBar>) {
    let input_bar = {
        let pb = mp.add(ProgressBar::new(0));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.yellow/red}]  Input: {pos}/{len} ({per_sec}, {msg}) {eta}")
                .unwrap()
                .progress_chars("#>-"),
        );
        pb.set_message("Waiting for total count...");
        Some(pb)
    };

    let output_bar = {
        let pb = mp.add(ProgressBar::new(0));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.blue} [{elapsed_precise}] [{wide_bar:.cyan/blue}] Output: {pos}/{len} ({per_sec}, {msg}) {eta}")
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    };

    (input_bar, output_bar)
}
