pub mod cli;
pub mod elasticsearch;
pub mod output;
pub mod retrieval;

pub async fn run() -> anyhow::Result<()> {
    use clap::Parser;

    let mut args = cli::Cli::parse();

    // Configure logger based on debug flag
    if args.debug {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .init();
        log::debug!("Debug logging enabled");
    } else {
        env_logger::init();
    }

    // Normalize slices=1 (Elasticsearch rejects slice.max=1) to an unsliced dump.
    // Placed after logger init so the warning is actually emitted.
    if args.slices == 1 {
        log::warn!(
            "--slices 1 is equivalent to an unsliced dump (Elasticsearch rejects slice.max=1); running unsliced"
        );
        args.slices = 0;
    }

    // Enable colors if not in quiet mode
    if !args.quiet {
        console::set_colors_enabled(true);
        log::debug!("Console colors enabled");
    }

    // Parse input URL and extract host/index information
    let (host_url, index, auth_username, auth_password) = elasticsearch::parse_input_url(&args)?;

    log::info!(
        "Dumping data from index: {} at {}",
        index,
        host_url.as_str()
    );

    // Set up Elasticsearch client
    let client = elasticsearch::create_client(
        host_url,
        auth_username,
        auth_password,
        &elasticsearch::ClientOptions::from_cli(&args),
    )?;

    // Perform the data dump
    retrieval::dump_data(&client, &index, args).await
}
