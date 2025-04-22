use anyhow::{Context, Result};
use std::path::Path;
use tokio::{
    fs as tokio_fs,
    io::{AsyncWrite, BufWriter as TokioBufWriter, stdout as tokio_stdout},
};

use crate::cli::Cli;

/// Represents a type that can be used as an output writer
pub type OutputWriter = TokioBufWriter<Box<dyn AsyncWrite + Unpin + Send>>;

/// Create an output writer based on CLI arguments
pub async fn create_output_writer(args: &Cli) -> Result<OutputWriter> {
    log::debug!("Setting up output writer");

    if args.output == "$" {
        // Write to stdout
        log::debug!("Using stdout for output");
        let stdout = tokio_stdout();
        Ok(TokioBufWriter::new(Box::new(stdout)))
    } else {
        // Write to file
        log::debug!("Using file for output: {}", args.output);
        let mut open_options = tokio_fs::OpenOptions::new();
        open_options.write(true).create(true);

        if args.overwrite {
            log::debug!("Output file will be overwritten if it exists");
            open_options.truncate(true);
        } else {
            log::debug!("Output file will fail if it already exists");
            open_options.create_new(true);
        }

        let file = open_options.open(&args.output).await.with_context(|| {
            if !args.overwrite && Path::new(&args.output).exists() {
                format!(
                    "Output file '{}' already exists. Use --overwrite to replace it.",
                    args.output
                )
            } else {
                format!("Failed to create/open output file: {}", args.output)
            }
        })?;

        log::debug!("Output file opened successfully");
        Ok(TokioBufWriter::new(Box::new(file)))
    }
}
