use anyhow::{Context, Result, anyhow};
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs as tokio_fs,
    io::{AsyncWrite, AsyncWriteExt, BufWriter as TokioBufWriter, stdout as tokio_stdout},
};

use crate::cli::Cli;

/// Represents a type that can be used as an output writer
pub type OutputWriter = TokioBufWriter<Box<dyn AsyncWrite + Unpin + Send>>;

enum OutputMode {
    Stdout,
    StagedFile {
        final_path: PathBuf,
        staging_path: PathBuf,
    },
}

pub struct OutputTarget {
    writer: Option<OutputWriter>,
    mode: OutputMode,
}

impl OutputTarget {
    pub async fn write_all(&mut self, buffer: &[u8]) -> Result<()> {
        self.writer_mut()?.write_all(buffer).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.writer_mut()?.flush().await?;
        Ok(())
    }

    pub async fn finalize(mut self) -> Result<()> {
        self.flush().await?;
        self.close_writer();

        if let OutputMode::StagedFile {
            final_path,
            staging_path,
        } = &self.mode
        {
            tokio_fs::rename(staging_path, final_path).await.with_context(|| {
                format!(
                    "Failed to move staged output '{}' into place at '{}'",
                    staging_path.display(),
                    final_path.display()
                )
            })?;
        }

        Ok(())
    }

    pub async fn abort(mut self) {
        self.close_writer();

        if let OutputMode::StagedFile { staging_path, .. } = &self.mode {
            if let Err(error) = tokio_fs::remove_file(staging_path).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    log::warn!(
                        "Failed to remove staged output '{}': {}",
                        staging_path.display(),
                        error
                    );
                }
            }
        }
    }

    fn writer_mut(&mut self) -> Result<&mut OutputWriter> {
        self.writer
            .as_mut()
            .ok_or_else(|| anyhow!("Output writer is no longer available"))
    }

    fn close_writer(&mut self) {
        self.writer.take();
    }
}

/// Create an output target based on CLI arguments
pub async fn create_output_target(args: &Cli) -> Result<OutputTarget> {
    log::debug!("Setting up output writer");

    if args.output == "$" {
        log::debug!("Using stdout for output");
        let stdout = tokio_stdout();
        Ok(OutputTarget {
            writer: Some(TokioBufWriter::new(Box::new(stdout))),
            mode: OutputMode::Stdout,
        })
    } else {
        log::debug!("Using file for output: {}", args.output);
        let final_path = PathBuf::from(&args.output);

        if !args.overwrite && Path::new(&args.output).exists() {
            return Err(anyhow!(
                "Output file '{}' already exists. Use --overwrite to replace it.",
                args.output
            ));
        }

        let staging_path = build_staging_path(&final_path)?;
        let mut open_options = tokio_fs::OpenOptions::new();
        open_options.write(true).create_new(true);

        let file = open_options.open(&staging_path).await.with_context(|| {
            format!(
                "Failed to create staged output file '{}' for '{}'",
                staging_path.display(),
                final_path.display()
            )
        })?;

        log::debug!(
            "Output file staged successfully at {}",
            staging_path.display()
        );
        Ok(OutputTarget {
            writer: Some(TokioBufWriter::new(Box::new(file))),
            mode: OutputMode::StagedFile {
                final_path,
                staging_path,
            },
        })
    }
}

fn build_staging_path(final_path: &Path) -> Result<PathBuf> {
    let parent = final_path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = final_path
        .file_name()
        .ok_or_else(|| anyhow!("Output path '{}' must name a file", final_path.display()))?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("System clock is before UNIX_EPOCH")?
        .as_nanos();
    let staging_name = format!(
        ".{}.part-{}-{}",
        file_name.to_string_lossy(),
        std::process::id(),
        timestamp
    );

    Ok(parent.join(staging_name))
}
