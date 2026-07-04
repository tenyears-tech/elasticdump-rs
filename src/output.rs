use anyhow::{Context, Result, anyhow};
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs as tokio_fs,
    io::{AsyncWriteExt, BufWriter as TokioBufWriter, stdout as tokio_stdout},
};

use crate::cli::Cli;

/// Concrete, buffered sink writers. Keeping the file variant a concrete
/// `tokio_fs::File` (rather than a boxed `dyn AsyncWrite`) is what lets
/// `finalize` recover the underlying handle to `sync_all()` it before commit.
enum SinkWriter {
    Stdout(TokioBufWriter<tokio::io::Stdout>),
    File(TokioBufWriter<tokio_fs::File>),
}

impl SinkWriter {
    async fn write_all(&mut self, buffer: &[u8]) -> std::io::Result<()> {
        match self {
            SinkWriter::Stdout(writer) => writer.write_all(buffer).await,
            SinkWriter::File(writer) => writer.write_all(buffer).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            SinkWriter::Stdout(writer) => writer.flush().await,
            SinkWriter::File(writer) => writer.flush().await,
        }
    }
}

enum OutputMode {
    Stdout,
    StagedFile {
        final_path: PathBuf,
        staging_path: PathBuf,
        overwrite: bool,
        /// Mode of a pre-existing destination being overwritten, preserved onto
        /// the staged file before commit. Always `None` off unix.
        prior_mode: Option<u32>,
    },
}

pub struct OutputTarget {
    writer: Option<SinkWriter>,
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

    /// Returns `true` when this target writes to stdout rather than a file.
    pub fn is_stdout(&self) -> bool {
        matches!(self.mode, OutputMode::Stdout)
    }

    pub async fn finalize(mut self) -> Result<()> {
        let writer = self.writer.take();

        match self.mode {
            OutputMode::Stdout => {
                if let Some(mut writer) = writer {
                    if let Err(error) = writer.flush().await {
                        let error = anyhow::Error::from(error);
                        // A vanished pipe reader is not our problem to report;
                        // there is nothing left to flush to.
                        if is_broken_pipe(&error) {
                            return Ok(());
                        }
                        return Err(error).context("Failed to flush stdout");
                    }
                }
                Ok(())
            }
            OutputMode::StagedFile {
                final_path,
                staging_path,
                overwrite,
                prior_mode,
            } => {
                // Flush the buffer, then fsync the file's data to disk *before*
                // it is committed so a crash after commit cannot expose a
                // truncated "successful" output.
                let Some(SinkWriter::File(mut buffered)) = writer else {
                    // A staged target is always constructed with a file writer.
                    remove_staging(&staging_path).await;
                    return Err(anyhow!(
                        "Staged output '{}' had no file writer to finalize",
                        staging_path.display()
                    ));
                };

                if let Err(error) = buffered.flush().await {
                    remove_staging(&staging_path).await;
                    return Err(error).with_context(|| {
                        format!("Failed to flush staged output '{}'", staging_path.display())
                    });
                }

                let file = buffered.into_inner();
                if let Err(error) = file.sync_all().await {
                    remove_staging(&staging_path).await;
                    return Err(error).with_context(|| {
                        format!("Failed to fsync staged output '{}'", staging_path.display())
                    });
                }
                drop(file);

                // Preserve the overwritten destination's permissions instead of
                // leaking the staging file's umask-default mode onto it.
                #[cfg(unix)]
                if let Some(mode) = prior_mode {
                    use std::os::unix::fs::PermissionsExt;
                    let permissions = std::fs::Permissions::from_mode(mode);
                    if let Err(error) = tokio_fs::set_permissions(&staging_path, permissions).await
                    {
                        remove_staging(&staging_path).await;
                        return Err(error).with_context(|| {
                            format!(
                                "Failed to preserve mode {mode:#o} on staged output '{}'",
                                staging_path.display()
                            )
                        });
                    }
                }

                let commit_result = if overwrite {
                    tokio_fs::rename(&staging_path, &final_path)
                        .await
                        .with_context(|| {
                            format!(
                                "Failed to move staged output '{}' into place at '{}'",
                                staging_path.display(),
                                final_path.display()
                            )
                        })
                } else {
                    // Race-free commit: hard_link fails atomically if the
                    // destination was created after our start-of-run check.
                    match tokio_fs::hard_link(&staging_path, &final_path).await {
                        Ok(()) => tokio_fs::remove_file(&staging_path).await.with_context(|| {
                            format!(
                                "Failed to remove staged output '{}' after linking it into place at '{}'",
                                staging_path.display(),
                                final_path.display()
                            )
                        }),
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(anyhow!(
                            "Output file '{}' was created while the dump was running. Use --overwrite to replace it.",
                            final_path.display()
                        )),
                        Err(e) => {
                            log::debug!(
                                "hard_link commit unsupported ({e}); falling back to rename"
                            );
                            tokio_fs::rename(&staging_path, &final_path).await.with_context(|| {
                                format!(
                                    "Failed to move staged output '{}' into place at '{}'",
                                    staging_path.display(),
                                    final_path.display()
                                )
                            })
                        }
                    }
                };

                if let Err(error) = commit_result {
                    remove_staging(&staging_path).await;
                    return Err(error);
                }

                sync_parent_dir(&final_path).await;
                Ok(())
            }
        }
    }

    pub async fn abort(mut self) {
        self.close_writer();

        if let OutputMode::StagedFile { staging_path, .. } = &self.mode {
            remove_staging(staging_path).await;
        }
    }

    fn writer_mut(&mut self) -> Result<&mut SinkWriter> {
        self.writer
            .as_mut()
            .ok_or_else(|| anyhow!("Output writer is no longer available"))
    }

    fn close_writer(&mut self) {
        self.writer.take();
    }
}

/// Returns `true` if any error in the chain is an [`std::io::Error`] with
/// [`std::io::ErrorKind::BrokenPipe`].
pub fn is_broken_pipe(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| io_error.kind() == std::io::ErrorKind::BrokenPipe)
    })
}

/// Remove a staged output file, ignoring a missing file and warning on anything
/// else. Shared by `finalize`'s error paths and `abort`.
async fn remove_staging(staging_path: &Path) {
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

/// Best-effort fsync of the destination's parent directory so the rename/link
/// that committed the file is itself durable. Failures are logged, not fatal.
#[cfg(unix)]
async fn sync_parent_dir(final_path: &Path) {
    let parent = match final_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let display = parent.display().to_string();
    let result = tokio::task::spawn_blocking(move || {
        std::fs::File::open(&parent).and_then(|d| d.sync_all())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log::warn!("Failed to fsync output directory '{display}': {error}"),
        Err(error) => log::warn!("Output directory fsync task for '{display}' failed: {error}"),
    }
}

#[cfg(not(unix))]
async fn sync_parent_dir(_final_path: &Path) {}

/// Capture the mode of a pre-existing destination that is about to be
/// overwritten, so it can be re-applied to the staged file before commit.
#[cfg(unix)]
fn capture_prior_mode(final_path: &Path, overwrite: bool) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    if overwrite {
        std::fs::metadata(final_path)
            .ok()
            .map(|metadata| metadata.permissions().mode())
    } else {
        None
    }
}

#[cfg(not(unix))]
fn capture_prior_mode(_final_path: &Path, _overwrite: bool) -> Option<u32> {
    None
}

/// Create an output target based on CLI arguments
pub async fn create_output_target(args: &Cli) -> Result<OutputTarget> {
    log::debug!("Setting up output writer");

    if args.output == "$" {
        log::debug!("Using stdout for output");
        let stdout = tokio_stdout();
        Ok(OutputTarget {
            writer: Some(SinkWriter::Stdout(TokioBufWriter::new(stdout))),
            mode: OutputMode::Stdout,
        })
    } else {
        log::debug!("Using file for output: {}", args.output);
        let final_path = PathBuf::from(&args.output);

        if !args.overwrite && final_path.exists() {
            return Err(anyhow!(
                "Output file '{}' already exists. Use --overwrite to replace it.",
                args.output
            ));
        }

        let prior_mode = capture_prior_mode(&final_path, args.overwrite);

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
            writer: Some(SinkWriter::File(TokioBufWriter::new(file))),
            mode: OutputMode::StagedFile {
                final_path,
                staging_path,
                overwrite: args.overwrite,
                prior_mode,
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

#[cfg(test)]
mod tests {
    use super::{OutputTarget, create_output_target};
    use crate::cli::Cli;
    use clap::Parser;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    fn file_cli(output: &Path, overwrite: bool) -> Cli {
        let mut argv = vec![
            "elasticdump-rs".to_string(),
            "--input".to_string(),
            "http://localhost:9200/test_index".to_string(),
            "--output".to_string(),
            output.to_string_lossy().into_owned(),
        ];
        if overwrite {
            argv.push("--overwrite".to_string());
        }
        Cli::parse_from(argv)
    }

    /// Collects any `.{name}.part-...` staging siblings left in `dir`.
    fn staging_siblings(dir: &Path, file_name: &str) -> Vec<PathBuf> {
        let prefix = format!(".{file_name}.part-");
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.starts_with(&prefix))
                    .unwrap_or(false)
            })
            .collect()
    }

    async fn write_line(target: &mut OutputTarget, line: &[u8]) {
        target.write_all(line).await.unwrap();
    }

    #[tokio::test]
    async fn finalize_commits_staged_file_and_removes_staging() {
        let dir = tempdir().unwrap();
        let dest = dir.path().join("out.jsonl");
        let cli = file_cli(&dest, false);

        let mut target = create_output_target(&cli).await.unwrap();
        write_line(&mut target, b"line-1\n").await;
        target.finalize().await.unwrap();

        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "line-1\n");
        assert!(
            staging_siblings(dir.path(), "out.jsonl").is_empty(),
            "staging file should be gone after a successful commit"
        );
    }

    #[tokio::test]
    async fn finalize_without_overwrite_fails_if_destination_appeared_meanwhile() {
        let dir = tempdir().unwrap();
        let dest = dir.path().join("out.jsonl");
        let cli = file_cli(&dest, false);

        // Destination is absent when the target is created.
        let mut target = create_output_target(&cli).await.unwrap();
        write_line(&mut target, b"fresh\n").await;

        // A racing writer creates the destination while the dump is running.
        std::fs::write(&dest, b"pre-existing\n").unwrap();

        let error = target.finalize().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("was created while the dump was running"),
            "unexpected error: {error}"
        );
        assert_eq!(
            std::fs::read_to_string(&dest).unwrap(),
            "pre-existing\n",
            "existing destination must not be clobbered by a non-overwrite commit"
        );
        assert!(
            staging_siblings(dir.path(), "out.jsonl").is_empty(),
            "staging file should be removed after a failed commit"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn finalize_with_overwrite_replaces_and_preserves_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let dest = dir.path().join("out.jsonl");
        std::fs::write(&dest, b"old\n").unwrap();
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o600)).unwrap();

        let cli = file_cli(&dest, true);
        let mut target = create_output_target(&cli).await.unwrap();
        write_line(&mut target, b"new\n").await;
        target.finalize().await.unwrap();

        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "new\n");
        let mode = std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "destination mode should be preserved on overwrite"
        );
        assert!(
            staging_siblings(dir.path(), "out.jsonl").is_empty(),
            "staging file should be gone after a successful commit"
        );
    }

    #[tokio::test]
    async fn abort_removes_staging_and_keeps_existing_destination() {
        let dir = tempdir().unwrap();
        let dest = dir.path().join("out.jsonl");
        std::fs::write(&dest, b"keep-me\n").unwrap();

        let cli = file_cli(&dest, true);
        let mut target = create_output_target(&cli).await.unwrap();
        write_line(&mut target, b"partial\n").await;
        target.abort().await;

        assert_eq!(
            std::fs::read_to_string(&dest).unwrap(),
            "keep-me\n",
            "existing destination must survive an abort"
        );
        assert!(
            staging_siblings(dir.path(), "out.jsonl").is_empty(),
            "staging file should be removed after abort"
        );
    }
}
