//! A simple frontend for benchmark purposes.

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

mod ids;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The action to perform
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create an repository
    Init(InitArgs),
    /// Create an new manifest
    Create(CreateArgs),
    /// Extract a manifest
    Extract(ExtractArgs),
    /// Create a fossil collection
    Collect(CollectArgs),
    /// Delete a fossil collection
    Delete(DeleteArgs),
}

#[derive(Args)]
struct InitArgs {
    /// The path of the new repository
    repository: PathBuf,
}

#[derive(Args)]
struct CreateArgs {
    /// The path of the repository
    repository: PathBuf,
    /// The name of the new manifest
    manifest: String,
    /// Paths to include in the manifest
    paths: Vec<PathBuf>,
    /// Size of the generated chunks
    #[arg(long)]
    chunk_size: Option<usize>,
    /// Number of operations to perform in parallel
    #[arg(long)]
    #[clap(default_value_t = 1)]
    parallelism: usize,
}

#[derive(Args)]
struct ExtractArgs {
    /// The path of the repository
    repository: PathBuf,
    /// The name of the manifest
    manifest: String,
    /// Directory for the extracted files
    destionation: PathBuf,
    /// Number of operations to perform in parallel
    #[arg(long)]
    #[clap(default_value_t = 1)]
    parallelism: usize,
}

#[derive(Args)]
struct CollectArgs {
    /// The path of the repository
    repository: PathBuf,
    /// Path to the fossil collection being created
    collection: PathBuf,
    /// Manifests to delete
    manifests: Vec<String>,
    /// Number of operations to perform in parallel
    #[arg(long)]
    #[clap(default_value_t = 1)]
    parallelism: usize,
}

#[derive(Args)]
struct DeleteArgs {
    /// The path of the repository
    repository: PathBuf,
    /// Path to the fossil collection being deleted
    collection: PathBuf,
    /// Number of operations to perform in parallel
    #[arg(long)]
    #[clap(default_value_t = 1)]
    parallelism: usize,
    /// Manifests to delete during pipelined collection
    #[arg(long)]
    collect: Vec<String>,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        _ => unimplemented!(),
    };
}
