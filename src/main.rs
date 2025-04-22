use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    elasticdump_rs::run().await
}
