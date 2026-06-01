use anyhow::Result;
use vergen_gitcl::{BuildBuilder, CargoBuilder, Emitter, GitclBuilder};

fn main() -> Result<()> {
    let build = BuildBuilder::default().build_timestamp(true).build()?;
    let cargo = CargoBuilder::default()
        .features(true)
        .target_triple(true)
        .build()?;
    let git = GitclBuilder::default()
        .branch(true)
        .commit_timestamp(true)
        .describe(false, true, None)
        .sha(false)
        .build()?;

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&git)?
        .emit()
}
