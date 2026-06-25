use anyhow::Result;
use vergen_gitcl::{Build, Cargo, Emitter, Gitcl};

fn main() -> Result<()> {
    let build = Build::builder().build_timestamp(true).build();
    let cargo = Cargo::builder().features(true).target_triple(true).build();
    let git = Gitcl::builder()
        .branch(true)
        .commit_timestamp(true)
        .describe(false, true, None)
        .sha(false)
        .build();

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&git)?
        .emit()
}
