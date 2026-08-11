# Pre-refactor Task 0 audit manifest

- UTC snapshot: `20260811T040118Z`
- Source repository: `/opt/bots/soxl-trading-jinho`
- Source HEAD: `2aa0e5ab49e0a858559520e59ea692a021bd926e`
- Source dirty-status SHA-256: `482e072363d71e51e42b1534c97e33d498c3a21ec96f465c4654ec52f026de5e`
- Immutable backup: `/root/backups/soxl-trading-jinho/pre-v4-official-20split-20260811T040118Z`
- Full archive SHA-256: `102c02d66d0d0fdac2eab89ce5fa997cf78e5a03a42afdb264e65857db9de9f8`
- Worktree-content archive SHA-256: `a5d8b57f7ee4a4305b39c94a66f89d601f52055727ea2826cd54daaee1c94401`
- Repository bundle SHA-256: `3042f728e3d2ea4d44bb239b917987f3abb8bc1254a02d905cd959e1d7b1c798`
- Dirty patch SHA-256: `de008a98f373cb0222e02540787cc5c8af8f31c18e9a528d059aca9bc1950453`
- Baseline commit: `28b559a195abc30f5cdfd8f1b65f23119da40ceb`
- Local tag: `backup/pre-v4-refactor-20260811T040118Z`

## Core operating-data SHA-256

```text
363fea9df14e5f76104771c6040cccaa68edb0539f76c5680570803043b172e2  data/manual_ledger.json
7aba0e0ab58a01d1dc9df56631344a928b97979e6e35ef565946b2cfed55c7bc  data/kis_execution_history_SOXL_20260622_20260811.json
fb263f6fec20cb763b7e42c9c3cb9bfbe85e4ffb20d29512fda0529d54b9a4d4  data/t_state.json
858af1834fbd14bf3491d1be7eade488a58e5f3c54bd4168b46066acff58d0b1  data/reverse_config.json
c3d8161a84743a96ac6199bd5daf4953f2f27d7b84545fea4e5ca82e0b6c397d  data/kis_balance.json
29eaa3798e9df86a75824a467b9153e7867ec30ba5a66d93774036919c05b160  data/version_config.json
```

## Verification gates

- `project-full.tar`: readable; 1,969 entries; `.git` and `data` present.
- `worktree-content.tar`: readable; 343 entries; root `.git` excluded.
- `sha256sum -c SHA256SUMS`: 16/16 passed.
- Core live-data `sha256sum -c`: 6/6 passed while the bot container was stopped.
- `git bundle verify`: passed.
- Restored worktree regular-file hashes: 336/336 passed before baseline commit.
- Credential guard: `.env` is ignored and unstaged; no sensitive credential filename or literal credential assignment was staged.
- RED: base image returned `No module named pytest` (exit 1).
- GREEN: isolated image under `--network none` returned `1 passed in 0.03s`.
