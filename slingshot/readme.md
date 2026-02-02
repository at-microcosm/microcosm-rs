# slingshot: atproto record edge cache

local dev running:

```bash
RUST_LOG=info,slingshot=trace ulimit -n 4096 && RUST_LOG=info cargo run -- --jetstream us-east-1 --cache-dir ./foyer
```

the identity cache uses a lot of files so you probably need to bump ulimit

on macos:

```bash
ulimit -n 4096
```
