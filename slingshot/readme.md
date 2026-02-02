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

## prod deploy

you **must** setcap the binary to run it on apollo!!!!

```bash
sudo setcap CAP_NET_BIND_SERVICE=+eip ../target/release/slingshot
```

then run with

```bash
RUST_BACKTRACE=1 RUST_LOG=info,slingshot=trace /home/ubuntu/links/target/release/slingshot \
  --jetstream wss://jetstream1.us-east.fire.hose.cam/subscribe \
  --healthcheck https://hc-ping.com/[REDACTED] \
  --cache-dir ./foyer \
  --record-cache-memory-mb 2048 \
  --record-cache-disk-gb 32 \
  --identity-cache-memory-mb 1024 \
  --identity-cache-disk-gb 8 \
  --collect-metrics \
  --acme-ipv6 \
  --acme-domain slingshot.microcosm.blue \
  --acme-contact phil@bad-example.com \
  --acme-cache-path /home/ubuntu/certs
```
