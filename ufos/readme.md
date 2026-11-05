# UFOs

_work in progress_



----

cross-compile for raspi 1:

set up `cross`

```bash
# build and deploy

cross build --release --target arm-unknown-linux-gnueabihf && scp ../target/arm-unknown-linux-gnueabihf/release/ufos angel-hair.local:ufos
```

for bonilla (rp4)

```bash
cross build --release --target armv7-unknown-linux-gnueabihf && scp ../target/armv7-unknown-linux-gnueabihf/release/ufos pi@bonilla.local:ufos
```

glibc will cause problems when switching between (`GLIBC_2.25` message). clean up (next build will be slowww)

```bash
cargo clean
```

for bonilla but 64-bit? (rp4)
```bash
cross build --release --target aarch64-unknown-linux-gnu && scp ../target/aarch64-unknown-linux-gnu/release/ufos pi@bonilla.local:ufos
# ^^ fails due to linker?

cross build --release --target aarch64-unknown-linux-musl && scp ../target/aarch64-unknown-linux-musl/release/ufos pi@bonilla.local:ufos
# seems to work

rsync -avhP ufos-bff-rl/ pi@bonilla:/mnt/ufos-db/

RUST_LOG=info ./ufos --jetstream us-west-2 --data /mnt/ufos-db/
```

nginx forward proxy for websocket (run this on another host):

```nginx


worker_processes  2;

pid nginx.pid;

events {
    worker_connections   2000;

    # use [ kqueue | epoll | /dev/poll | select | poll ];
    # use kqueue;
}

http {
    default_type  application/octet-stream;


    log_format main      '$remote_addr - $remote_user [$time_local] '
                         '"$request" $status $bytes_sent '
                         '"$http_referer" "$http_user_agent" '
                         '"$gzip_ratio"';

    log_format download  '$remote_addr - $remote_user [$time_local] '
                         '"$request" $status $bytes_sent '
                         '"$http_referer" "$http_user_agent" '
                         '"$http_range" "$sent_http_content_range"';

    client_header_timeout  3m;
    client_body_timeout    3m;
    send_timeout           3m;

    client_header_buffer_size    1k;
    large_client_header_buffers  4 4k;

    gzip on;
    gzip_min_length  1100;
    gzip_buffers     4 8k;
    gzip_types       text/plain;

    output_buffers   1 32k;
    postpone_output  1460;

    sendfile         on;
    tcp_nopush       on;
    tcp_nodelay      on;
    send_lowat       12000;

    keepalive_timeout  75 20;

    upstream websocket {
        server jetstream2.us-west.bsky.network:443;
    }

    server {
        listen        8080;

        access_log /dev/null;

        location / {
            proxy_pass https://websocket;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }
    }

}
```

running

```bash
RUST_LOG=info ./ufos --jetstream ws://192.168.1.139:8080/subscribe --jetstream-force --jetstream-no-zstd --data /mnt/ufos-data-blah/
```

try without info-level logs for better perf

running on bonilla

```bash
./ufos --jetstream us-west-2 --jetstream-force --data /mnt/ufos-data-no-compression-2/
```

(reusing data dir from angel-hair)


ipv6 is having some trouble. but also maybe there's a deadlock somewhere

```bash
sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1
```


---

## fuzzing

got bit by https://github.com/cloudflare/cardinality-estimator/pull/12, so now we have a fuzz target.

install cargo-fuzz and then

```bash
RUSTFLAGS="-Z sanitizer=address" cargo +nightly fuzz run cardinality_estimator
```

to fuzz the counts value things


## license

This work is dual-licensed under MIT and Apache 2.0. You can choose between one of them if you use this work.

`SPDX-License-Identifier: MIT OR Apache-2.0`
