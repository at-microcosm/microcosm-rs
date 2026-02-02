microcosm HTTP APIs + rust crates
=================================
[![@microcosm.blue: bluesky](https://img.shields.io/badge/@microcosm.blue-bluesky-blue)](https://bsky.app/profile/microcosm.blue)
[![microcosm discord: join](https://img.shields.io/badge/microcosm_discord-join-purple)](https://discord.gg/tcDfe4PGVB)
[![github sponsors: support](https://img.shields.io/badge/github_sponsors-support-pink)](https://github.com/sponsors/uniphil/)
[![ko-fi: support](https://img.shields.io/badge/ko--fi-support-pink)](https://ko-fi.com/bad_example)

Welcome! Documentation is under active development. If you like reading API docs, you'll probably hit the ground running!

Tutorials, how-to guides, and client SDK libraries are all in the works for gentler on-ramps, but are not quite ready yet. But don't let that stop you! Hop in the [microcosm discord](https://discord.gg/tcDfe4PGVB), or post questions and tag [@bad-example.com](https://bsky.app/profile/bad-example.com) on Bluesky if you get stuck anywhere.

> [!tip]
> This repository's primary home is moving to tangled: [@microcosm.blue/microcosm-rs](https://tangled.org/microcosm.blue/microcosm-rs). It will continue to be mirrored on [github](https://github.com/at-microcosm/microcosm-rs) for the forseeable future, and it's fine to open issues or pulls in either place!


🌌 [Constellation](./constellation/)
------------------------------------

A global atproto interactions backlink index as a simple JSON API. Works with every lexicon, runs on a raspberry pi, consumes less than 2GiB of disk per day. Handles record deletion, account de/re-activation, and account deletion, ensuring accurate link counts while respecting users' data choices.

- Source: [./constellation/](./constellation/)
- [Public instance/API docs](https://constellation.microcosm.blue/)
- Status: used in production. APIs will change but backwards compatibility will be maintained as long as needed.


🎇 [Spacedust](./spacedust/)
----------------------------

A global atproto interactions firehose. Extracts all at-uris, DIDs, and URLs from every lexicon in the firehose, and exposes them over a websocket modelled after [jetstream](github.com/bluesky-social/jetstream).

- Source: [./spacedust/](./spacedust/)
- [Public instance/API docs](https://spacedust.microcosm.blue/)
- Status: v0: the basics work and the APIs are in place! missing cursor replay, forward link storage, and delete event link hydration.

### Demos:

- [Spacedust notifications](https://notifications.microcosm.blue/): web push notifications for _every_ atproto app
- [Zero-Bluesky real-time interaction-updating post embed](https://bsky.bad-example.com/zero-bluesky-realtime-embed/)


🛰️ [Slingshot](./slingshot)
---------------------------

A fast, eager, production-grade edge cache for atproto records and identities. Pre-caches all records from the firehose and maintains a longer-term cache of requested records on disk.

- Source: [./slingshot/](./slingshot/)
- [Public instance/API docs](https://slingshot.microcosm.blue/)
- Status: v0: most XRPC APIs are working. cache storage is being reworked.


🛸 [UFOs API](./ufos)
---------------------

Timeseries stats and sample records for every [collection](https://atproto.com/guides/glossary#collection) ever seen in the atproto firehose. Unique users are counted in hyperloglog sketches enabling arbitrary cardinality aggregation across time buckets and/or NSIDs.

- Source: [./ufos/](./ufos/)
- [Public instance/API docs](https://ufos-api.microcosm.blue/)
- Status: Used in production. It has APIs and they work! Needs improvement on indexing; needs more indexes and some more APIs to the data exposed.

> [!tip]
>  See also: [UFOs atproto explorer](https://ufos.microcosm.blue/) built on UFOs API. ([source](github.com/at-microcosm/spacedust-utils))


💫 [Links](./links)
-------------------

Rust library for parsing and extracting links (at-uris, DIDs, and URLs) from atproto records.

- Source: [./links/](./links/)
- Status: not yet published to crates.io; needs some rework


🛩️ [Jetstream](./jetstream)
---------------------------

A low-overhead jetstream client with cursor handling and automatic reconnect.

- Source: [./links/](./links/)
- Status: used in multiple apps in production, but not yet published to crates.io; some rework planned

> [!tip]
> See also: [Rocketman](https://github.com/teal-fm/cadet/tree/main/rocketman), another excellent rust jetstream client which shares some lineage and _is_ published on crates.io.



🔭 Deprecated: [Who am I](./who-am-i)
-------------------------------------

An identity bridge for microcosm demos, that kinda worked. Fixing its problems is about equivalent to reinventing a lot of OIDC, so it's being retired.

- Source: [./who-am-i/](./who-am-i/)
- Status: ready for retirement.

> [!warning]
> `who-am-i` is still in use for the Spacedust Notifications demo, but that will hopefully be migrated to use atproto oauth directly instead.
