_A [gravitational slingshot](https://en.wikipedia.org/wiki/Gravity_assist) makes use of the gravity and relative movements of celestial bodies to accelerate a spacecraft and change its trajectory._


# Slingshot: edge record cache

Applications in [ATProtocol](https://atproto.com/) store data in users' own [PDS](https://atproto.com/guides/self-hosting) (Personal Data Server), which are distributed across thousands of independently-run servers all over the world. Trying to access this data poses challenges for client applications:

- A PDS might be far away with long network latency
- or may be on an unreliable connection
- or overloaded when you need it, or offline, or…

Large projects like [Bluesky](https://bsky.app/) control their performance and reliability by syncing all app-relevant data from PDSs into first-party databases. But for new apps, building out this additional data infrastructure adds significant effort and complexity up front.

**Slingshot is a fast, eager, production-grade cache of data in the [ATmosphere](https://atproto.com/)**, offering performance and reliability without custom infrastructure.


### Current status

> [!important]
> Slingshot is currently in a **v0, pre-release state**. There is one production instance and you can use it! Expect short downtimes for restarts as development progresses and lower cache hit-rates as the internal storage caches are adjusted and reset.

The core APIs will not change, since they are standard third-party `com.atproto` query APIs from ATProtocol.


## Eager caching

In many cases, Slingshot can cache the data you need *before* first request!

Slingshot subscribes to the global [Firehose](https://atproto.com/specs/sync#firehose) of data updates. It keeps a short-term rolling indexed window of *all* data, and automatically promotes content likely to be requested to its longer-term main cache. _(automatic promotion is still a work in progress)_

When there is a cache miss, Slingshot can often still accelerate record fetching, since it keeps a large cache of resolved identities: it can usually request from the correct PDS without extra lookups.


## Precise invalidation

The fireshose includes **update** and **delete** events, which Slingshot uses to ensure stale and deleted data is removed within a very short window. Additonally, identity and account-level events can trigger rapid cleanup of data for deactivated and deleted accounts. _(some of this is still a work in progress)_


## Low-trust

The "AT" in ATProtocol [stands for _Authenticated Transfer_](https://atproto.com/guides/glossary#at-protocol): all data is cryptographically signed, which makes it possible to broadcast data through third parties and trust that it's real _without_ having to directly contact the originating server.

Two core standard query APIs are supported to balance convenience and trust. They both fetch [records](https://atproto.com/guides/glossary#record):

### [`com.atproto.repo.getRecord`](#tag/comatproto-queries/get/xrpc/com.atproto.repo.getRecord)

- convenient `JSON` response format
- cannot be proven authentic

### [`com.atproto.sync.getRecord`](#tag/comatproto-queries/get/xrpc/com.atproto.sync.getRecord)

- [`DAG-CBOR`](https://atproto.com/specs/data-model)-encoded response requires extra libraries to decode, but
- includes a cryptographic proof of authenticity!

_(work on this endpoint is in progress)_


## Service proxying

Clients can proxy atproto queries through their own PDS with [Service Proxying](https://atproto.com/specs/xrpc#service-proxying), and this is supported by Slingshot. The Slingshot instance must be started the `--domain` argument specified.

Service-proxied requests can specify a Slingshot instance via the `atproto-proxy` header:

```http
GET /xrpc/com.bad-example.identity.resolveMiniDoc?identifier=bad-example.com
Host: <your pds>
atproto-proxy: did:web:<slingshot domain>#slingshot
```

Where `<your pds>` is the user's own PDS host, and `<slingshot domain>` is the domain that the slingshot instance is deployed at (eg. `slingshot.microcosm.blue`). See the [Service Proxying](https://atproto.com/specs/xrpc#service-proxying) docs for more.

> [!tip]
> Service proxying is supported but completely optional. All APIs are directly accessible over the public internet, and GeoDNS helps route users to the closest instance to them for the lowest possible latency. (_note: deploying multiple slingshot instances with GeoDNS is still TODO_)


## Ergonomic APIs

- Slingshot also offers variants of the `getRecord` endpoints that accept a full `at-uri` as a parameter, to save clients from needing to parse and validate all parts of a record location.

- Bi-directionally verifying identity endpoints, so you can directly exchange atproto [`handle`](https://atproto.com/guides/glossary#handle)s for [`DID`](https://atproto.com/guides/glossary#did-decentralized-id)s without extra steps, plus a convenient [Mini-Doc](#tag/slingshot-specific-queries/get/xrpc/com.bad-example.identity.resolveMiniDoc) verified identity summary.


## Part of microcosm

[Microcosm](https://www.microcosm.blue/) is a collection of services and independent community-run infrastructure for ATProtocol.

Slingshot excels when combined with _shallow indexing_ services, which offer fast queries of global data relationships but with only references to the data records. Microcosm has a few!

- [🌌 Constellation](https://constellation.microcosm.blue/), a global backlink index (all social interactions in atproto are links!)
- [🎇 Spacedust](https://spacedust.microcosm.blue/), a firehose of all social interactions

> [!success]
> All microcosm projects are [open source](https://tangled.org/bad-example.com/microcosm-links). **You can help sustain Slingshot** and all of microcosm by becoming a [Github sponsor](https://github.com/sponsors/uniphil/) or a [Ko-fi supporter](https://ko-fi.com/bad_example)!
