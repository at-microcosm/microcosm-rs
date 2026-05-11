# constellation 🌌

A global atproto backlink index ✨

- Self hostable: handles the full write throughput of the global atproto firehose on a raspberry pi 4b + single SSD
- Storage efficient: less than 2GB/day disk consumption indexing all references in all lexicons and all non-atproto URLs
- Handles record deletion, account de/re-activation, and account deletion, ensuring accurate link counts and respecting users data choices
- Simple JSON API

All social interactions in atproto tend to be represented by links (or references) between PDS records. This index can answer questions like "how many likes does a bsky post have", "who follows an account", "what are all the comments on a [frontpage](https://frontpage.fyi/) post", and more.

- **status**: works! api is unstable and likely to change, and no known instances have a full network backfill yet.
- source: [./constellation/](./constellation/)
- public instance: [constellation.microcosm.blue](https://constellation.microcosm.blue/)

_note: the public instance currently runs on a little raspberry pi in my house, feel free to use it! it comes with only with best-effort uptime, no commitment to not breaking the api for now, and possible rate-limiting. if you want to be nice you can put your project name and bsky username (or email) in your user-agent header for api requests._


## API endpoints

currently this is a bit out of date -- refer to the [api docs hosted by the app itself](https://constellation.microcosm.blue/) for now. they also let you try out live requests.

terms as used here:

- "URI": a URI, AT-URI, or DID.
- "JSON path": a dot-separated (and dot-prefixed, for now) path to a field in an atproto record. Arrays are noted by `[]` and cannot contain a specific index.

### `GET /links/count`

The number of backlinks to a URI from a specified collection + json path.

#### Required URL parameters

- `target` (required): the URI. must be URL-encoded.
  - example: `at%3A%2F%2Fdid%3Aplc%3A57vlzz2egy6eqr4nksacmbht%2Fapp.bsky.feed.post%2F3lg2pgq3gq22b`
- `collection` (required): the source NSID of referring documents to consider.
  - example: `app.bsky.feed.post`
- `path` (required): the JSON path in referring documents to consider.
  - example: `.subject.uri`

#### Response

A number (u64) in plain text format

#### cURL example: Get a count of all bluesky likes for a post

```bash
curl '<HOST>/links/count?target=at%3A%2F%2Fdid%3Aplc%3A57vlzz2egy6eqr4nksacmbht%2Fapp.bsky.feed.post%2F3lg2pgq3gq22b&collection=app.bsky.feed.like&path=.subject.uri'

40
```

### `GET /links/all/count`

The number of backlinks to a URI from any source collection or json path

#### Required URL parameters

- `target` (required): the URI. must be URL-encoded.
  - example: `did:plc:vc7f4oafdgxsihk4cry2xpze`

#### Response

A JSON object `{[NSID]: {[JSON path]: [N]}}`

#### cURL example: Get reference counts to a DID from any collection at any path

```bash
curl '<HOST>/links/all/count?target=did:plc:vc7f4oafdgxsihk4cry2xpze'

curl '<HOST>/links/all/count?target=did:plc:vc7f4oafdgxsihk4cry2xpze'
{
    "app.bsky.graph.block": { ".subject": 13 },
    "app.bsky.graph.follow": { ".subject": 159 },
    "app.bsky.feed.post": { ".facets[].features[].did": 16 },
    "app.bsky.graph.listitem": { ".subject": 6 },
    "app.bsky.graph.starterpack":
    {
        ".feeds[].creator.did": 1,
        ".feeds[].creator.labels[].src": 1
    }
}
```


## Contributions

### Licensing

Constellation's source code is currently available exclusively under the AGPL license (see [LICENSE](./LICENSE)).

In the future, its code MAY become available under the MIT and/or Apache2.0 licenses, at the sole discretion of the microcosm organization. Contributing implies acceptance with this possible future licensing change. The change has not happed yet and is not guaranteed.


some todos

- [x] find links and write them to rocksdb
- [x] handle account active status
- [x] handle account deletion
- [ ] handle account privacy setting? (is this a bsky-nsid-specific config and should that matter?)
  - instead of looking this up, should be able to listen for it to be published on the firehose.
    - this should _work_, but without backfill it won't be accurate. targeted backfill might be an option.
- [x] move ownership of canonical seq to an owned non-atomic
- [x] custom path for db storage
- [x] api server to look up backlink count
- [~] other useful endpoints for the api server
  - [x] show all nisd/path links to target
  - [x] get backlinking dids
  - [x] paging for all backlinking dids
  - [x] get count + most recent dids
  - [ ] get count with any dids from provided set
- [~] write this readme
- [?] fix it sometimes getting stuck
  - seems to unstick in my possibly-different repro (letting laptop fall asleep) after a bit.
  - [ ] add a detection for no new links coming in after some period
  - [x] add tcp connect, read, and write timeouts 🤞
- [x] handle jetstream restart: don't miss events (currently sketch: rewinds cursor by 1us so we will always double-count at least one event)
  - [x] especially: figure out what the risk is to rotating to another jetstream server in terms of gap/overlap from a different jetstream instance's cursor (follow up separately)
  - [x] jetstream: don't rotate servers, explicitly pass via cli
- [x] metrics!
  - [x] event ts lag
- [x] machine resource metrics
  - [x] disk consumption
  - [x] cpu usage
  - [x] mem usage
  - [x] network?
- [x] make all rocks apis return Result instead of unwrapping
- [~] handle all the unwraps
- [ ] deadletter queue of some kind for failed db writes
  - [ ] also for valid json that was rejected?
- [x] get it running on raspi
- [x] get an estimate of disk usage per day after a few days of running
  - very close to 1GB with data model before adding rkeys to linkers + fixing paths
- [x] make the did_init check only happen on test config (or remove it) (removed)
- [ ] actual error types (thiserror?) for lib-ish code
- [~] clean up the main readme
- [x] web server metrics
  - [x] origin and ua labels
- [ ] tokio metrics?
- [x] handle shutdown cleanly -- be nice to rocksdb
- [x] add user-agent to jetstream request
- [ ] wow the shutdown stuff i wrote is really bad and doesn't work a lot
- [x] serve html for browser requests
- [ ] add a health check endpoint
- [x] add seq numbers to metrics
- [ ] persist the jetstream server url, error if started with a different one (maybe with --switch-streams or something)
- [ ] put delete-account tasks into a separate (persisted?) task queue for the writer so it can work on them incrementally.
- [ ] jetstream: connect retry: only reset counter after some *time* has passed.
- [x] either count or estimate the total number of links added (distinct from link targets)
- [x] jetstream: don't crash on connection refused (retry * backoff)
- [x] allow cors requests (ie. atproto-browser. (but it's really meant for backends))
- [x] api: get distinct linking dids (https://bsky.app/profile/bnewbold.net/post/3lhhzejv7zc2h)
  - [x] endpoint for count
  - [x] endpoint for listing them
  - [x] add to exploratory /all endpoint
- [ ] nginx: support http2
- [x] nginx metrics
- [ ] add TimeoutLayer for axum
- [~] rocksdb metrics
  - [x] write ops (count? per actionable?)
  - [x] write time hist
  - [ ] read ops (api)
  - [ ] expose internal stats?
- [ ] figure out what's the right thing to do if merge op fails. happened on startup after an unclean reboot.
- [x] backups!
  - [x] manual backup on startup
  - [x] background task to create backups on an interval
- [ ] add a low-ulimit check on startup?

cache
- [ ] set api response headers
  - [ ] put "stale-while-revalidate" in Cache-Control w/ num seconds
  - [ ] put "stale-if-error" in Cache-Control w/ num seconds
  - [ ] set Expires or Cache-Control expires
  - [ ] add Accept to vary response
- [ ] cache vary: might need to take bsky account privacy setting into account (unless this ends up being in query)

data fixes
- [x] add rkey to linkers 🤦‍♀️
- [x] don't remove deleted links from the reverse records -- null them out. this will keep things stable for paging.
- [x] don't show deactivated accounts in link responses
- [ ] canonicalize handles to dids!
- [ ] links:
  - [~] pull `$type`/`type` from object children of arrays (distinguish replies, quotes, etc)
    - just $type to start
  - [ ] rewrite the entire "path" stuff
    - [ ] actually define the format (deal with in-band dots etc)
    - [x] ~_could_ throw cid neighbour into the target. probably should? but it's a lot of high volume uncompressible bytes~
      - and it could be looked up from the linker's doc
      - ^^ for now, look up from source doc to get cid. might revisit this later.
