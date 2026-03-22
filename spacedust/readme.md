# spacedust

links firehose

fyi: the current verison of spacedust is very lightweight, but also limited: it offers no replay window, and cannot emit delete events since it doesn't retain any history.

(it does offer link filtering by link subject (aka backlinks!) which is still pretty neat)

the next version of spacedust will be quite a large overhaul: a complete forward-link index with full-network replay and hydrated deletes. it won't be lightweight anymore but it's going to be a lot more powerful.

currently since there are a few spacedust consumers out there, and since it's so lightweight and stable, i'm planning to keep the existing version running on its pi4 host machine indefinitely.


## license

This work is dual-licensed under MIT and Apache 2.0. You can choose between one of them if you use this work.

`SPDX-License-Identifier: MIT OR Apache-2.0`
