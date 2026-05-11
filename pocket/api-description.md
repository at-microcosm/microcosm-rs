_A pocket dimension to stash a bit of non-public user data._


# Pocket: user preference storage

This API leverages atproto service proxying to offer a bit of per-user per-app non-public data storage.
Perfect for things like application preferences that might be better left out of the public PDS data.

The intent is to use oauth scopes to isolate storage on a per-application basis, and to allow easy data migration from a community hosted instance to your own if you end up needing that.


### Current status

> [!important]
> Pocket is currently in a **v0, pre-release state**. There is one production instance and you can use it! Expect short downtimes for restarts as development progresses and occaisional data loss until it's stable.

ATProto might end up adding a similar feature to [PDSs](https://atproto.com/guides/glossary#pds-personal-data-server). If/when that happens, you should use it instead of this!
