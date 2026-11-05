# who am i

a little auth service for microcosm demos

**you probably SHOULD NOT USE THIS in any serious environment**

for now the deployment is restricted to microcosm -- expanding it for wider use likely requires solving a number of challenges that oauth exists for.


## a little auth service

- you drop an iframe and a short few lines of JS on your web page, and get a nice-ish atproto login prompt.
- if the user has ever authorized this service before (and within some expiration), they will be presented with an in-frame one-click option to proceed.
- otherwise they get bounced over to the normal atproto oauth flow (in a popup or new tab)
- you get a callback containing
    - a verified DID and handle
    - a JWT containing the same that can be verified by public key
- **no write permissions** or any atproto permissions at all, just a verified identity

**you probably SHOULD NOT USE THIS in any serious environment**


### problems

- clickjacking: if this were allowed on arbitrary domains, malicious sites could trick users into proving their atproto identity.
- all the other problems oauth exists to solve: it's a little tricky to hook around the oauth flow so there are probably some annoying attacks.
- auth in front of auth: it's just a bit awkward to run an auth service that acts as an intermediary for a more-real auth behind it, but that's worse, less secure, and doesn't conform to any standards.

so, **you probably SHOULD NOT USE THIS in any serious environment**


## why

sometimes you want to make a thing that people can use with an atproto identity, and you might not want to let them put in any else's identity. apps that operate on public data like skircle, cred.blue, and the microcosm spacedust notifications demo don't require any special permission to operate for any user, and that's sometimes fine, but sometimes creepy/stalker-y/etc.

to avoid building a small torment nexus for a microcosm demo (while also not wanting to get deep into oauth or operate a demo-specific auth backend), i made this little service to just get a verified identity.

note: **you probably SHOULD NOT USE THIS in any serious environment**

---

since the requirements (read-only, just verifying identity) seem modest, i was hoping that a fairly simple implementation could be Good Enough, but in the time that i was willing to spend on it, the simple version without major obvious weaknesses i was hoping for didn't emerge.

it's still nice to have an explicit opt-in on a per-demo basis for microcosm so it will be used for that. it's allow-listed for the microcosm domain however (so not deployed on any adversarial hosting pages), so it's simultaenously overkill and restrictive.

i will get back to oauth eventually and hopefully roll out a microcosm service to make it easy for clients (and demos), but there are a few more things in the pipeline to get to first.


### todo

provide a pubkey-signed JWT of the identity (just the DID as `sub` probably). (**you probably SHOULD NOT USE THIS in any serious environment**)


## building

for raspi 1 model b:

atrium-oauth uses reqwest with default tls config that requires openssl which `cross` doesn't have a good time getting the os deps for.

fortunately, simply *enabling* a differnent tls feature for reqwest actually stops the default problematic one from causing problems, so we have a `reqwest` direct dependency with a feature enabled, even though it's never imported into actual code,

it builds with

```bash
cross build --release --target arm-unknown-linux-gnueabihf
```
