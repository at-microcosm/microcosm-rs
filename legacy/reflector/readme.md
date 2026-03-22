# reflector

a tiny did:web service server that maps subdomains to a single service endpoint

receiving requests from multiple subdomains is left as a problem for the reverse proxy to solve, since acme wildcard certificates (ie. letsencrypt) require the most complicated and involved challenge type (DNS).

caddy [has good support for](https://caddyserver.com/docs/caddyfile/patterns#wildcard-certificates) configuring the wildcard DNS challenge with various DNS providers, and also supports [on-demand](https://caddyserver.com/docs/automatic-https#using-on-demand-tls) provisioning via the simpler methods.

if you only need a small fixed number of subdomains, you can also use certbot or otherwise individually configure them in your reverse proxy.


## license

This work is dual-licensed under MIT and Apache 2.0. You can choose between one of them if you use this work.

`SPDX-License-Identifier: MIT OR Apache-2.0`
