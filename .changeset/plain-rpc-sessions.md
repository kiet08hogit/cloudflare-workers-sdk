---
"miniflare": patch
---

Release plain cross-process service binding results when they contain no stubs

Local dev sessions were keeping two loopback connections open for every plain object returned over a service binding until the caller's request ended. Those results are now copied and disposed as soon as they arrive. Results that still contain a remote stub keep their connection.
