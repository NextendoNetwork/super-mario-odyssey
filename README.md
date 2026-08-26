<h1 align="center">super-mario-odyssey</h1>

<p align="center">
  <b>Nextendo Network game server for Super Mario Odyssey.</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/license-PolyForm%20Shield%201.0.0-orange" alt="License: PolyForm Shield 1.0.0">
  <img src="https://img.shields.io/badge/go-1.23%2B-00ADD8" alt="Go 1.23+">
</p>

---

## What is this?

The NEX game server for **Super Mario Odyssey** on
[Nextendo Network](https://nextendo.network), on the same from-scratch
[**nextendo-nex**](https://github.com/NextendoNetwork/nextendo-nex) NEX stack as
[`arms`](https://github.com/NextendoNetwork/arms)/MPS/SMB35/Tennis Aces/SSBU. No third-party NEX
library involved.

## What's different about this title

Every other game server in this codebase spends most of its effort on matchmaking (lobbies,
NAT traversal, session search). Odyssey has **none of that** — its only online feature is
**Balloon World**: place a "capture" balloon (a photo + costume snapshot at some in-game
location) via DataStore, and other players search for and pop it. There is no live session,
no lobby, no P2P. The entire feature surface is the DataStore protocol (`0x73`).

That means this server is architecturally simpler in one direction (no
Matchmaking/MatchmakeExtension/NATTraversal registered at all — see `main.go`) and more
involved in another: every other title in this codebase treats DataStore as an all-but-empty
stub (see `arms_stubs.go`'s "nextendo-nex has no DataStore implementation" comment). Here it
has to be real, because it *is* the game.

## Three servers, one process

| Server | Port | Role |
| --- | --- | --- |
| Auth | `:8453` | TicketGranting — `LoginEx` issues the Kerberos ticket |
| Secure | `:60013` | SecureConnection + Utility + Ranking + the real DataStore (`0x73`) |
| Object store | `:8459` | HTTPS blob host standing in for the S3 bucket real Nintendo servers point `PreparePostObject`/`PrepareGetObject` at |

Real Nintendo backs DataStore objects with S3: the NEX server only tracks metadata and issues
a signed upload/download URL, and the actual bytes (the capture-pose screenshot + costume
data) never touch it. We don't have S3, so `objectstore.go` is a tiny local HTTPS file host on
its own port; the URLs handed back in `DataStoreReqPostInfo`/`DataStoreReqGetInfo` point at
`https://<NEXTENDO_HOST>:<OBJECT_PORT>/object/<dataId>` instead of Amazon, gated by a
short-lived per-request token (`X-Object-Token` header) rather than a signed query string.

## Identity

- Game Server ID `255BA201`, access key `afef0ecf` — from the
  [kinnay/NintendoClients wiki Game Server List](https://github.com/kinnay/NintendoClients/wiki/Game-Server-List),
  **not** independently confirmed against a real Odyssey binary the way ARMS's key was pulled
  from its `.rodata`. Treat as reference-verified, not binary-verified.
- NEX version defaults to `40000` (`SMO_NEX_VERSION`) — the wiki's Game Server List doesn't
  record a per-title NEX version, so this borrows the value this codebase's own dashboard
  already uses for the contemporary Splatoon 2/ACNH/MK8D era. Unconfirmed; the first thing to
  try changing if `LoginEx` never completes.
- Title ID `0100000000010000`.

## DataStore: what's real vs. stubbed

Implemented for real (see `datastore.go`), backed by an in-memory map flushed to
`SMO_DATASTORE_FILE` every 5s:

- `GetMeta` / `GetMetas` (8/9)
- `PreparePostObject` / `CompletePostObject` / `PrepareGetObject` (24/26/25) — the actual
  place/find upload-download flow, wired to the object store above
- `DeleteObject` (4)
- `SearchObject` (12) — generic search, filtered by `dataType` only (the rest of
  `DataStoreSearchParam`'s fields aren't decoded, same "don't risk desyncing on an unconfirmed
  layout" call `ranking.go` makes for `GetRanking`)
- `RateObject` / `GetRating` (15/16) — Balloon World's rating claps
- The full SMO extension (47–53): `AddToBufferQueue(s)` / `GetBufferQueue(s)` /
  `ClearBufferQueues` (the capture-pose screenshot buffers), `SearchBalloon`, `FetchMyInfos`

Everything else on the base DataStore method list (46 methods total — see the wiki) falls
through to `notImplementedDS`, logged with the full method ID + body so a real capture can
fill it in fast if Odyssey turns out to need it.

**None of the wire shapes here have been cross-checked against a real Odyssey capture** —
they're transcribed from the kinnay wiki's documented (generic NEX, not Odyssey-specific)
layout. Field-level mistakes are plausible, especially around `SearchBalloon`'s result
bucketing (the exact per-kingdom/rank grouping rule Nintendo uses isn't known) and
`AddToBufferQueue`'s `BufferQueueParam` slot field width. Every DataStore call is logged with
proto+method+pid+body length so a live test quickly shows what needs correcting.

## Running

```sh
cp example.env .env    # then edit .env
go run .
```

Configuration is entirely through environment variables — see [`example.env`](example.env). No
secrets are baked into the source.

If running behind [`sni-router`](https://github.com/NextendoNetwork/sni-router), it needs a route
added for Odyssey's SNI hostname once that's known/observed.

## What this is not

This server ships **no** Nintendo code, keys, or copyrighted assets. It is an independent
reimplementation for use with a community-run replacement service, not affiliated with, endorsed by,
or associated with Nintendo. The NEX access key it uses is a well-known per-title value derivable
from the game itself, not a secret.

## License

Released under the **[PolyForm Shield License 1.0.0](LICENSE.md)** — source-available: read, use,
modify, and self-host, but do not use it to provide a product that competes with Nextendo Network.
