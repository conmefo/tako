# tako

A segmented download accelerator in Go, in the spirit of aria2 and axel. It splits a file into byte ranges, downloads them over several HTTP/1.1 connections at once, and writes each range at its own offset.

It is a learning project for networking, concurrency and file I/O.

## Features

- Parallel range downloads with work stealing: idle connections take half of the largest remaining chunk.
- Resume after Ctrl-C or a crash from a crash-safe state file, guarded by `If-Range`.
- Retries with exponential backoff and jitter, continuing from the bytes already received.
- Adaptive connection count: backs off on `429` / `503` and grows again later.
- A terminal dashboard with a speed graph, a per-connection chunk map and keys to pause or change the connection count.
- Falls back to one connection when the server doesn't support ranges.

## Install

```sh
go install github.com/conmefo/tako/cmd/tako@latest
```

## Usage

```sh
tako get [-n N] [-o FILE|DIR] [-q] [-plain] URL
tako info URL
tako version
```

`-n` sets the number of connections (default 4). Run the same command again to resume an interrupted download.

Dashboard keys: `p` pause/resume, `+` / `-` change the connection limit, `d` per-connection lanes, `l` event log, `q` save and quit.

## Test server

`takoserve` serves generated data or a file and can inject faults, which is handy for trying out retries and throttling:

```sh
go run ./cmd/takoserve -size 256MiB -rate 2MiB -drop 0.05 -429 0.02
tako get -n 8 http://127.0.0.1:8080/test.bin
```

Other flags: `-file`, `-stall`, `-max-conns`, `-no-ranges`, `-addr`.
