# tako

A segmented download accelerator in Go.

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
