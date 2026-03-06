# gnew

Append only **new** lines to a file (like `anew`), built for **24M+ line** files with minimal memory and maximum speed.

## Install

```bash
go install github.com/hackruler/gnew@latest
```

Ensure `$GOPATH/bin` or `$HOME/go/bin` is in your `PATH`.

## Why

- **Zero-copy for existing file**: The existing file is loaded once; lines are stored as (start,end) spans into that buffer. No 24M string allocations.
- **xxHash**: 64-bit hashing at ~17 GB/s; collision chains stay short.
- **Chunked new-line buffer**: New uniques are appended in 64 MiB chunks; no realloc so existing spans stay valid.
- **Large I/O buffers**: 2 MiB read, 1 MiB write by default.

## Usage

```bash
# Append new lines from stdin to existing.txt (default: append to that file)
cat new_lines.txt | ./gnew existing.txt

# Write new uniques to a different file
cat new_lines.txt | ./gnew existing.txt -o only_new.txt

# Trim spaces when comparing (e.g. " foo " == "foo")
cat new_lines.txt | ./gnew existing.txt -trim

# Quiet (only exit code; no stats to stderr)
cat new_lines.txt | ./gnew existing.txt -q
```

## Build

```bash
go build -o gnew .
```

## Flags

| Argument | Description |
|----------|-------------|
| `existing-file` | Path to existing file (required). If missing, treated as empty. |
| `-o`     | Output file. Default: same as existing file (append). |
| `-trim`  | Trim spaces when comparing lines. |
| `-q`     | Quiet: no stats on stderr. |

## Stats (stderr)

- **existing**: Lines already in the file.
- **input**: Lines read from stdin.
- **new**: Lines that were not in the file.
- **written**: Lines written to output.
