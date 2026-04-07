# gnew

Append only **new** lines to a file (like `anew`), built for **24M+ line** files with minimal memory and maximum speed.

## Install

```bash
go install github.com/hackruler/gnew@latest
```

Ensure `$GOPATH/bin` or `$HOME/go/bin` is in your `PATH`.

## Why

- **Low RAM on huge files**: Existing file is scanned in streaming mode (no full-file load).
- **Compact dedupe index**: Uses an open-addressed `uint64` hash set to avoid high-overhead maps of strings/spans.
- **Fast hashing**: XXH3 gives very high throughput and stable performance on large inputs.
- **Buffered I/O**: Large scanner/writer buffers keep syscalls low.

## Trade-off

This implementation deduplicates by 64-bit hash (XXH3), which is extremely unlikely to collide in practice, but not mathematically impossible. In return, memory usage is dramatically lower than exact byte-span indexing.

## Usage

```bash
# Append new lines from stdin to existing.txt (default: append to that file)
cat new_lines.txt | ./gnew existing.txt

# Write new uniques to a different file
cat new_lines.txt | ./gnew existing.txt -o only_new.txt

# Trim spaces when comparing (e.g. " foo " == "foo")
cat new_lines.txt | ./gnew existing.txt -trim

# Quiet: no output (only exit code)
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
| `-q`     | Quiet: no output (only exit code). |

## Output

By default, gnew prints only the **newly added lines** to stderr (one per line). Use `-q` to suppress all output.
