// gnew: append only lines that are not already in a file.
// Optimized for very large files with low memory usage:
// - stream existing file (no full file load)
// - store only 64-bit hashes in an open-addressed set
// - stream stdin and append new lines in one pass
//
// Usage:
//   gnew existing.txt                 # read stdin, append new lines to existing.txt
//   gnew existing.txt -o out.txt      # write new uniques to out.txt
//   cat new.txt | gnew existing.txt
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/zeebo/xxh3"
	"github.com/zeebo/wyhash"
	"go.dw1.io/rapidhash"
)

const (
	defaultReadBuf  = 8 << 20 // 8 MiB scanner line limit
	defaultWriteBuf = 4 << 20 // 4 MiB output buffer
)

func main() {
	outputPath := flag.String("o", "", "output file (default: append to existing file)")
	trimSpace := flag.Bool("trim", false, "trim spaces when comparing")
	quiet := flag.Bool("q", false, "quiet: no output (only exit code)")
	hashAlgo := flag.String("hash", "rapidhash", "hash algorithm: rapidhash|wyhash|xxhash|xxh3")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: gnew <existing-file> [-o out] [-trim] [-q] [-hash rapidhash|wyhash|xxhash|xxh3]\n")
		os.Exit(1)
	}
	existingPath := args[0]
	if *outputPath == "" {
		*outputPath = existingPath
	}

	hasher, err := getHasher(*hashAlgo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gnew: %v\n", err)
		os.Exit(1)
	}

	_, err = run(existingPath, *outputPath, *trimSpace, *quiet, hasher)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gnew: %v\n", err)
		os.Exit(1)
	}
}

type hashFunc func([]byte) uint64

func run(existingPath, outputPath string, trim, quiet bool, hash hashFunc) (int64, error) {
	showInserted := !quiet
	set := newHashSet(1024)

	if err := loadExistingHashes(existingPath, trim, set, hash); err != nil {
		return 0, err
	}

	out, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	w := bufio.NewWriterSize(out, defaultWriteBuf)
	defer func() { _ = w.Flush() }()

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64<<10), defaultReadBuf)
	var written int64

	for sc.Scan() {
		line := sc.Bytes()
		if trim {
			line = bytes.TrimSpace(line)
		}
		if len(line) == 0 {
			continue
		}
		if set.AddHash(hashLine(line, hash)) {
			if showInserted {
				_, _ = os.Stderr.Write(line)
				_, _ = os.Stderr.Write([]byte{'\n'})
			}
			_, _ = w.Write(line)
			_ = w.WriteByte('\n')
			written++
		}
	}

	if err := sc.Err(); err != nil {
		return written, err
	}
	if err := w.Flush(); err != nil {
		return written, err
	}
	return written, nil
}

func loadExistingHashes(path string, trim bool, set *hashSet, hash hashFunc) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), defaultReadBuf)
	for sc.Scan() {
		line := sc.Bytes()
		if trim {
			line = bytes.TrimSpace(line)
		}
		if len(line) == 0 {
			continue
		}
		set.AddHash(hashLine(line, hash))
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return nil
}

func hashLine(line []byte, hash hashFunc) uint64 {
	h := hash(line)
	if h == 0 {
		return 1 // keep zero as empty-slot sentinel
	}
	return h
}

func getHasher(name string) (hashFunc, error) {
	switch name {
	case "rapidhash":
		return rapidhash.Hash, nil
	case "wyhash":
		return func(b []byte) uint64 { return wyhash.Hash(b, 0) }, nil
	case "xxhash":
		return xxhash.Sum64, nil
	case "xxh3":
		return xxh3.Hash, nil
	default:
		return nil, fmt.Errorf("invalid -hash value %q (use rapidhash|wyhash|xxhash|xxh3)", name)
	}
}

type hashSet struct {
	keys []uint64 // 0 == empty slot
	used int
}

func newHashSet(minCap int) *hashSet {
	if minCap < 16 {
		minCap = 16
	}
	size := nextPow2(minCap)
	return &hashSet{keys: make([]uint64, size)}
}

func (s *hashSet) AddHash(h uint64) bool {
	// keep max load ~70% for speed and short probe chains
	if (s.used+1)*10 >= len(s.keys)*7 {
		s.grow()
	}

	mask := uint64(len(s.keys) - 1)
	i := h & mask
	for {
		k := s.keys[i]
		if k == 0 {
			s.keys[i] = h
			s.used++
			return true
		}
		if k == h {
			return false
		}
		i = (i + 1) & mask
	}
}

func (s *hashSet) grow() {
	old := s.keys
	s.keys = make([]uint64, len(old)*2)
	s.used = 0
	mask := uint64(len(s.keys) - 1)

	for _, h := range old {
		if h == 0 {
			continue
		}
		i := h & mask
		for s.keys[i] != 0 {
			i = (i + 1) & mask
		}
		s.keys[i] = h
		s.used++
	}
}

func nextPow2(n int) int {
	x := 1
	for x < n {
		x <<= 1
	}
	return x
}