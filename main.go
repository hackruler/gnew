// gnew: append only lines that are not already in a file. Built for 24M+ line files, sub-second scale.
// Parallel parse via channel (no big bucket copy), flat span storage, compact int32 spans.
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
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

const (
	defaultReadBuf  = 8 << 20  // 8 MiB input
	defaultWriteBuf = 4 << 20  // 4 MiB output
	defaultNewBuf   = 16 << 20 // 16 MiB per chunk (balance: fewer allocs, reasonable RAM)
	numPartitions   = 256      // lock-free lookup by hash % N
	noNext          = -1       // end of collision chain
	buildChanBuf    = 1024     // per-partition channel buffer; 256 partitions = 256k in flight
)

func main() {
	outputPath := flag.String("o", "", "output file (default: append to existing file)")
	trimSpace := flag.Bool("trim", false, "trim spaces when comparing")
	quiet := flag.Bool("q", false, "quiet: no output (only exit code)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: gnew <existing-file> [-o out] [-trim] [-q]\n")
		os.Exit(1)
	}
	existingPath := args[0]
	if *outputPath == "" {
		*outputPath = existingPath
	}

	_, err := run(existingPath, *outputPath, *trimSpace, *quiet)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gnew: %v\n", err)
		os.Exit(1)
	}
}

type stats struct {
	Existing int64
	Input    int64
	New      int64
	Written  int64
}

// spanCompact: flat list per partition; next is index of next span for same hash (noNext = end).
// int32 for start/end (file < 2GB); one slice header for all spans instead of one per key.
type spanCompact struct {
	start, end int32
	chunkIdx   int16 // -1 = existingBuf, else newChunks[chunkIdx]
	next       int32
}

func run(existingPath, outputPath string, trim, quiet bool) (*stats, error) {
	showInserted := !quiet
	s := &stats{}

	existingBuf, err := os.ReadFile(existingPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		existingBuf = nil
	}

	// Partitioned set: 256 maps, build in parallel; lookup is hash % 256, no locks
	set := newPartitionedSet(existingBuf, numPartitions)
	if existingBuf != nil {
		n := buildSetParallel(existingBuf, trim, set)
		s.Existing = int64(n)
	}

	out, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	defer out.Close()
	w := bufio.NewWriterSize(out, defaultWriteBuf)
	defer w.Flush()

	// Stdin: Scanner with 8 MiB buffer (fast batch read)
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 64<<10), defaultReadBuf)
	for sc.Scan() {
		line := sc.Bytes()
		if trim {
			line = bytes.TrimSpace(line)
		}
		if len(line) == 0 {
			continue
		}
		s.Input++
		if set.addIfNew(line) {
			s.New++
			if showInserted {
				os.Stderr.Write(line)
				os.Stderr.Write([]byte{'\n'})
			}
			_, _ = w.Write(line)
			w.WriteByte('\n')
			s.Written++
		}
	}
	if err := sc.Err(); err != nil && err != io.EOF {
		return s, err
	}
	return s, nil
}

// buildEntry: one line for the parallel build channel (bounded; no full-file bucket copy).
type buildEntry struct {
	part       int
	h          uint64
	start, end int32
}

// advanceToNextLine returns the byte offset of the first character of the next line after pos.
func advanceToNextLine(buf []byte, pos int) int {
	for pos < len(buf) && buf[pos] != '\n' {
		pos++
	}
	if pos < len(buf) {
		pos++
	}
	return pos
}

// buildSetParallel: workers parse chunks; each of 256 partition consumers fills its own part in parallel.
// No single consumer bottleneck; bounded per-partition channels.
func buildSetParallel(buf []byte, trim bool, set *partitionedSet) int {
	numWorkers := runtime.GOMAXPROCS(0)
	if numWorkers <= 0 {
		numWorkers = 1
	}
	if len(buf) == 0 {
		return 0
	}

	// One channel per partition so 256 consumers run in parallel.
	var chs [numPartitions]chan buildEntry
	for i := range chs {
		chs[i] = make(chan buildEntry, buildChanBuf)
	}

	// Line-aligned chunk starts so no worker splits a line.
	starts := make([]int, numWorkers+1)
	starts[0] = 0
	starts[numWorkers] = len(buf)
	for i := 1; i < numWorkers; i++ {
		approx := i * len(buf) / numWorkers
		starts[i] = advanceToNextLine(buf, approx)
	}

	var prodWg sync.WaitGroup
	hash := xxhash.Sum64

	for w := 0; w < numWorkers; w++ {
		lo, hi := starts[w], starts[w+1]
		if lo >= hi {
			continue
		}
		prodWg.Add(1)
		go func(lo, hi int) {
			defer prodWg.Done()
			start := lo
			for start < hi {
				i := bytes.IndexByte(buf[start:], '\n')
				if i < 0 {
					i = len(buf) - start
				}
				p, q := start, start+i
				if trim {
					for p < q && (buf[p] == ' ' || buf[p] == '\t' || buf[p] == '\r') {
						p++
					}
					for q > p && (buf[q-1] == ' ' || buf[q-1] == '\t' || buf[q-1] == '\r') {
						q--
					}
				}
				if p < q {
					line := buf[p:q]
					h := hash(line)
					b := h % numPartitions
					chs[b] <- buildEntry{part: int(b), h: h, start: int32(p), end: int32(q)}
				}
				start = start + i + 1
			}
		}(lo, hi)
	}

	var n atomic.Int64
	var consWg sync.WaitGroup
	for i := 0; i < numPartitions; i++ {
		consWg.Add(1)
		go func(part int) {
			defer consWg.Done()
			for e := range chs[part] {
				p := &set.parts[part]
				prev := p.m[e.h]
				p.spans = append(p.spans, spanCompact{
					start:    e.start,
					end:      e.end,
					chunkIdx: -1,
					next:     prev - 1,
				})
				p.m[e.h] = int32(len(p.spans))
				n.Add(1)
			}
		}(i)
	}

	prodWg.Wait()
	for i := range chs {
		close(chs[i])
	}
	consWg.Wait()
	return int(n.Load())
}

// partition: flat span list + map from hash to first index. No per-key []span = huge RAM save.
type partition struct {
	m     map[uint64]int32
	spans []spanCompact
}

type partitionedSet struct {
	existingBuf []byte
	newChunks   [][]byte
	parts       []partition
}

func newPartitionedSet(existingBuf []byte, n int) *partitionedSet {
	parts := make([]partition, n)
	capPer := 25 << 20 / n
	if capPer < 1 << 10 {
		capPer = 1 << 10
	}
	for i := range parts {
		parts[i].m = make(map[uint64]int32, capPer)
		parts[i].spans = make([]spanCompact, 0, capPer)
	}
	return &partitionedSet{
		existingBuf: existingBuf,
		newChunks:   make([][]byte, 0, 32),
		parts:       parts,
	}
}

func (s *partitionedSet) getLine(part int, idx int32) []byte {
	sp := &s.parts[part].spans[idx]
	if sp.chunkIdx < 0 {
		return s.existingBuf[sp.start:sp.end]
	}
	chunk := s.newChunks[sp.chunkIdx]
	return chunk[sp.start:sp.end]
}

// addIfNew returns true if line was new and is now added.
func (s *partitionedSet) addIfNew(line []byte) bool {
	h := xxhash.Sum64(line)
	p := h % numPartitions
	idx := s.parts[p].m[h]
	if idx == 0 {
		s.addNew(line, h, int(p))
		return true
	}
	idx-- // 1-based -> 0-based
	for idx != noNext {
		if bytes.Equal(s.getLine(int(p), idx), line) {
			return false
		}
		idx = s.parts[p].spans[idx].next
	}
	s.addNew(line, h, int(p))
	return true
}

func (s *partitionedSet) addNew(line []byte, h uint64, part int) {
	chunkIdx := len(s.newChunks) - 1
	if chunkIdx < 0 {
		s.newChunks = append(s.newChunks, make([]byte, 0, defaultNewBuf))
		chunkIdx = 0
	}
	cur := &s.newChunks[chunkIdx]
	if len(*cur)+len(line) > cap(*cur) {
		s.newChunks = append(s.newChunks, make([]byte, 0, defaultNewBuf))
		chunkIdx = len(s.newChunks) - 1
		cur = &s.newChunks[chunkIdx]
	}
	start := len(*cur)
	*cur = append(*cur, line...)
	prev := s.parts[part].m[h] // 1-based, 0 = absent
	s.parts[part].spans = append(s.parts[part].spans, spanCompact{
		start:    int32(start),
		end:      int32(len(*cur)),
		chunkIdx: int16(chunkIdx),
		next:     prev - 1,
	})
	s.parts[part].m[h] = int32(len(s.parts[part].spans))
}