package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// Used to define the encoding we use to store records
	enc = binary.BigEndian
)

const (
	// Number of bytes used to store the records length
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// A wrapper around file we store the records in
// exposes two APIs, one to append to a file and
// another to read from the file
func newStore(f *os.File) (*store, error) {
	// Check if the file exists and get stats
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// Get the size of the file
	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var bytesWritten uint64

	// Set the current store size as the position
	pos = s.size

	// Write the length of the record to writer so that we know how
	// many bytes to read.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	bytesWritten += lenWidth

	nn, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	bytesWritten += uint64(nn)

	// Update store size
	s.size += bytesWritten
	return bytesWritten, pos, nil
}

// Read returns the record stored at a given position
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the writer buffer in case there is data which is
	// yet to be flushed
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Find out how many bytes we have to read to get the whole record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Create buffer to store the record
	record := make([]byte, enc.Uint64(size))

	// Read the record from pos + size bytes offset
	if _, err := s.File.ReadAt(record, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *store) ReadAt(p []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
