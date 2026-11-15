package persist

import (
	"atlas-render/internal/model"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const (
	// A lock is normally held for the duration of one small read-modify-write.
	// These bounds keep a stalled lock holder (for example a suspended
	// process) from blocking callers indefinitely; callers surface a storage
	// error instead.
	readLockTimeout  = 2 * time.Second
	writeLockTimeout = 5 * time.Second
	probeLockTimeout = 750 * time.Millisecond
)

// Store owns a state file shared by one HTTP process and any number of
// execution processes. Every transaction takes the advisory file lock for its
// own duration and reloads the state from disk inside that lock, so no process
// relies on another process's memory and no two transactions can interleave
// their read-modify-write cycle.
//
// The durability contract is unchanged: a transaction validates the candidate
// state, writes a temporary file, fsyncs it, and atomically renames it over the
// target before reporting success.
type Store struct {
	mu   sync.Mutex
	path string
	lock *os.File
}

func Open(path string) (*Store, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	lock, e := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	s := &Store{path: path, lock: lock}
	state, _, e := s.load()
	if e != nil {
		lock.Close()
		return nil, e
	}
	if e = Validate(state); e != nil {
		lock.Close()
		return nil, e
	}
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	e := s.lock.Close()
	s.lock = nil
	return e
}

// Path is the state file shared by the API and execution processes.
func (s *Store) Path() string { return s.path }

// Read returns a snapshot of the committed state. The shared lock keeps a
// concurrent writer from publishing while the file is being read.
func (s *Store) Read() (model.State, error) {
	return s.read(readLockTimeout)
}

func (s *Store) read(timeout time.Duration) (model.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.lockFile(syscall.LOCK_SH, timeout); e != nil {
		return model.State{}, e
	}
	defer s.unlockFile()
	state, _, e := s.load()
	return state, e
}

// Inspect runs fn against a consistent committed state without taking the
// exclusive lock. It is the cheap path for read-only decisions such as lease
// ownership checks, which are re-verified inside Update before writing. It
// waits only briefly so a stalled writer cannot hold up a worker's heartbeat
// or an idle poll for long.
func (s *Store) Inspect(fn func(model.State) error) error {
	st, e := s.read(probeLockTimeout)
	if e != nil {
		return e
	}
	return fn(st)
}

// Update runs fn against the freshest committed state under an exclusive lock
// and commits the result atomically. fn must not have side effects before the
// commit succeeds. A transaction that leaves the state unchanged performs no
// write, so idle polling does not churn the file.
func (s *Store) Update(fn func(*model.State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.lockFile(syscall.LOCK_EX, writeLockTimeout); e != nil {
		return e
	}
	defer s.unlockFile()
	current, raw, e := s.load()
	if e != nil {
		return e
	}
	next := model.Clone(current)
	if e = fn(&next); e != nil {
		return e
	}
	if e = Validate(next); e != nil {
		return e
	}
	b, e := json.Marshal(next)
	if e != nil {
		return e
	}
	if bytes.Equal(b, raw) {
		return nil
	}
	if e = atomicWrite(s.path, b); e != nil {
		return fmt.Errorf("persist state: %w", e)
	}
	return nil
}

func (s *Store) load() (model.State, []byte, error) {
	data, e := os.ReadFile(s.path)
	if os.IsNotExist(e) {
		return model.NewState(), nil, nil
	}
	if e != nil {
		return model.State{}, nil, e
	}
	var state model.State
	if e = json.Unmarshal(data, &state); e != nil {
		return model.State{}, nil, fmt.Errorf("invalid persisted JSON: %w", e)
	}
	return state, data, nil
}

func (s *Store) lockFile(how int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		e := syscall.Flock(int(s.lock.Fd()), how|syscall.LOCK_NB)
		if e == nil {
			return nil
		}
		if errors.Is(e, syscall.EINTR) {
			continue
		}
		if !errors.Is(e, syscall.EWOULDBLOCK) && !errors.Is(e, syscall.EAGAIN) {
			return fmt.Errorf("state lock: %w", e)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("state lock timeout after %s: another process is holding the state file", timeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (s *Store) unlockFile() {
	_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
}

func atomicWrite(path string, data []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".atlas-write-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(data)
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	// Make the rename itself durable when the platform supports it.
	if dir, e := os.Open(filepath.Dir(path)); e == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}
