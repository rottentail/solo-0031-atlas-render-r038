package engine

import (
	"atlas-render/internal/codec"
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"atlas-render/internal/raster"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

// Defaults chosen so that a healthy worker renews its lease well before the
// reaper may take the job back, while a killed worker is noticed in bounded
// time.
const (
	DefaultLease       = 5 * time.Second
	DefaultPoll        = 150 * time.Millisecond
	DefaultConcurrency = 1
)

// Config describes one execution process. ID must be unique per process so
// claims and events can be attributed; Run adds a per-slot suffix.
type Config struct {
	ID          string
	Lease       time.Duration
	Poll        time.Duration
	Concurrency int
	Logf        func(string, ...any)
}

func DefaultConfig() Config {
	id := os.Getenv("ATLAS_WORKER_ID")
	if id == "" {
		host, _ := os.Hostname()
		id = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	return Config{ID: id, Lease: DefaultLease, Poll: DefaultPoll, Concurrency: DefaultConcurrency, Logf: log.Printf}
}

func (c Config) normalized() Config {
	if c.ID == "" {
		c.ID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	if c.Lease <= 0 {
		c.Lease = DefaultLease
	}
	if c.Poll <= 0 {
		c.Poll = DefaultPoll
	}
	if c.Concurrency < 1 {
		c.Concurrency = DefaultConcurrency
	}
	if c.Logf == nil {
		c.Logf = log.Printf
	}
	return c
}

// Worker consumes the persistent queue. Several Worker processes may run at
// once against the same Store; the claim transaction guarantees that exactly
// one of them owns a given job and attempt.
type Worker struct {
	store *persist.Store
	cfg   Config
}

func New(st *persist.Store, cfg Config) *Worker {
	return &Worker{store: st, cfg: cfg.normalized()}
}

// Recover requeues jobs whose lease has expired, including legacy running
// records that carry no lease. It never touches a job whose lease is still
// valid, because that job may be executed by a healthy peer process.
func (w *Worker) Recover() error {
	var requeued int
	e := w.store.Update(func(st *model.State) error {
		requeued = persist.RequeueExpired(st, time.Now().UTC(), w.cfg.ID)
		return nil
	})
	if e == nil && requeued > 0 {
		w.cfg.Logf("requeued %d interrupted job(s)", requeued)
	}
	return e
}

// Run executes up to Config.Concurrency jobs in parallel until ctx ends. On a
// graceful stop it returns its own unfinished jobs to the queue so a peer or
// the next start can execute them without waiting for lease expiry.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		slot := w.cfg.ID
		if w.cfg.Concurrency > 1 {
			slot = fmt.Sprintf("%s#%d", w.cfg.ID, i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx, slot)
		}()
	}
	<-ctx.Done()
	wg.Wait()
}

// requeueClaim returns one owned job to the queue without counting a new
// attempt. The claim check keeps a concurrent cancel, retry, or takeover safe.
func requeueClaim(st *model.State, id string, attempt int, claim string, actor string) bool {
	j, ok := st.Jobs[id]
	if !ok || j.State != "running" || j.Attempt != attempt || j.Claim != claim {
		return false
	}
	j.State = "queued"
	j.Claim = ""
	j.WorkerID = ""
	j.LeaseUntil = time.Time{}
	j.UpdatedAt = time.Now().UTC()
	st.Jobs[id] = j
	st.EmitBy("job.released", id, actor)
	return true
}

func (w *Worker) loop(ctx context.Context, slot string) {
	delay := w.cfg.Poll
	for {
		if ctx.Err() != nil {
			return
		}
		worked, e := w.step(ctx, slot)
		if e != nil && ctx.Err() == nil {
			w.cfg.Logf("render worker %s: %v", slot, e)
		}
		if worked {
			delay = w.cfg.Poll
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

func newClaim() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// claim commits the transition queued -> running for the oldest eligible job.
// Reaping expired leases happens in the same transaction, so a dead worker's
// job becomes available without a separate coordination step.
func (w *Worker) claim(slot string, now time.Time) (model.Job, bool, error) {
	// Probe without the exclusive lock: an idle queue needs no transaction at
	// all. Anything observed here is re-checked under the lock below. A probe
	// that cannot take the lock quickly is treated as "try again later"; the
	// periodic reaper still guarantees expired leases are eventually returned.
	candidate := false
	e := w.store.Inspect(func(st model.State) error {
		for _, j := range st.Jobs {
			if j.State == "queued" || (j.State == "running" && persist.LeaseExpired(j, now)) {
				candidate = true
				return nil
			}
		}
		return nil
	})
	if e != nil {
		return model.Job{}, false, nil
	}
	if !candidate {
		return model.Job{}, false, nil
	}
	var job model.Job
	found := false
	e = w.store.Update(func(st *model.State) error {
		persist.RequeueExpired(st, now, slot+"/reaper")
		ids := make([]string, 0, len(st.Jobs))
		for id, j := range st.Jobs {
			if j.State == "queued" {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return nil
		}
		sort.Slice(ids, func(i, j int) bool {
			a, b := st.Jobs[ids[i]], st.Jobs[ids[j]]
			if a.CreatedAt.Equal(b.CreatedAt) {
				return a.ID < b.ID
			}
			return a.CreatedAt.Before(b.CreatedAt)
		})
		j, ok := st.Jobs[ids[0]]
		if !ok || j.State != "queued" {
			return nil
		}
		j.State = "running"
		j.Claim = newClaim()
		j.WorkerID = slot
		j.LeaseUntil = now.Add(w.cfg.Lease)
		j.Error = ""
		j.UpdatedAt = now
		st.Jobs[j.ID] = j
		st.EmitBy("job.running", j.ID, slot)
		job = j
		found = true
		return nil
	})
	return job, found, e
}

func (w *Worker) step(ctx context.Context, slot string) (bool, error) {
	parent := ctx
	j, ok, e := w.claim(slot, time.Now().UTC())
	if e != nil || !ok {
		return false, e
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// The heartbeat renews the lease and reports loss of ownership, either
	// because another worker took over or because the job was cancelled. On
	// loss it interrupts the local render immediately.
	owned := make(chan bool, 1)
	stopBeat := make(chan struct{})
	go func() {
		kept := w.heartbeat(ctx, j.ID, j.Attempt, j.Claim, stopBeat)
		if !kept {
			cancel()
		}
		owned <- kept
	}()

	plan, renderErr := raster.Compile(j.Payload, j.Scale)
	var data []byte
	if renderErr == nil {
		var rendered *image.NRGBA
		rendered, renderErr = plan.Render(ctx)
		if renderErr == nil {
			data, renderErr = codec.Encode(rendered)
		}
	}
	close(stopBeat)
	lost := !<-owned
	if parent.Err() != nil {
		// Interrupted by shutdown, not by cancellation of this job. Hand the
		// job back rather than leaving the claim to expire, then let Run exit.
		_ = w.store.Update(func(st *model.State) error {
			requeueClaim(st, j.ID, j.Attempt, j.Claim, slot)
			return nil
		})
		return true, nil
	}
	if lost {
		w.cfg.Logf("job %s attempt %d: claim no longer held (cancelled or taken over), discarding local result", j.ID, j.Attempt)
		return true, nil
	}
	return true, w.finish(j, plan, data, renderErr)
}

// heartbeat renews the claim until stopBeat closes. It returns false as soon
// as the persisted job no longer belongs to this claim and attempt.
func (w *Worker) heartbeat(ctx context.Context, id string, attempt int, claim string, stop <-chan struct{}) bool {
	interval := w.cfg.Lease / 4
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	// Check ownership once before rendering starts so a cancellation that
	// committed between claim and registration is honoured immediately.
	if ok, e := w.renew(id, attempt, claim); e == nil && !ok {
		return false
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return true
		case <-ctx.Done():
			return false
		case <-t.C:
			ok, e := w.renew(id, attempt, claim)
			if e != nil {
				// A transient storage error is not proof of lost ownership;
				// the next tick retries while the render keeps running.
				w.cfg.Logf("job %s: lease renewal failed: %v", id, e)
				continue
			}
			if !ok {
				return false
			}
		}
	}
}

// renew confirms that this worker still owns the claim and extends the lease.
// It only rewrites the state when the lease has less than half its lifetime
// left, so a heartbeat does not force a full-state commit on every tick.
func (w *Worker) renew(id string, attempt int, claim string) (bool, error) {
	// A read-only probe avoids a full-state commit when the lease is still
	// fresh. Ownership is re-checked inside Update before any extension. If the
	// probe cannot complete, report success and retry on the next tick: a
	// transient read failure is not proof that ownership was lost.
	var current model.Job
	e := w.store.Inspect(func(st model.State) error {
		current = st.Jobs[id]
		return nil
	})
	if e != nil {
		return true, nil
	}
	if current.State != "running" || current.Attempt != attempt || current.Claim != claim {
		return false, nil
	}
	now := time.Now().UTC()
	if !current.LeaseUntil.Before(now.Add(w.cfg.Lease / 2)) {
		return true, nil
	}
	ok := false
	e = w.store.Update(func(st *model.State) error {
		j, exists := st.Jobs[id]
		if !exists || j.State != "running" || j.Attempt != attempt || j.Claim != claim {
			return nil
		}
		j.LeaseUntil = now.Add(w.cfg.Lease)
		st.Jobs[id] = j
		ok = true
		return nil
	})
	return ok, e
}

// finish publishes the outcome only while the job is still running under this
// exact claim. Cancellation, a retry, or another worker's takeover between
// render completion and commit therefore discards this result.
func (w *Worker) finish(j model.Job, plan raster.Plan, data []byte, renderErr error) error {
	now := time.Now().UTC()
	return w.store.Update(func(st *model.State) error {
		v, ok := st.Jobs[j.ID]
		if !ok || v.State != "running" || v.Attempt != j.Attempt || v.Claim != j.Claim {
			return nil
		}
		v.UpdatedAt = now
		v.Claim = ""
		v.WorkerID = ""
		v.LeaseUntil = time.Time{}
		if renderErr != nil {
			if errors.Is(renderErr, context.Canceled) {
				v.State = "cancelled"
			} else {
				v.State = "failed"
				v.Error = renderErr.Error()
			}
			st.Jobs[v.ID] = v
			st.EmitBy("job."+v.State, v.ID, w.cfg.ID)
			return nil
		}
		a := model.Artifact{ID: model.ID("png", st.Sequence+1), JobID: v.ID, State: "live", Digest: model.Digest(data), Width: plan.Width, Height: plan.Height, PNG: data}
		st.Artifacts[a.ID] = a
		v.State = "succeeded"
		v.ArtifactID = a.ID
		st.Jobs[v.ID] = v
		st.EmitBy("job.succeeded", v.ID, w.cfg.ID)
		return nil
	})
}

// ReapLoop periodically requeues jobs whose lease expired. The API process
// runs it so a dead worker does not leave jobs stuck forever even when no
// replacement worker is currently running.
func ReapLoop(ctx context.Context, st *persist.Store, lease time.Duration, logf func(string, ...any)) {
	if lease <= 0 {
		lease = DefaultLease
	}
	if logf == nil {
		logf = log.Printf
	}
	interval := lease / 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var n int
			e := st.Update(func(s *model.State) error {
				n = persist.RequeueExpired(s, time.Now().UTC(), "reaper")
				return nil
			})
			if e != nil {
				logf("lease reaper: %v", e)
				continue
			}
			if n > 0 {
				logf("lease reaper requeued %d expired job(s)", n)
			}
		}
	}
}
