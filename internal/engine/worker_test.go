package engine

import (
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"path/filepath"
	"testing"
	"time"
)

func testState(jobs ...model.Job) model.State {
	s := model.NewState()
	doc := model.Document{Width: 4, Height: 4, Background: "#FFFFFF"}
	snap := model.Snapshot{ID: "snap-00000001", SceneID: "scene-1", Revision: 1, State: "active", Document: doc,
		Assets: map[string]model.Asset{}, CreatedAt: time.Now().UTC()}
	snap.Digest = model.SnapshotDigest(snap)
	s.Scenes["scene-1"] = model.Scene{ID: "scene-1", Name: "test", Revision: 1, State: "active", Document: doc, UpdatedAt: time.Now().UTC()}
	s.Snapshots[snap.ID] = snap
	for _, j := range jobs {
		j.Payload = snap
		s.Jobs[j.ID] = j
	}
	return s
}

func testJob(id string, attempt int) model.Job {
	now := time.Now().UTC()
	return model.Job{
		ID: id, SnapshotID: "snap-00000001", State: "running", Scale: 1, Attempt: attempt,
		CreatedAt: now, UpdatedAt: now, Claim: "claim-" + id, WorkerID: "worker-a",
		LeaseUntil: now.Add(time.Minute),
	}
}

func testStore(t *testing.T, jobs ...model.Job) *persist.Store {
	t.Helper()
	st, e := persist.Open(filepath.Join(t.TempDir(), "state.json"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { st.Close() })
	seed := testState(jobs...)
	if e := st.Update(func(s *model.State) error {
		*s = seed
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	return st
}

func jobField(t *testing.T, st *persist.Store, id string) model.Job {
	t.Helper()
	state, e := st.Read()
	if e != nil {
		t.Fatal(e)
	}
	return state.Jobs[id]
}

// A worker whose claim was cleared by a cancellation must not publish.
func TestFinishRejectsCancelledClaim(t *testing.T) {
	j := testJob("job-00000001", 1)
	st := testStore(t, j)
	// Simulate the API committing cancellation: state changes and claim clears.
	if e := st.Update(func(s *model.State) error {
		v := s.Jobs[j.ID]
		v.State, v.Claim, v.WorkerID, v.LeaseUntil = "cancelled", "", "", time.Time{}
		s.Jobs[j.ID] = v
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	w := New(st, Config{ID: "worker-a", Lease: time.Minute, Logf: func(string, ...any) {}})
	if e := w.finish(j, planStub(), []byte("png"), nil); e != nil {
		t.Fatal(e)
	}
	if got := jobField(t, st, j.ID); got.State != "cancelled" || got.ArtifactID != "" {
		t.Fatalf("stale result published: %+v", got)
	}
}

// A retry increments the attempt and clears the claim. An older attempt's
// late result must not resurrect the job.
func TestFinishRejectsStaleAttempt(t *testing.T) {
	old := testJob("job-00000001", 1)
	st := testStore(t, old)
	retried := jobField(t, st, old.ID)
	retried.Attempt, retried.Claim, retried.WorkerID, retried.LeaseUntil = 2, "claim-new", "worker-b", time.Now().UTC().Add(time.Minute)
	if e := st.Update(func(s *model.State) error {
		s.Jobs[old.ID] = retried
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	w := New(st, Config{ID: "worker-a", Lease: time.Minute, Logf: func(string, ...any) {}})
	if e := w.finish(old, planStub(), []byte("png"), nil); e != nil {
		t.Fatal(e)
	}
	if got := jobField(t, st, old.ID); got.Attempt != 2 || got.State != "running" || got.ArtifactID != "" {
		t.Fatalf("stale attempt overwrote newer attempt: %+v", got)
	}
	if state, _ := st.Read(); len(state.Artifacts) != 0 {
		t.Fatalf("stale attempt wrote an artifact: %+v", state.Artifacts)
	}
}

// The owning claim does publish, and publishing clears the lease fields.
func TestFinishPublishesOwnedClaim(t *testing.T) {
	j := testJob("job-00000001", 1)
	st := testStore(t, j)
	w := New(st, Config{ID: "worker-a", Lease: time.Minute, Logf: func(string, ...any) {}})
	if e := w.finish(j, planStub(), []byte("png"), nil); e != nil {
		t.Fatal(e)
	}
	got := jobField(t, st, j.ID)
	if got.State != "succeeded" || got.ArtifactID == "" || got.Claim != "" || got.WorkerID != "" || !got.LeaseUntil.IsZero() {
		t.Fatalf("owned claim did not publish cleanly: %+v", got)
	}
	state, _ := st.Read()
	if a := state.Artifacts[got.ArtifactID]; a.JobID != j.ID || string(a.PNG) != "png" {
		t.Fatalf("artifact mismatch: %+v", a)
	}
}

// Requeued interrupted work keeps its attempt and can be claimed again by a
// different worker.
func TestReclaimAfterLeaseExpiryKeepsAttempt(t *testing.T) {
	j := testJob("job-00000001", 1)
	j.LeaseUntil = time.Now().UTC().Add(-time.Second)
	st := testStore(t, j)
	w := New(st, Config{ID: "worker-b", Lease: time.Minute, Logf: func(string, ...any) {}})
	claimed, ok, e := w.claim("worker-b", time.Now().UTC())
	if e != nil || !ok {
		t.Fatalf("claim failed: ok=%v err=%v", ok, e)
	}
	if claimed.ID != j.ID || claimed.Attempt != 1 || claimed.WorkerID != "worker-b" || claimed.Claim == j.Claim {
		t.Fatalf("unexpected takeover claim: %+v", claimed)
	}
}

// Two claims against the same job in sequence only ever produce one running
// owner; the second caller sees the queue empty rather than a duplicate.
func TestClaimIsExclusive(t *testing.T) {
	j := testJob("job-00000001", 1)
	j.State, j.Claim, j.WorkerID, j.LeaseUntil = "queued", "", "", time.Time{}
	st := testStore(t, j)
	w1 := New(st, Config{ID: "worker-a", Lease: time.Minute, Logf: func(string, ...any) {}})
	w2 := New(st, Config{ID: "worker-b", Lease: time.Minute, Logf: func(string, ...any) {}})
	first, ok, e := w1.claim("worker-a", time.Now().UTC())
	if e != nil || !ok {
		t.Fatalf("first claim failed: %v", e)
	}
	if _, ok, e := w2.claim("worker-b", time.Now().UTC()); e != nil || ok {
		t.Fatalf("second claim should find nothing: ok=%v err=%v", ok, e)
	}
	got := jobField(t, st, j.ID)
	if got.Claim != first.Claim || got.WorkerID != "worker-a" || got.State != "running" {
		t.Fatalf("ownership changed under contention: %+v", got)
	}
}
