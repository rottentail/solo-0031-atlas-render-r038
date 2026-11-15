package persist

import (
	"atlas-render/internal/model"
	"testing"
	"time"
)

func seeded() (model.State, model.Job) {
	st := model.NewState()
	j := model.Job{
		ID:         "job-00000001",
		SnapshotID: "snap-00000001",
		State:      "running",
		Scale:      1,
		Attempt:    1,
		CreatedAt:  time.Now().UTC(),
		Claim:      "claim-a",
		WorkerID:   "worker-a",
		LeaseUntil: time.Now().UTC().Add(time.Minute),
	}
	st.Jobs[j.ID] = j
	return st, j
}

func TestRequeueExpiredLeavesLiveLease(t *testing.T) {
	st, _ := seeded()
	if n := RequeueExpired(&st, time.Now().UTC(), "test"); n != 0 {
		t.Fatalf("live lease requeued: %d", n)
	}
	if st.Jobs["job-00000001"].State != "running" {
		t.Fatal("live lease changed state")
	}
}

func TestRequeueExpiredReturnsInterruptedAttempt(t *testing.T) {
	st, j := seeded()
	j.LeaseUntil = time.Now().UTC().Add(-time.Second)
	st.Jobs[j.ID] = j
	if n := RequeueExpired(&st, time.Now().UTC(), "test"); n != 1 {
		t.Fatalf("expired lease not requeued: %d", n)
	}
	got := st.Jobs[j.ID]
	if got.State != "queued" || got.Attempt != 1 || got.Claim != "" || got.WorkerID != "" || !got.LeaseUntil.IsZero() {
		t.Fatalf("unexpected requeued record: %+v", got)
	}
}

func TestRequeueExpiredHandlesLegacyRecord(t *testing.T) {
	st, j := seeded()
	j.Claim = ""
	j.WorkerID = ""
	j.LeaseUntil = time.Time{}
	st.Jobs[j.ID] = j
	if n := RequeueExpired(&st, time.Now().UTC(), "test"); n != 1 {
		t.Fatalf("legacy running record not requeued: %d", n)
	}
	if st.Jobs[j.ID].State != "queued" || st.Jobs[j.ID].Attempt != 1 {
		t.Fatalf("legacy record not recovered: %+v", st.Jobs[j.ID])
	}
}

func TestLeaseExpired(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name string
		job  model.Job
		want bool
	}{
		{"no claim", model.Job{State: "running"}, true},
		{"future lease", model.Job{State: "running", Claim: "c", LeaseUntil: now.Add(time.Second)}, false},
		{"past lease", model.Job{State: "running", Claim: "c", LeaseUntil: now.Add(-time.Second)}, true},
	}
	for _, c := range cases {
		if got := LeaseExpired(c.job, now); got != c.want {
			t.Fatalf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
