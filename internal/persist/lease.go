package persist

import (
	"atlas-render/internal/model"
	"time"
)

// LeaseExpired reports whether a running job has lost its execution lease. A
// record without a claim predates the lease protocol and counts as expired.
func LeaseExpired(j model.Job, now time.Time) bool {
	return j.Claim == "" || !j.LeaseUntil.After(now)
}

// RequeueExpired returns interrupted jobs to the queue without counting a new
// attempt. A running job is interrupted when its lease has expired, or when it
// predates the lease protocol and carries no claim at all. It is never touched
// while a live worker still owns a valid lease, which is what allows several
// execution processes to share one state file.
func RequeueExpired(s *model.State, now time.Time, actor string) int {
	n := 0
	for id, j := range s.Jobs {
		if j.State != "running" || !LeaseExpired(j, now) {
			continue
		}
		j.State = "queued"
		j.Claim = ""
		j.WorkerID = ""
		j.LeaseUntil = time.Time{}
		j.UpdatedAt = now
		s.Jobs[id] = j
		s.EmitBy("job.recovered", id, actor)
		n++
	}
	return n
}
