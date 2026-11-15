package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const FormatVersion = 1

type Point struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}
type Rect struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}
type Transform struct {
	X       float64 `json:"x"`
	Y       float64 `json:"y"`
	ScaleX  float64 `json:"scaleX"`
	ScaleY  float64 `json:"scaleY"`
	Degrees float64 `json:"degrees"`
}
type Paint struct {
	Kind     string `json:"kind"`
	Color    string `json:"color"`
	EndColor string `json:"endColor,omitempty"`
	From     Point  `json:"from"`
	To       Point  `json:"to"`
}
type Layer struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Width     float64   `json:"width"`
	Height    float64   `json:"height"`
	Transform Transform `json:"transform"`
	Opacity   float64   `json:"opacity"`
	Fill      Paint     `json:"fill"`
	Points    []Point   `json:"points,omitempty"`
	AssetID   string    `json:"assetId,omitempty"`
	Clip      *Rect     `json:"clip,omitempty"`
	Children  []Layer   `json:"children,omitempty"`
}
type Document struct {
	Width      int     `json:"width"`
	Height     int     `json:"height"`
	Background string  `json:"background"`
	Layers     []Layer `json:"layers"`
}
type Scene struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Revision  int       `json:"revision"`
	State     string    `json:"state"`
	Document  Document  `json:"document"`
	UpdatedAt time.Time `json:"updatedAt"`
}
type Asset struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
	State  string `json:"state"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	PNG    []byte `json:"png"`
}
type Snapshot struct {
	ID        string           `json:"id"`
	SceneID   string           `json:"sceneId"`
	Revision  int              `json:"revision"`
	State     string           `json:"state"`
	Document  Document         `json:"document"`
	Assets    map[string]Asset `json:"assets"`
	Digest    string           `json:"digest"`
	CreatedAt time.Time        `json:"createdAt"`
}
type Job struct {
	ID         string    `json:"id"`
	SnapshotID string    `json:"snapshotId"`
	State      string    `json:"state"`
	Scale      int       `json:"scale"`
	Attempt    int       `json:"attempt"`
	Error      string    `json:"error,omitempty"`
	ArtifactID string    `json:"artifactId,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
	Payload    Snapshot  `json:"payload"`
	// Claim identifies one execution lease. A result can only be committed
	// while the job still carries this exact claim and attempt. It is cleared
	// whenever the job leaves running, so a worker that lost contact with the
	// state cannot publish a stale result.
	Claim      string    `json:"claim,omitempty"`
	WorkerID   string    `json:"workerId,omitempty"`
	LeaseUntil time.Time `json:"leaseUntil,omitempty"`
}
type Artifact struct {
	ID     string `json:"id"`
	JobID  string `json:"jobId"`
	State  string `json:"state"`
	Digest string `json:"digest"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	PNG    []byte `json:"png,omitempty"`
}
type Receipt struct {
	Key         string `json:"key"`
	Fingerprint string `json:"fingerprint"`
	JobID       string `json:"jobId"`
}
type Event struct {
	Sequence int       `json:"sequence"`
	Kind     string    `json:"kind"`
	Subject  string    `json:"subject"`
	Actor    string    `json:"actor,omitempty"`
	At       time.Time `json:"at"`
}
type State struct {
	Version   int                 `json:"version"`
	Scenes    map[string]Scene    `json:"scenes"`
	Assets    map[string]Asset    `json:"assets"`
	Snapshots map[string]Snapshot `json:"snapshots"`
	Jobs      map[string]Job      `json:"jobs"`
	Artifacts map[string]Artifact `json:"artifacts"`
	Receipts  map[string]Receipt  `json:"receipts"`
	Events    []Event             `json:"events"`
	Sequence  int                 `json:"sequence"`
}

func NewState() State {
	return State{Version: FormatVersion, Scenes: map[string]Scene{}, Assets: map[string]Asset{}, Snapshots: map[string]Snapshot{}, Jobs: map[string]Job{}, Artifacts: map[string]Artifact{}, Receipts: map[string]Receipt{}}
}
func Digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func CanonicalDigest(v any) string {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return Digest(b)
}
func SnapshotDigest(s Snapshot) string {
	return CanonicalDigest(struct {
		Document Document
		Assets   map[string]Asset
	}{s.Document, s.Assets})
}
func Clone[T any](v T) T {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	var out T
	if e = json.Unmarshal(b, &out); e != nil {
		panic(e)
	}
	return out
}
func (s *State) Emit(kind, id string) {
	s.EmitBy(kind, id, "")
}

// EmitBy records an event attributed to the actor that caused it. Events
// remain ordered by Sequence regardless of which process appended them.
func (s *State) EmitBy(kind, id, actor string) {
	s.Sequence++
	s.Events = append(s.Events, Event{Sequence: s.Sequence, Kind: kind, Subject: id, Actor: actor, At: time.Now().UTC()})
	if len(s.Events) > 500 {
		s.Events = s.Events[len(s.Events)-500:]
	}
}
func ID(prefix string, seq int) string { return fmt.Sprintf("%s-%08d", prefix, seq) }
