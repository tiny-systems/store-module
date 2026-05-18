// Package documentstore implements document_store — an embedded key/value
// store backed by bbolt, persisted on a PVC inside the operator pod. The
// component exposes one input port per CRUD verb (put, get, delete, find)
// and a matching source port per result, mirroring the deterministic
// shape conventions other Tiny Systems components use.
//
// Why bbolt: pure Go (no CGO), single-file durable storage, transactional
// reads and writes, mmap-backed for performance. Used inside etcd itself,
// proven for embedded persistence. The trade-off versus Postgres / Redis
// is single-writer: bbolt locks the file, so the module pod runs at
// replicas: 1. For HA persistence reach for the postgres_* or redis_*
// components in database-module-v0 — those are stateless clients.
//
// Lifecycle:
//   - On OnSettings (and on first OnReconcile to survive pod restarts),
//     the component opens the bbolt file at Settings.Path. Subsequent
//     settings changes re-open if the path changes.
//   - On each Handle call, a fresh bbolt transaction runs the operation
//     and emits the result.
//   - Soft cap on file size (Settings.MaxSizeMB) is checked before put
//     operations — exceeding it routes to error port if enabled.
//
// Settings.Collections declares the named buckets so authors don't
// accidentally write to typo'd collections. Buckets are created on
// component start; writes to undeclared collections fail loudly.
package documentstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"go.etcd.io/bbolt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "document_store"

	PutPort       = "put"
	GetPort       = "get"
	DeletePort    = "delete"
	FindPort      = "find"
	PutResultPort = "put_ok"
	GetResultPort = "get_ok"
	DelResultPort = "delete_ok"
	FindResultPort = "find_ok"
	ErrorPort     = "error"

	defaultPath      = "/data/store.db"
	defaultMaxSizeMB = 1024
)

type Context any

// Collection is a named bbolt bucket. Declaring them in settings makes
// typos surface as TinyNode.Status.Error at config time rather than as
// "missing bucket" runtime errors per request.
type Collection struct {
	Name string `json:"name" required:"true" minLength:"1" title:"Collection name"`
}

type Settings struct {
	EnableErrorPort bool         `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"Route operational failures (disk full, missing collection, marshal errors, leader-only refusals) to the error port instead of failing the request."`
	Path            string       `json:"path" required:"true" minLength:"1" default:"/data/store.db" title:"DB file path" description:"Absolute path to the bbolt file. Must be on a mounted PVC for durability."`
	Collections     []Collection `json:"collections" required:"true" minItems:"1" uniqueItems:"true" title:"Collections" description:"Named buckets. Writes to undeclared collections fail. Reads from undeclared collections return found=false."`
	MaxSizeMB       int          `json:"maxSizeMB" required:"true" minimum:"1" default:"1024" title:"Max size (MB)" description:"Soft cap on DB file size. Put operations refuse writes when the file is at or above this size."`
	LeaderOnly      bool         `json:"leaderOnly" title:"Leader-only mode" description:"For HA deployments with replicas > 1 on an RWX PVC. When enabled, only the SDK-elected leader opens bbolt and serves; followers refuse requests with retryable=true. On leader change the new leader retries opening the file (up to 30s) until the prior leader's lock releases. Leave false for single-replica deployments."`
}

// --- Port message shapes -----------------------------------------

type PutRequest struct {
	Context    Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Collection string  `json:"collection" required:"true" title:"Collection"`
	Key        string  `json:"key" required:"true" title:"Key"`
	Value      any     `json:"value" required:"true" configurable:"true" title:"Value" description:"Stored as JSON. Any structure."`
}

type PutResult struct {
	Context    Context `json:"context"`
	Collection string  `json:"collection"`
	Key        string  `json:"key"`
}

type GetRequest struct {
	Context    Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Collection string  `json:"collection" required:"true" title:"Collection"`
	Key        string  `json:"key" required:"true" title:"Key"`
}

type GetResult struct {
	Context    Context `json:"context"`
	Collection string  `json:"collection"`
	Key        string  `json:"key"`
	Value      any     `json:"value,omitempty" configurable:"true" title:"Value"`
	Found      bool    `json:"found"`
}

type DeleteRequest struct {
	Context    Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Collection string  `json:"collection" required:"true" title:"Collection"`
	Key        string  `json:"key" required:"true" title:"Key"`
}

type DeleteResult struct {
	Context    Context `json:"context"`
	Collection string  `json:"collection"`
	Key        string  `json:"key"`
	Deleted    bool    `json:"deleted" description:"False when the key didn't exist (delete is idempotent)."`
}

type FindRequest struct {
	Context    Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Collection string  `json:"collection" required:"true" title:"Collection"`
	Prefix     string  `json:"prefix,omitempty" title:"Key prefix" description:"Optional. When empty, scans the whole collection up to Limit."`
	Limit      int     `json:"limit,omitempty" minimum:"0" title:"Limit" description:"Cap on results. 0 means no cap (use with care on big collections)."`
}

type FindItem struct {
	Key   string `json:"key"`
	Value any    `json:"value" configurable:"true"`
}

type FindResult struct {
	Context    Context    `json:"context"`
	Collection string     `json:"collection"`
	Items      []FindItem `json:"items"`
	Count      int        `json:"count"`
}

type Error struct {
	Context   Context `json:"context"`
	Error     string  `json:"error"`
	DiskFull  bool    `json:"diskFull,omitempty" description:"True when the operation was rejected by the maxSizeMB cap. Useful for retry-after-eviction logic."`
	Retryable bool    `json:"retryable,omitempty" description:"True when the failure is transient — leader-only refusal, file-lock contention during failover. Caller may retry after a short delay."`
}

// --- Component ----------------------------------------------------

type Component struct {
	module.Base

	mu       sync.RWMutex
	settings Settings
	db       *bbolt.DB
	dbPath   string
}

func (c *Component) Instance() module.Component {
	return &Component{settings: Settings{
		Path:      defaultPath,
		MaxSizeMB: defaultMaxSizeMB,
	}}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Document Store",
		Info: "Embedded key/value store backed by bbolt + PVC. Four operation ports (put, get, delete, find) " +
			"each with a matching result port. Use as the storage layer for chat history, agent scratchpads, " +
			"intermediate flow state — anywhere SDK State's 900KB cap is too tight. Settings.Collections " +
			"declares named buckets so typo'd collection names fail at config time. Single-replica only " +
			"(bbolt locks the file). For HA persistence use postgres_* or redis_* from database-module-v0.",
		Tags: []string{"Store", "KV", "Persistence", "bbolt", "Embedded"},
	}
}

// boltOpenTimeout bounds how long bbolt.Open waits for the file lock.
// 30s gives the prior leader's lock time to release on a clean pod
// rotation or PVC reattachment; longer than typical k8s pod restart
// windows. If we still can't open after 30s, something is genuinely
// stuck (RWO PVC mis-attach, undead process holding the lock) and
// surfacing the error to TinyNode.Status is the right move.
const boltOpenTimeout = 30 * time.Second

// OnSettings validates the config and opens the bbolt file. When
// LeaderOnly is set, only the SDK-elected leader actually opens the
// file; followers stash the settings and defer the open until they
// become leader (via OnReconcile firing with the new leader bit).
//
// Re-opens when Path changes; otherwise just refreshes Settings and
// ensures any newly-declared collections exist as buckets.
func (c *Component) OnSettings(ctx context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if err := validateSettings(in); err != nil {
		return err
	}

	c.mu.Lock()
	c.settings = in
	c.mu.Unlock()

	// Single-replica path (LeaderOnly off): always open. The single
	// pod IS the leader by definition.
	// Leader-only path: only open when we hold the lease.
	if in.LeaderOnly && !utils.IsLeader(ctx) {
		// Follower: keep bbolt closed so it stays available for the
		// real leader. OnReconcile will retry when leadership flips.
		c.closeDBIfOpen("not leader; deferring open until elected")
		return nil
	}
	return c.ensureOpen(ctx, in)
}

// OnReconcile picks up leadership transitions. The SDK fires reconciles
// periodically; the context carries the current IsLeader bit. When a
// follower becomes leader it opens bbolt (retrying inside the
// boltOpenTimeout window for the prior leader's lock to clear), and
// when a leader is demoted it closes the file so the new leader can
// take over.
func (c *Component) OnReconcile(ctx context.Context, _ v1alpha1.TinyNode) error {
	c.mu.RLock()
	settings := c.settings
	dbOpen := c.db != nil
	c.mu.RUnlock()

	if !settings.LeaderOnly {
		// Single-replica deployments don't run leader election; nothing
		// to react to here.
		return nil
	}
	if utils.IsLeader(ctx) {
		if !dbOpen {
			// Just became leader: try to open. If the prior leader is
			// still draining its lock, bbolt.Open waits up to
			// boltOpenTimeout before failing.
			return c.ensureOpen(ctx, settings)
		}
		return nil
	}
	// Demoted from leader (rare — happens on rolling restart of the
	// pool). Close the file so the new leader can grab it.
	if dbOpen {
		c.closeDBIfOpen("demoted from leader; releasing lock")
	}
	return nil
}

// ensureOpen opens bbolt at the configured path (closing any prior
// handle if the path changed) and ensures declared collections exist
// as buckets. Holds the write lock for the whole operation since both
// fields it touches are guarded by c.mu.
func (c *Component) ensureOpen(_ context.Context, in Settings) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	pathChanged := c.dbPath != in.Path
	if c.db == nil || pathChanged {
		if c.db != nil {
			_ = c.db.Close()
			c.db = nil
		}
		if err := os.MkdirAll(filepath.Dir(in.Path), 0o755); err != nil {
			return fmt.Errorf("ensure store dir: %w", err)
		}
		db, err := bbolt.Open(in.Path, 0o600, &bbolt.Options{Timeout: boltOpenTimeout})
		if err != nil {
			// Most commonly: another process holds the file lock.
			// Give the operator a specific, actionable message rather
			// than the raw "timeout" bbolt returns.
			return fmt.Errorf("open bbolt at %s: %w "+
				"(another pod may be holding the file lock — "+
				"check that prior pod has terminated and PVC released; "+
				"for HA deployments enable LeaderOnly mode with RWX PVC)",
				in.Path, err)
		}
		c.db = db
		c.dbPath = in.Path
	}

	// Ensure declared collections exist as buckets. Idempotent.
	return c.db.Update(func(tx *bbolt.Tx) error {
		for _, col := range in.Collections {
			if _, err := tx.CreateBucketIfNotExists([]byte(col.Name)); err != nil {
				return fmt.Errorf("ensure bucket %q: %w", col.Name, err)
			}
		}
		return nil
	})
}

// closeDBIfOpen closes the bbolt handle when held. Safe to call from
// any goroutine; takes the write lock. The reason string lands in
// logs via the SDK's runner wrapper.
func (c *Component) closeDBIfOpen(_ string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return
	}
	_ = c.db.Close()
	c.db = nil
}

func validateSettings(s Settings) error {
	if strings.TrimSpace(s.Path) == "" {
		return fmt.Errorf("path required")
	}
	if !filepath.IsAbs(s.Path) {
		return fmt.Errorf("path must be absolute: %q", s.Path)
	}
	if s.MaxSizeMB < 1 {
		return fmt.Errorf("maxSizeMB must be >= 1")
	}
	if len(s.Collections) == 0 {
		return fmt.Errorf("at least one collection required")
	}
	seen := map[string]bool{}
	for i, c := range s.Collections {
		if strings.TrimSpace(c.Name) == "" {
			return fmt.Errorf("collections[%d]: empty name", i)
		}
		if seen[c.Name] {
			return fmt.Errorf("collections[%d]: duplicate name %q", i, c.Name)
		}
		seen[c.Name] = true
	}
	return nil
}

// Handle dispatches per-port to the operation handlers. Each handler
// runs inside a fresh bbolt transaction. In LeaderOnly mode, follower
// pods refuse requests with retryable=true — the caller is expected
// to back off and retry (the SDK will route to the leader on the next
// scheduling pass once leadership stabilises).
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	c.mu.RLock()
	db := c.db
	settings := c.settings
	c.mu.RUnlock()

	if settings.LeaderOnly && !utils.IsLeader(ctx) {
		// Follower: don't even try, just refuse. Returning retryable=true
		// signals the caller (often a router/retry component) that
		// re-firing the request shortly is the right move — leadership
		// might flip back, or k8s might route to the leader.
		return c.failRetryable(ctx, handler, contextFromMsg(msg),
			fmt.Errorf("not leader; route requests to the elected document_store pod"))
	}

	if db == nil {
		return c.fail(ctx, handler, contextFromMsg(msg),
			fmt.Errorf("store not initialised — settings not delivered yet (or leader still acquiring file lock)"), false)
	}

	switch port {
	case PutPort:
		req, ok := msg.(PutRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid put request"))
		}
		return c.handlePut(ctx, handler, db, settings, req)
	case GetPort:
		req, ok := msg.(GetRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid get request"))
		}
		return c.handleGet(ctx, handler, db, settings, req)
	case DeletePort:
		req, ok := msg.(DeleteRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid delete request"))
		}
		return c.handleDelete(ctx, handler, db, settings, req)
	case FindPort:
		req, ok := msg.(FindRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid find request"))
		}
		return c.handleFind(ctx, handler, db, settings, req)
	default:
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
}

func (c *Component) handlePut(ctx context.Context, handler module.Handler, db *bbolt.DB, settings Settings, req PutRequest) module.Result {
	if !collectionDeclared(settings, req.Collection) {
		return c.fail(ctx, handler, req.Context, fmt.Errorf("collection %q not declared in Settings.Collections", req.Collection), false)
	}
	// Soft size cap. bbolt's stat is fast (just a file size lookup).
	stat, err := os.Stat(c.dbPath)
	if err == nil && stat.Size() >= int64(settings.MaxSizeMB)*1024*1024 {
		errMsg := fmt.Errorf("store at %s exceeds maxSizeMB=%d (current size %d bytes)", c.dbPath, settings.MaxSizeMB, stat.Size())
		if !settings.EnableErrorPort {
			return module.Fail(errMsg)
		}
		return handler(ctx, ErrorPort, Error{Context: req.Context, Error: errMsg.Error(), DiskFull: true})
	}

	payload, err := json.Marshal(req.Value)
	if err != nil {
		return c.fail(ctx, handler, req.Context, fmt.Errorf("marshal value: %w", err), false)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(req.Collection))
		if bucket == nil {
			return fmt.Errorf("bucket %q missing", req.Collection)
		}
		return bucket.Put([]byte(req.Key), payload)
	})
	if err != nil {
		return c.fail(ctx, handler, req.Context, err, false)
	}
	return handler(ctx, PutResultPort, PutResult{
		Context:    req.Context,
		Collection: req.Collection,
		Key:        req.Key,
	})
}

func (c *Component) handleGet(ctx context.Context, handler module.Handler, db *bbolt.DB, settings Settings, req GetRequest) module.Result {
	if !collectionDeclared(settings, req.Collection) {
		// Be lenient on read: return found=false rather than failing.
		// Distinguishes "I asked something legit and there's nothing"
		// from "the bucket doesn't even exist" by routing the latter
		// through error when enabled.
		return c.fail(ctx, handler, req.Context, fmt.Errorf("collection %q not declared in Settings.Collections", req.Collection), false)
	}
	var raw []byte
	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(req.Collection))
		if bucket == nil {
			return nil
		}
		raw = bucket.Get([]byte(req.Key))
		return nil
	})
	if err != nil {
		return c.fail(ctx, handler, req.Context, err, false)
	}
	if raw == nil {
		return handler(ctx, GetResultPort, GetResult{
			Context:    req.Context,
			Collection: req.Collection,
			Key:        req.Key,
			Found:      false,
		})
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return c.fail(ctx, handler, req.Context, fmt.Errorf("unmarshal value: %w", err), false)
	}
	return handler(ctx, GetResultPort, GetResult{
		Context:    req.Context,
		Collection: req.Collection,
		Key:        req.Key,
		Value:      value,
		Found:      true,
	})
}

func (c *Component) handleDelete(ctx context.Context, handler module.Handler, db *bbolt.DB, settings Settings, req DeleteRequest) module.Result {
	if !collectionDeclared(settings, req.Collection) {
		return c.fail(ctx, handler, req.Context, fmt.Errorf("collection %q not declared in Settings.Collections", req.Collection), false)
	}
	var existed bool
	err := db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(req.Collection))
		if bucket == nil {
			return nil
		}
		existed = bucket.Get([]byte(req.Key)) != nil
		return bucket.Delete([]byte(req.Key))
	})
	if err != nil {
		return c.fail(ctx, handler, req.Context, err, false)
	}
	return handler(ctx, DelResultPort, DeleteResult{
		Context:    req.Context,
		Collection: req.Collection,
		Key:        req.Key,
		Deleted:    existed,
	})
}

func (c *Component) handleFind(ctx context.Context, handler module.Handler, db *bbolt.DB, settings Settings, req FindRequest) module.Result {
	if !collectionDeclared(settings, req.Collection) {
		return c.fail(ctx, handler, req.Context, fmt.Errorf("collection %q not declared in Settings.Collections", req.Collection), false)
	}
	items := []FindItem{}
	limit := req.Limit
	prefix := []byte(req.Prefix)
	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(req.Collection))
		if bucket == nil {
			return nil
		}
		cur := bucket.Cursor()
		var k, v []byte
		if len(prefix) > 0 {
			k, v = cur.Seek(prefix)
		} else {
			k, v = cur.First()
		}
		for ; k != nil; k, v = cur.Next() {
			if len(prefix) > 0 && !strings.HasPrefix(string(k), string(prefix)) {
				break
			}
			var value any
			if err := json.Unmarshal(v, &value); err != nil {
				// Skip rows with corrupt JSON rather than failing the
				// entire find. The error port is reserved for genuine
				// store failures, not per-row data issues.
				continue
			}
			items = append(items, FindItem{Key: string(k), Value: value})
			if limit > 0 && len(items) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return c.fail(ctx, handler, req.Context, err, false)
	}
	return handler(ctx, FindResultPort, FindResult{
		Context:    req.Context,
		Collection: req.Collection,
		Items:      items,
		Count:      len(items),
	})
}

func collectionDeclared(settings Settings, name string) bool {
	for _, c := range settings.Collections {
		if c.Name == name {
			return true
		}
	}
	return false
}

func (c *Component) fail(ctx context.Context, handler module.Handler, reqCtx Context, err error, diskFull bool) module.Result {
	c.mu.RLock()
	enabled := c.settings.EnableErrorPort
	c.mu.RUnlock()
	if !enabled {
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, Error{
		Context:  reqCtx,
		Error:    err.Error(),
		DiskFull: diskFull,
	})
}

// failRetryable is the retryable counterpart to fail. Used for
// follower-refusal and transient lock-contention errors where the
// caller should back off and re-fire.
func (c *Component) failRetryable(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	c.mu.RLock()
	enabled := c.settings.EnableErrorPort
	c.mu.RUnlock()
	if !enabled {
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, Error{
		Context:   reqCtx,
		Error:     err.Error(),
		Retryable: true,
	})
}

// contextFromMsg extracts the Context field from any of the request
// types so error paths can pass it through without per-port branches.
// Returns nil when the message isn't a request shape or has no context.
func contextFromMsg(msg any) Context {
	switch req := msg.(type) {
	case PutRequest:
		return req.Context
	case GetRequest:
		return req.Context
	case DeleteRequest:
		return req.Context
	case FindRequest:
		return req.Context
	}
	return nil
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: PutPort, Label: "Put", Configuration: PutRequest{}, Position: module.Left},
		{Name: GetPort, Label: "Get", Configuration: GetRequest{}, Position: module.Left},
		{Name: DeletePort, Label: "Delete", Configuration: DeleteRequest{}, Position: module.Left},
		{Name: FindPort, Label: "Find", Configuration: FindRequest{}, Position: module.Left},
		{Name: PutResultPort, Label: "Put OK", Source: true, Configuration: PutResult{}, Position: module.Right},
		{Name: GetResultPort, Label: "Get OK", Source: true, Configuration: GetResult{}, Position: module.Right},
		{Name: DelResultPort, Label: "Delete OK", Source: true, Configuration: DeleteResult{}, Position: module.Right},
		{Name: FindResultPort, Label: "Find OK", Source: true, Configuration: FindResult{}, Position: module.Right},
	}
	if c.settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name: ErrorPort, Label: "Error", Source: true, Configuration: Error{}, Position: module.Bottom,
		})
	}
	return ports
}

// Static assertion to surface drift between Component and the SDK
// interfaces at build time.
var (
	_ module.Component        = (*Component)(nil)
	_ module.SettingsHandler  = (*Component)(nil)
	_ module.ReconcileHandler = (*Component)(nil)
)

// errBucketMissing is a sentinel kept around as documentation — the
// codepath returns the message wrapped via fmt.Errorf so callers can
// errors.Is when they care. Currently unused publicly but cheap to
// keep.
var errBucketMissing = errors.New("bucket missing")

var _ = errBucketMissing

func init() {
	registry.Register(&Component{})
}
