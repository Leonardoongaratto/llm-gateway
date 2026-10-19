package auth_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/gen/lgw/control/v1"
	"github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockControlClient struct {
	controlv1.ControlServiceClient
	mu       sync.Mutex
	snapshot *controlv1.Snapshot
	err      error
	calls    int
}

func (m *mockControlClient) GetSnapshot(_ context.Context, _ *controlv1.GetSnapshotRequest, _ ...grpc.CallOption) (*controlv1.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	return m.snapshot, nil
}

func (m *mockControlClient) setSnapshot(s *controlv1.Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshot = s
}

func hashKey(rawKey, pepper string) string {
	h := sha256.Sum256([]byte(rawKey + pepper))
	return hex.EncodeToString(h[:])
}

const (
	testPepper   = "test-pepper-12345"
	validKey1    = "lgw_0123456789abcdef0123456789abcdef"
	validKey2    = "lgw_abcdef0123456789abcdef0123456789"
	expiredKey   = "lgw_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	inactiveKey  = "lgw_ffffffffffffffffffffffffffffffff"
	unallowedKey = "lgw_11111111111111111111111111111111"
)

func runConcurrentReaders(ctx context.Context, wg *sync.WaitGroup, mgr *auth.Manager, t *testing.T) {
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				k, err := mgr.Authenticate(validKey1)
				if err != nil && !errors.Is(err, llm.ErrInvalidKey) {
					t.Errorf("unexpected error in auth: %v", err)
					return
				}
				if k != nil && k.ID != "key-1" {
					t.Errorf("expected key-1, got %v", k.ID)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}
}

func runConcurrentUpdater(ctx context.Context, wg *sync.WaitGroup, mgr *auth.Manager) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		v := int64(2)
		for ctx.Err() == nil {
			mgr.UpdateSnapshot(&auth.Snapshot{
				Version: v,
				KeysByHash: map[string]*auth.KeyInfo{
					hashKey(validKey1, testPepper): {
						ID:            "key-1",
						Name:          "Key One",
						KeyHash:       hashKey(validKey1, testPepper),
						AllowedModels: []string{"gpt-4o"},
						Active:        true,
					},
				},
			})
			v++
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

func TestSnapshot_AtomicSwap(t *testing.T) {
	mgr, err := auth.NewManager(testPepper, nil)
	if err != nil {
		t.Fatalf("unexpected error creating manager: %v", err)
	}

	snap1 := &auth.Snapshot{
		Version: 1,
		KeysByHash: map[string]*auth.KeyInfo{
			hashKey(validKey1, testPepper): {
				ID:            "key-1",
				Name:          "Key One",
				KeyHash:       hashKey(validKey1, testPepper),
				AllowedModels: []string{"gpt-4o"},
				Active:        true,
			},
		},
	}
	mgr.UpdateSnapshot(snap1)

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	runConcurrentReaders(ctx, &wg, mgr, t)
	runConcurrentUpdater(ctx, &wg, mgr)

	wg.Wait()
	if mgr.GetSnapshot().Version < 2 {
		t.Fatalf("expected version >= 2, got %d", mgr.GetSnapshot().Version)
	}
}

func verifySnapshotKeys(t *testing.T, snap *auth.Snapshot) {
	t.Helper()
	key := snap.FindKeyByHash(hashKey(validKey1, testPepper))
	if key == nil || key.ID != "k-1" {
		t.Fatalf("expected key k-1, got %v", key)
	}
	if key.RPM != 60 || key.DailyLimitMicros != 10000000 {
		t.Errorf("unexpected key limits: %+v", key)
	}
}

func verifySnapshotPricing(t *testing.T, snap *auth.Snapshot) {
	t.Helper()
	p := snap.FindPricing("claude-3-5-sonnet", "anthropic")
	if p == nil || p.InputPerMTokMicros != 3000000 {
		t.Fatalf("expected pricing for claude-3-5-sonnet, got %+v", p)
	}
	pFallback := snap.FindPricing("claude-3-5-sonnet", "")
	if pFallback == nil || pFallback.InputPerMTokMicros != 3000000 {
		t.Fatalf("expected fallback pricing for claude-3-5-sonnet, got %+v", pFallback)
	}
	if snap.FindPricing("unknown-model", "") != nil {
		t.Fatal("expected nil pricing for unknown model")
	}
}

func verifySnapshotLogicalModels(t *testing.T, snap *auth.Snapshot) {
	t.Helper()
	chain := snap.ResolveChain("rapido")
	if len(chain) != 2 || chain[0] != "claude-3-5-haiku" || chain[1] != "gpt-4o-mini" {
		t.Fatalf("expected chain [claude-3-5-haiku, gpt-4o-mini], got %v", chain)
	}
	directChain := snap.ResolveChain("gpt-4o")
	if len(directChain) != 1 || directChain[0] != "gpt-4o" {
		t.Fatalf("expected direct chain [gpt-4o], got %v", directChain)
	}
}

func verifyNilReceivers(t *testing.T) {
	t.Helper()
	var nilSnap *auth.Snapshot
	if nilSnap.FindKeyByHash("abc") != nil {
		t.Fatal("expected nil for nilSnap.FindKeyByHash")
	}
	if nilSnap.FindPricing("m", "p") != nil {
		t.Fatal("expected nil for nilSnap.FindPricing")
	}
	if res := nilSnap.ResolveChain("m"); len(res) != 1 || res[0] != "m" {
		t.Fatalf("expected fallback for nilSnap.ResolveChain, got %v", res)
	}
	var nilKey *auth.KeyInfo
	if !nilKey.IsExpired(time.Now()) {
		t.Fatal("nilKey should be considered expired")
	}
	if nilKey.IsModelAllowed("m") {
		t.Fatal("nilKey should not allow any model")
	}
}

func TestManager_LoadSnapshot(t *testing.T) {
	mockClient := &mockControlClient{
		snapshot: &controlv1.Snapshot{
			Version: 42,
			Keys: []*controlv1.KeyInfo{
				{
					Id:                 "k-1",
					Name:               "Test Key",
					KeyHash:            hashKey(validKey1, testPepper),
					AllowedModels:      []string{"claude-3-5-sonnet", "gpt-4o"},
					Active:             true,
					DailyLimitMicros:   10000000,
					MonthlyLimitMicros: 50000000,
					Rpm:                60,
				},
			},
			Pricing: []*controlv1.PricingInfo{
				{
					Model:                  "claude-3-5-sonnet",
					Provider:               "anthropic",
					InputPerMtokMicros:     3000000,
					OutputPerMtokMicros:    15000000,
					CacheReadPerMtokMicros: 300000,
				},
			},
			LogicalModels: []*controlv1.LogicalModelInfo{
				{
					Name:  "rapido",
					Chain: []string{"claude-3-5-haiku", "gpt-4o-mini"},
				},
			},
		},
	}

	mgr, err := auth.NewManager(testPepper, mockClient)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	ctx := context.Background()
	if err := mgr.LoadSnapshot(ctx); err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	snap := mgr.GetSnapshot()
	if snap == nil || snap.Version != 42 {
		t.Fatalf("unexpected snapshot version: %+v", snap)
	}

	verifySnapshotKeys(t, snap)
	verifySnapshotPricing(t, snap)
	verifySnapshotLogicalModels(t, snap)
	verifyNilReceivers(t)
}

func TestManager_EdgeCases(t *testing.T) {
	if _, err := auth.NewManager("", nil); !errors.Is(err, auth.ErrEmptyPepper) {
		t.Fatalf("expected ErrEmptyPepper, got %v", err)
	}

	mgr, err := auth.NewManager("pepper", nil, auth.WithLogger(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mgr.UpdateSnapshot(nil)

	if _, err := mgr.StartConfigWatcher(context.Background(), nil); err == nil {
		t.Fatal("expected error on nil JetStream")
	}

	emptySnap := auth.FromProto(nil)
	if emptySnap == nil || len(emptySnap.KeysByHash) != 0 {
		t.Fatal("expected empty snapshot from nil proto")
	}
}

func assertAuthResult(t *testing.T, k *auth.KeyInfo, err, wantErr error, wantID string) {
	t.Helper()
	if wantErr != nil {
		if !errors.Is(err, wantErr) {
			t.Fatalf("expected error %v, got %v", wantErr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if k.ID != wantID {
		t.Fatalf("expected key id %s, got %s", wantID, k.ID)
	}
}

func TestManager_Authenticate(t *testing.T) {
	past := time.Now().Add(-1 * time.Hour)
	future := time.Now().Add(24 * time.Hour)

	mockClient := &mockControlClient{
		snapshot: &controlv1.Snapshot{
			Version: 1,
			Keys: []*controlv1.KeyInfo{
				{
					Id:            "k-valid",
					KeyHash:       hashKey(validKey1, testPepper),
					Active:        true,
					AllowedModels: []string{"claude-3-5-sonnet"},
					ExpiresAt:     timestamppb.New(future),
				},
				{
					Id:        "k-expired",
					KeyHash:   hashKey(expiredKey, testPepper),
					Active:    true,
					ExpiresAt: timestamppb.New(past),
				},
				{
					Id:      "k-inactive",
					KeyHash: hashKey(inactiveKey, testPepper),
					Active:  false,
				},
			},
		},
	}

	mgr, err := auth.NewManager(testPepper, mockClient)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	if err := mgr.LoadSnapshot(context.Background()); err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	tests := []struct {
		name    string
		rawKey  string
		wantErr error
		wantID  string
	}{
		{name: "valid key", rawKey: validKey1, wantErr: nil, wantID: "k-valid"},
		{name: "invalid prefix", rawKey: "sk_0123456789abcdef0123456789abcdef", wantErr: llm.ErrInvalidKey},
		{name: "too short", rawKey: "lgw_abc", wantErr: llm.ErrInvalidKey},
		{name: "non hex chars", rawKey: "lgw_0123456789abcdef0123456789zzzzzz", wantErr: llm.ErrInvalidKey},
		{name: "unknown key", rawKey: validKey2, wantErr: llm.ErrInvalidKey},
		{name: "expired key", rawKey: expiredKey, wantErr: llm.ErrInvalidKey},
		{name: "inactive key", rawKey: inactiveKey, wantErr: llm.ErrInvalidKey},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k, err := mgr.Authenticate(tc.rawKey)
			assertAuthResult(t, k, err, tc.wantErr, tc.wantID)
		})
	}
}

func TestManager_AuthorizeModel(t *testing.T) {
	mgr, _ := auth.NewManager(testPepper, nil)

	wildcardKey := &auth.KeyInfo{
		ID:            "wildcard",
		AllowedModels: nil, // empty allows all
		Active:        true,
	}
	restrictedKey := &auth.KeyInfo{
		ID:            "restricted",
		AllowedModels: []string{"gpt-4o", "claude-3-5-sonnet"},
		Active:        true,
	}

	// Wildcard allows any model
	if err := mgr.AuthorizeModel(wildcardKey, "gpt-4o"); err != nil {
		t.Errorf("wildcard should allow gpt-4o, got %v", err)
	}
	if err := mgr.AuthorizeModel(wildcardKey, "gemini-1.5-pro"); err != nil {
		t.Errorf("wildcard should allow gemini-1.5-pro, got %v", err)
	}

	// Restricted allows only listed
	if err := mgr.AuthorizeModel(restrictedKey, "gpt-4o"); err != nil {
		t.Errorf("restricted should allow gpt-4o, got %v", err)
	}
	if err := mgr.AuthorizeModel(restrictedKey, "claude-3-5-sonnet"); err != nil {
		t.Errorf("restricted should allow claude-3-5-sonnet, got %v", err)
	}
	if err := mgr.AuthorizeModel(restrictedKey, "gemini-1.5-pro"); !errors.Is(err, llm.ErrModelNotAllowed) {
		t.Errorf("restricted should deny gemini-1.5-pro with ErrModelNotAllowed, got %v", err)
	}
}

func setupTestNats(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	srv, natsURL := natsx.StartEmbeddedServer(t)
	nc, js, err := natsx.Connect(natsURL)
	if err != nil {
		t.Fatalf("failed to connect to embedded nats: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := natsx.InitStreams(ctx, js); err != nil {
		t.Fatalf("failed to init streams: %v", err)
	}
	return srv, nc, js
}

func TestManager_StartConfigWatcher(t *testing.T) {
	_, _, js := setupTestNats(t)

	mockClient := &mockControlClient{
		snapshot: &controlv1.Snapshot{
			Version: 1,
			Keys: []*controlv1.KeyInfo{
				{
					Id:      "k-1",
					KeyHash: hashKey(validKey1, testPepper),
					Active:  true,
				},
			},
		},
	}

	mgr, err := auth.NewManager(testPepper, mockClient)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := mgr.LoadSnapshot(ctx); err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	sub, err := mgr.StartConfigWatcher(ctx, js)
	if err != nil {
		t.Fatalf("StartConfigWatcher failed: %v", err)
	}
	defer sub.Stop()

	mockClient.setSnapshot(&controlv1.Snapshot{
		Version: 2,
		Keys: []*controlv1.KeyInfo{
			{
				Id:      "k-1",
				KeyHash: hashKey(validKey1, testPepper),
				Active:  true,
			},
			{
				Id:      "k-2",
				KeyHash: hashKey(validKey2, testPepper),
				Active:  true,
			},
		},
	})

	cfgEvt := &eventsv1.ConfigChanged{
		Kind:    eventsv1.ConfigChanged_KEY,
		Version: 2,
	}
	evtBytes, err := protojson.Marshal(cfgEvt)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	_, err = natsx.Publish(ctx, js, "lgw.config.changed.v1", evtBytes, "cfg-evt-1")
	if err != nil {
		t.Fatalf("failed to publish config changed event: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var updated bool
	for time.Now().Before(deadline) {
		k, err := mgr.Authenticate(validKey2)
		if err == nil && k != nil && k.ID == "k-2" {
			updated = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !updated {
		t.Fatalf("expected snapshot to update and authenticate key-2 within 2 seconds")
	}
}

func TestManager_IsLoaded(t *testing.T) {
	mgr, err := auth.NewManager("pepper", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mgr.IsLoaded() {
		t.Error("expected IsLoaded to be false initially")
	}

	mgr.UpdateSnapshot(&auth.Snapshot{Version: 1})
	if !mgr.IsLoaded() {
		t.Error("expected IsLoaded to be true after UpdateSnapshot")
	}
}
