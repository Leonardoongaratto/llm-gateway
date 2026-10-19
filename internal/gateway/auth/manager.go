package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/gen/lgw/control/v1"
	"github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	KeyPrefix      = "lgw_"
	KeyTotalLength = 36 // 4 prefix + 32 hex chars
)

// ErrEmptyPepper is returned when NewManager is called with an empty pepper.
var ErrEmptyPepper = errors.New("key pepper must not be empty")

// ManagerOption configures the auth Manager.
type ManagerOption func(*Manager)

// WithLogger sets the logger for the Manager.
func WithLogger(l *slog.Logger) ManagerOption {
	return func(m *Manager) {
		if l != nil {
			m.logger = l
		}
	}
}

// Manager manages in-memory authentication and snapshot synchronization for the gateway.
type Manager struct {
	pepper string
	client controlv1.ControlServiceClient
	snap   atomic.Pointer[Snapshot]
	logger *slog.Logger
}

// NewManager creates a new auth Manager with the given pepper and control gRPC client.
func NewManager(pepper string, client controlv1.ControlServiceClient, opts ...ManagerOption) (*Manager, error) {
	if pepper == "" {
		return nil, ErrEmptyPepper
	}

	m := &Manager{
		pepper: pepper,
		client: client,
		logger: slog.Default(),
	}
	m.snap.Store(&Snapshot{
		KeysByHash:    make(map[string]*KeyInfo),
		Pricing:       make(map[string]*PricingInfo),
		LogicalModels: make(map[string][]string),
	})

	for _, opt := range opts {
		opt(m)
	}

	return m, nil
}

// LoadSnapshot fetches the authoritative snapshot from the control service and stores it atomically.
func (m *Manager) LoadSnapshot(ctx context.Context) error {
	if m.client == nil {
		return nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	protoSnap, err := m.client.GetSnapshot(reqCtx, &controlv1.GetSnapshotRequest{})
	if err != nil {
		return fmt.Errorf("failed to fetch snapshot from control service: %w", err)
	}

	snap := FromProto(protoSnap)
	m.snap.Store(snap)
	m.logger.InfoContext(ctx, "loaded snapshot from control service",
		slog.Int64("version", snap.Version),
		slog.Int("keys_count", len(snap.KeysByHash)),
		slog.Int("pricing_count", len(snap.Pricing)),
	)
	return nil
}

// UpdateSnapshot replaces the in-memory snapshot atomically.
func (m *Manager) UpdateSnapshot(s *Snapshot) {
	if s == nil {
		return
	}
	m.snap.Store(s)
}

// GetSnapshot returns the current in-memory snapshot.
func (m *Manager) GetSnapshot() *Snapshot {
	return m.snap.Load()
}

// Authenticate verifies the raw virtual key and returns its metadata if valid, active, and not expired.
func (m *Manager) Authenticate(rawKey string) (*KeyInfo, error) {
	if len(rawKey) != KeyTotalLength || !strings.HasPrefix(rawKey, KeyPrefix) {
		return nil, llm.ErrInvalidKey
	}

	hexPart := rawKey[len(KeyPrefix):]
	for i := 0; i < len(hexPart); i++ {
		c := hexPart[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return nil, llm.ErrInvalidKey
		}
	}

	h := sha256.Sum256([]byte(rawKey + m.pepper))
	hashStr := hex.EncodeToString(h[:])

	snap := m.snap.Load()
	if snap == nil {
		return nil, llm.ErrInvalidKey
	}

	key := snap.FindKeyByHash(hashStr)
	if key == nil || !key.Active || key.IsExpired(time.Now()) {
		return nil, llm.ErrInvalidKey
	}

	return key, nil
}

// AuthorizeModel verifies that the given model is permitted for the key.
func (m *Manager) AuthorizeModel(key *KeyInfo, model string) error {
	if !key.IsModelAllowed(model) {
		return llm.ErrModelNotAllowed
	}
	return nil
}

// StartConfigWatcher starts a JetStream consumer listening for lgw.config.changed.v1 events.
func (m *Manager) StartConfigWatcher(ctx context.Context, js jetstream.JetStream) (jetstream.ConsumeContext, error) {
	if js == nil {
		return nil, errors.New("jetstream context is nil")
	}

	setupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	consumer, err := js.CreateOrUpdateConsumer(setupCtx, natsx.StreamConfig, jetstream.ConsumerConfig{
		Durable:       "gateway-config-watcher",
		FilterSubject: "lgw.config.changed.v1",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    5,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create config watcher consumer: %w", err)
	}

	return consumer.Consume(func(msg jetstream.Msg) {
		msgCtx := natsx.ExtractTraceContext(ctx, msg)
		var cfgEvent eventsv1.ConfigChanged
		if err := protojson.Unmarshal(msg.Data(), &cfgEvent); err != nil {
			m.logger.WarnContext(msgCtx, "failed to unmarshal config changed event",
				slog.Any("error", err),
			)
			_ = msg.Ack()
			return
		}

		m.logger.InfoContext(msgCtx, "received config changed event, reloading snapshot",
			slog.Int64("event_version", cfgEvent.Version),
			slog.String("kind", cfgEvent.Kind.String()),
		)

		if err := m.LoadSnapshot(msgCtx); err != nil {
			m.logger.ErrorContext(msgCtx, "failed to reload snapshot on config event",
				slog.Any("error", err),
			)
			_ = msg.Nak()
			return
		}

		_ = msg.Ack()
	})
}
