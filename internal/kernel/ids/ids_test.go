package ids_test

import (
	"sort"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/ids"
)

// AC-1A-05: Given 10.000 RequestID gerados em sequência, when ordenados como texto,
// then a ordem coincide com a de geração e não há duplicados; um ID malformado é rejeitado no parse.
func TestRequestID_SequenceAndUniqueness_AC1A05(t *testing.T) {
	const n = 10000
	generated := make([]ids.RequestID, n)
	seen := make(map[ids.RequestID]struct{}, n)

	for i := 0; i < n; i++ {
		id, err := ids.NewRequestID()
		if err != nil {
			t.Fatalf("failed to generate request ID: %v", err)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate RequestID generated: %s", id)
		}
		seen[id] = struct{}{}
		generated[i] = id
	}

	sorted := make([]ids.RequestID, n)
	copy(sorted, generated)
	sort.Slice(sorted, func(i, j int) bool {
		return string(sorted[i]) < string(sorted[j])
	})

	for i := 0; i < n; i++ {
		if generated[i] != sorted[i] {
			t.Fatalf("order mismatch at index %d: generated=%s, sorted=%s", i, generated[i], sorted[i])
		}
	}
}

func TestParseRequestID_RejectsMalformed(t *testing.T) {
	malformed := []string{
		"",
		"invalid-uuid",
		"12345678-1234-1234-1234-123456789012", // UUIDv1 or invalid version
		"00000000-0000-0000-0000-000000000000", // nil UUID
		"018e3810-7b56-4b6e-826a-93a0279d026b", // UUIDv4 (not v7)
	}

	for _, s := range malformed {
		_, err := ids.ParseRequestID(s)
		if err == nil {
			t.Errorf("expected error for malformed ID %q, got nil", s)
		}
	}

	valid, err := ids.NewRequestID()
	if err != nil {
		t.Fatalf("unexpected error creating valid request ID: %v", err)
	}
	parsed, err := ids.ParseRequestID(string(valid))
	if err != nil {
		t.Fatalf("unexpected error parsing valid request ID: %v", err)
	}
	if parsed != valid {
		t.Fatalf("parsed ID %s does not match valid ID %s", parsed, valid)
	}
}

func TestReservationID(t *testing.T) {
	valid, err := ids.NewReservationID()
	if err != nil {
		t.Fatalf("unexpected error creating reservation ID: %v", err)
	}
	parsed, err := ids.ParseReservationID(string(valid))
	if err != nil {
		t.Fatalf("unexpected error parsing valid reservation ID: %v", err)
	}
	if parsed != valid {
		t.Fatalf("parsed reservation ID %s does not match %s", parsed, valid)
	}

	_, err = ids.ParseReservationID("bad-id")
	if err == nil {
		t.Fatal("expected error parsing malformed reservation ID")
	}
}
