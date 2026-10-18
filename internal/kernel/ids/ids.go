package ids

import (
	"errors"

	"github.com/google/uuid"
)

var (
	ErrInvalidID = errors.New("invalid id: must be a valid UUIDv7")
)

type RequestID string
type ReservationID string

// NewRequestID generates a new UUIDv7 time-ordered request identifier.
func NewRequestID() (RequestID, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return RequestID(u.String()), nil
}

// ParseRequestID parses and validates that s is a valid UUIDv7.
func ParseRequestID(s string) (RequestID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return "", err
	}
	if u.Version() != 7 {
		return "", ErrInvalidID
	}
	return RequestID(u.String()), nil
}

// NewReservationID generates a new UUIDv7 time-ordered reservation identifier.
func NewReservationID() (ReservationID, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return ReservationID(u.String()), nil
}

// ParseReservationID parses and validates that s is a valid UUIDv7.
func ParseReservationID(s string) (ReservationID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return "", err
	}
	if u.Version() != 7 {
		return "", ErrInvalidID
	}
	return ReservationID(u.String()), nil
}
