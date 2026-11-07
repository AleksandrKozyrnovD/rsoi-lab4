package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"

	"gateway/models"
)


var (
	ErrNotFound           = errors.New("not found")
	ErrAlreadyCanceled    = errors.New("already canceled")
	ErrServiceUnavailable = errors.New("service unavailable")
)


type ClientOption func(*serviceClient)

// WithRetryConfig overrides the default retry/backoff settings.
func WithRetryConfig(cfg RetryConfig) ClientOption {
	return func(s *serviceClient) {
		s.retryCfg = cfg
	}
}

// WithCircuitBreakerConfig overrides the default circuit-breaker settings.
func WithCircuitBreakerConfig(cfg CircuitBreakerConfig) ClientOption {
	return func(s *serviceClient) {
		s.cbCfg = cfg
		s.cb = newCircuitBreaker[any](s.baseURL, cfg)
	}
}

// serviceClient — обёртка над http.Client с baseURL, CB и retry
type serviceClient struct {
	baseURL  string
	http     *http.Client
	retryCfg RetryConfig
	cbCfg    CircuitBreakerConfig
	cb       *CircuitBreaker[any]
}

func newServiceClient(baseURL string) *serviceClient {
	return &serviceClient{
		baseURL:  baseURL,
		http:     &http.Client{Timeout: 10 * time.Second},
		retryCfg: DefaultRetryConfig(),
		cbCfg:    DefaultCircuitBreakerConfig(),
		cb:       newCircuitBreaker[any](baseURL, DefaultCircuitBreakerConfig()),
	}
}

// forwardRequestRaw performs an HTTP request with retry + exponential backoff.
// Returns (statusCode, error).
//   - For 4xx responses statusCode is set and error is nil.
//   - For 5xx / network errors it returns ErrServiceUnavailable after retries.
func (s *serviceClient) forwardRequestRaw(
	method, path string,
	body any,
	headers map[string]string,
	out any,
) (int, error) {
	var status int

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := retryExecute(ctx, s.retryCfg, func() error {
		var reader io.Reader
		if body != nil {
			data, err := json.Marshal(body)
			if err != nil {
				return backoff.Permanent(err)
			}
			reader = bytes.NewReader(data)
		}

		req, err := http.NewRequest(method, s.baseURL+path, reader)
		if err != nil {
			return backoff.Permanent(err)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := s.http.Do(req)
		if err != nil {
			return err // retryable: network
		}
		defer resp.Body.Close()

		// 5xx — retryable
		if resp.StatusCode >= 500 {
			return fmt.Errorf("server error %d", resp.StatusCode)
		}

		// Only decode successful responses (4xx bodies are error payloads
		// that we do not need to unmarshal into the success model).
		if out != nil && resp.StatusCode < 400 {
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return err // retryable
			}
			if len(data) > 0 {
				if err := json.Unmarshal(data, out); err != nil {
					return backoff.Permanent(err)
				}
			}
		}

		status = resp.StatusCode
		return nil
	})

	if err != nil {
		return 0, ErrServiceUnavailable
	}
	return status, nil
}

// forwardRequest optionally wraps forwardRequestRaw with a circuit breaker.
func (s *serviceClient) forwardRequest(
	method, path string,
	body any,
	headers map[string]string,
	out any,
	useCB bool,
) (int, error) {
	if useCB {
		var status int
		var innerErr error
		_, cbErr := s.cb.Execute(func() (any, error) {
			status, innerErr = s.forwardRequestRaw(method, path, body, headers, out)
			return nil, innerErr
		})
		if cbErr != nil {
			return 0, ErrServiceUnavailable
		}
		return status, innerErr
	}
	return s.forwardRequestRaw(method, path, body, headers, out)
}

// LoyaltyService
type LoyaltyClient struct {
	svc *serviceClient
}

func NewLoyaltyClient(baseURL string, opts ...ClientOption) *LoyaltyClient {
	svc := newServiceClient(baseURL)
	for _, opt := range opts {
		opt(svc)
	}
	return &LoyaltyClient{svc: svc}
}

// GetLoyalty — read operation, uses Circuit Breaker.
func (l *LoyaltyClient) GetLoyalty(username string) (*models.LoyaltyInfoResponse, error) {
	var out models.LoyaltyInfoResponse
	status, err := l.svc.forwardRequest(
		http.MethodGet,
		"/api/v1/loyalty/"+url.PathEscape(username),
		nil, nil, &out,
		true, // use CB
	)
	if err != nil {
		return nil, err
	}
	switch {
	case status == http.StatusOK:
		return &out, nil
	case status == http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("loyalty service returned status %d", status)
	}
}

// CreateLoyalty — write operation, no Circuit Breaker.
func (l *LoyaltyClient) CreateLoyalty(username string) (*models.LoyaltyInfoResponse, error) {
	req := models.LoyaltyRequest{
		Username:         username,
		ReservationCount: 0,
		Status:           models.StatusBronze,
		Discount:         5,
	}
	var out models.LoyaltyInfoResponse
	status, err := l.svc.forwardRequest(
		http.MethodPost,
		"/api/v1/loyalty",
		req, nil, &out,
		false,
	)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, fmt.Errorf("loyalty service returned status %d", status)
	}
	return &out, nil
}

func (l *LoyaltyClient) IncrementReservations(username string) error {
	status, err := l.svc.forwardRequest(
		http.MethodPost,
		"/api/v1/loyalty/"+url.PathEscape(username)+"/reservations",
		nil, nil, nil,
		false,
	)
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("loyalty service returned status %d", status)
	}
	return nil
}

func (l *LoyaltyClient) DecrementReservations(username string) error {
	status, err := l.svc.forwardRequest(
		http.MethodDelete,
		"/api/v1/loyalty/"+url.PathEscape(username)+"/reservations",
		nil, nil, nil,
		false,
	)
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("loyalty service returned status %d", status)
	}
	return nil
}

// ReservationService
type ReservationClient struct {
	svc *serviceClient
}

func NewReservationClient(baseURL string, opts ...ClientOption) *ReservationClient {
	svc := newServiceClient(baseURL)
	for _, opt := range opts {
		opt(svc)
	}
	return &ReservationClient{svc: svc}
}

// ListHotels — read operation, uses Circuit Breaker.
func (r *ReservationClient) ListHotels(page, size int) (*models.PaginationResponse, error) {
	var out models.PaginationResponse
	status, err := r.svc.forwardRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/hotels?page=%d&size=%d", page, size),
		nil, nil, &out,
		true,
	)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, fmt.Errorf("reservation service returned status %d", status)
	}
	return &out, nil
}

// ListReservations — read operation, uses Circuit Breaker.
func (r *ReservationClient) ListReservations(username string) ([]models.ReservationResponse, error) {
	var out []models.ReservationResponse
	status, err := r.svc.forwardRequest(
		http.MethodGet,
		"/api/v1/reservations",
		nil, map[string]string{"X-User-Name": username}, &out,
		true,
	)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, fmt.Errorf("reservation service returned status %d", status)
	}
	if out == nil {
		out = []models.ReservationResponse{}
	}
	return out, nil
}

// GetReservation — read operation, uses Circuit Breaker.
func (r *ReservationClient) GetReservation(username, reservationUid string) (*models.ReservationResponse, error) {
	var out models.ReservationResponse
	status, err := r.svc.forwardRequest(
		http.MethodGet,
		"/api/v1/reservations/"+url.PathEscape(reservationUid),
		nil, map[string]string{"X-User-Name": username}, &out,
		true,
	)
	if err != nil {
		return nil, err
	}
	switch {
	case status == http.StatusOK:
		return &out, nil
	case status == http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("reservation service returned status %d", status)
	}
}

// CreateReservation — write operation, no Circuit Breaker.
func (r *ReservationClient) CreateReservation(
	username string,
	req models.CreateReservationRequest,
) (*models.CreateReservationResponse, error) {
	var out models.CreateReservationResponse
	status, err := r.svc.forwardRequest(
		http.MethodPost,
		"/api/v1/reservations",
		req, map[string]string{"X-User-Name": username}, &out,
		false,
	)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, fmt.Errorf("reservation service returned status %d", status)
	}
	return &out, nil
}

// CancelReservation — write operation, no Circuit Breaker.
func (r *ReservationClient) CancelReservation(username, reservationUid string) error {
	status, err := r.svc.forwardRequest(
		http.MethodDelete,
		"/api/v1/reservations/"+url.PathEscape(reservationUid),
		nil, map[string]string{"X-User-Name": username}, nil,
		false,
	)
	if err != nil {
		return err
	}
	switch {
	case status == http.StatusNotFound:
		return ErrNotFound
	case status == http.StatusBadRequest:
		return ErrAlreadyCanceled
	case status >= 400:
		return fmt.Errorf("reservation service returned status %d", status)
	}
	return nil
}

// UncancelReservation — compensating action for rolling back a cancellation.
func (r *ReservationClient) UncancelReservation(username, reservationUid string) error {
	status, err := r.svc.forwardRequest(
		http.MethodPatch,
		"/api/v1/reservations/"+url.PathEscape(reservationUid)+"/uncancel",
		nil, map[string]string{"X-User-Name": username}, nil,
		false,
	)
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("reservation service returned status %d", status)
	}
	return nil
}

// ============================================================
// PaymentService
// ============================================================

type PaymentClient struct {
	svc *serviceClient
}

func NewPaymentClient(baseURL string, opts ...ClientOption) *PaymentClient {
	svc := newServiceClient(baseURL)
	for _, opt := range opts {
		opt(svc)
	}
	return &PaymentClient{svc: svc}
}

// Pay — write operation, no Circuit Breaker.
func (p *PaymentClient) Pay(price int) (*models.PaymentResponse, error) {
	var out models.PaymentResponse
	status, err := p.svc.forwardRequest(
		http.MethodPost,
		"/api/v1/payment",
		models.PaymentRequest{Price: price},
		nil, &out,
		false,
	)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, fmt.Errorf("payment service returned status %d", status)
	}
	return &out, nil
}

// Reverse — compensating action that reverses a previously successful payment.
func (p *PaymentClient) Reverse(paymentUid string) error {
	status, err := p.svc.forwardRequest(
		http.MethodPost,
		"/api/v1/payment/"+url.PathEscape(paymentUid)+"/reverse",
		nil, nil, nil,
		false,
	)
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("payment service returned status %d", status)
	}
	return nil
}
