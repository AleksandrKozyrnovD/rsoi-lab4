package api

import (
	"errors"
	"sync"
	"time"
)

// State — состояние circuit breaker.
type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

var (
	// ErrOpenState возвращается, когда breaker открыт.
	ErrOpenState = errors.New("circuit breaker is open")
	// ErrTooManyRequests возвращается в half-open, если превышен MaxRequests.
	ErrTooManyRequests = errors.New("too many requests")
)

// Counts — счётчики вызовов в текущем окне.
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

func (c *Counts) onRequest() {
	c.Requests++
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

// ReadyToTrip решает, открывать ли breaker на основе текущих счётчиков окна.
type ReadyToTrip func(Counts) bool

// Settings — конфигурация circuit breaker.
type Settings struct {
	Name string

	// MaxRequests — макс. запросов в half-open.
	MaxRequests uint32

	// Interval — длительность окна счётчиков в closed-состоянии.
	// По истечении окно сбрасывается целиком (фиксированное окно).
	Interval time.Duration

	// Buckets — DEPRECATED и игнорируется.
	// Оставлено для совместимости с прежней конфигурацией.
	Buckets int

	// Timeout — время нахождения в open до half-open.
	Timeout time.Duration

	ReadyToTrip   ReadyToTrip
	OnStateChange func(name string, from, to State)
	IsSuccessful  func(err error) bool
}

func (s Settings) withDefaults() Settings {
	if s.MaxRequests == 0 {
		s.MaxRequests = 1
	}
	if s.Interval == 0 {
		s.Interval = 60 * time.Second
	}
	if s.Timeout == 0 {
		s.Timeout = 60 * time.Second
	}
	if s.ReadyToTrip == nil {
		s.ReadyToTrip = func(c Counts) bool { return c.ConsecutiveFailures > 5 }
	}
	if s.IsSuccessful == nil {
		s.IsSuccessful = func(err error) bool { return err == nil }
	}
	return s
}

// CircuitBreaker — generic circuit breaker с фиксированным окном счётчиков.
type CircuitBreaker[T any] struct {
	mu         sync.Mutex
	settings   Settings
	state      State
	generation uint64
	counts     Counts
	expiry     time.Time
}

// NewCircuitBreaker создаёт новый CircuitBreaker.
func NewCircuitBreaker[T any](s Settings) *CircuitBreaker[T] {
	s = s.withDefaults()
	cb := &CircuitBreaker[T]{
		settings: s,
		state:    StateClosed,
	}
	cb.startGeneration(time.Now())
	return cb
}

// Name возвращает имя breaker'а.
func (cb *CircuitBreaker[T]) Name() string {
	return cb.settings.Name
}

// State возвращает текущее состояние (с учётом истечения таймеров).
func (cb *CircuitBreaker[T]) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maybeAdvance(time.Now())
	return cb.state
}

// Counts возвращает счётчики текущего окна.
func (cb *CircuitBreaker[T]) Counts() Counts {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.maybeAdvance(time.Now())
	return cb.counts
}

// Execute выполняет fn через breaker. Возвращает ErrOpenState / ErrTooManyRequests,
// если вызов отклонён.
func (cb *CircuitBreaker[T]) Execute(fn func() (T, error)) (T, error) {
	gen, err := cb.beforeRequest()
	if err != nil {
		var zero T
		return zero, err
	}
	result, fnErr := fn()
	cb.afterRequest(gen, cb.settings.IsSuccessful(fnErr))
	return result, fnErr
}

func (cb *CircuitBreaker[T]) beforeRequest() (uint64, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.maybeAdvance(now)

	if cb.state == StateOpen {
		return 0, ErrOpenState
	}
	if cb.state == StateHalfOpen && cb.counts.Requests >= cb.settings.MaxRequests {
		return 0, ErrTooManyRequests
	}
	cb.counts.onRequest()
	return cb.generation, nil
}

func (cb *CircuitBreaker[T]) afterRequest(gen uint64, success bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.maybeAdvance(now)

	// Ответ от предыдущего поколения — игнорируем.
	if gen != cb.generation {
		return
	}

	if success {
		cb.counts.onSuccess()
	} else {
		cb.counts.onFailure()
	}

	switch cb.state {
	case StateClosed:
		if cb.settings.ReadyToTrip(cb.counts) {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		if cb.counts.ConsecutiveSuccesses >= cb.settings.MaxRequests {
			cb.setState(StateClosed, now)
		}
	}
}

// maybeAdvance лениво применяет:
//   - в closed: сброс окна по истечении Interval;
//   - в open:    переход open -> half-open по истечении Timeout.
//
// Вызывается только под уже захваченным мьютексом.
func (cb *CircuitBreaker[T]) maybeAdvance(now time.Time) {
	switch cb.state {
	case StateClosed:
		if !cb.expiry.IsZero() && now.After(cb.expiry) {
			// Сброс счётчиков без смены поколения: in-flight запросы
			// по-прежнему попадут в новое окно (как в исходной версии).
			cb.counts = Counts{}
			cb.expiry = now.Add(cb.settings.Interval)
		}
	case StateOpen:
		if now.After(cb.expiry) {
			cb.setState(StateHalfOpen, now)
		}
	}
}

func (cb *CircuitBreaker[T]) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}
	prev := cb.state
	cb.state = state
	cb.startGeneration(now)
	if cb.settings.OnStateChange != nil {
		cb.settings.OnStateChange(cb.settings.Name, prev, state)
	}
}

// startGeneration сбрасывает счётчики и выставляет expiry под новое состояние.
func (cb *CircuitBreaker[T]) startGeneration(now time.Time) {
	cb.generation++
	cb.counts = Counts{}
	cb.expiry = time.Time{}
	switch cb.state {
	case StateClosed:
		cb.expiry = now.Add(cb.settings.Interval)
	case StateOpen:
		cb.expiry = now.Add(cb.settings.Timeout)
		// StateHalfOpen: expiry остаётся zero — счётчики half-open
		// накапливаются без автоматического сброса.
	}
}
