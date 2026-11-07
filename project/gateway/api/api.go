package api

import (
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"gateway/models"
)

// ============================================================
// Интерфейсы действий микросервисов
// ============================================================

type LoyaltyService interface {
	GetLoyalty(username string) (*models.LoyaltyInfoResponse, error)
	CreateLoyalty(username string) (*models.LoyaltyInfoResponse, error)
	IncrementReservations(username string) error
	DecrementReservations(username string) error
}

type ReservationService interface {
	ListHotels(page, size int) (*models.PaginationResponse, error)
	ListReservations(username string) ([]models.ReservationResponse, error)
	GetReservation(username, reservationUid string) (*models.ReservationResponse, error)
	CreateReservation(username string, req models.CreateReservationRequest) (*models.CreateReservationResponse, error)
	CancelReservation(username, reservationUid string) error
	UncancelReservation(username, reservationUid string) error
}

type PaymentService interface {
	Pay(price int) (*models.PaymentResponse, error)
	Reverse(paymentUid string) error
}

type V1 struct {
	loyalty      LoyaltyService
	reservations ReservationService
	payments     PaymentService

	mu                sync.Mutex
	pendingDecrements map[string]int // username -> сколько декрементов не применилось
}

func NewV1(loyalty LoyaltyService, reservations ReservationService, payments PaymentService) *V1 {
	return &V1{
		loyalty:           loyalty,
		reservations:      reservations,
		payments:          payments,
		pendingDecrements: make(map[string]int),
	}
}

// ============================================================
// helpers
// ============================================================

// getUserName достаёт X-User-Name. Если заголовка нет — пишет 400 и false.
func getUserName(c *gin.Context) (string, bool) {
	username := c.GetHeader("X-User-Name")
	if username == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Message: "X-User-Name header is required"})
		return "", false
	}
	return username, true
}

// defaultLoyalty returns a fallback loyalty object when the loyalty service
// is unavailable (non-critical data source).
func defaultLoyalty() *models.LoyaltyInfoResponse {
	return &models.LoyaltyInfoResponse{
		Status:           models.StatusBronze,
		Discount:         5,
		ReservationCount: 0,
	}
}

// getLoyalty тянет лояльность; если сервис отдаёт ErrNotFound — создаёт запись.
func (a *V1) getLoyalty(username string) (*models.LoyaltyInfoResponse, error) {
	a.flushPendingDecrements(username)

	loyalty, err := a.loyalty.GetLoyalty(username)
	if err == nil {
		return loyalty, nil
	}
	if errors.Is(err, ErrNotFound) {
		return a.loyalty.CreateLoyalty(username)
	}
	return nil, err
}

// findHotel ищет отель по uid среди всех отелей reservation-service.
func (a *V1) findHotel(hotelUid string) (*models.HotelResponse, error) {
	page, err := a.reservations.ListHotels(0, 100)
	if err != nil {
		return nil, err
	}
	for i := range page.Items {
		if page.Items[i].HotelUid == hotelUid {
			return &page.Items[i], nil
		}
	}
	return nil, nil
}

// nightsBetween считает количество ночей между двумя датами YYYY-MM-DD.
func nightsBetween(start, end string) (int, error) {
	s, err := time.Parse("2006-01-02", start)
	if err != nil {
		return 0, err
	}
	e, err := time.Parse("2006-01-02", end)
	if err != nil {
		return 0, err
	}
	return int(e.Sub(s).Hours() / 24), nil
}

// writeValidationError — сокращение для частого ответа.
func writeValidationError(c *gin.Context, errs ...models.ErrorDescription) {
	c.JSON(http.StatusBadRequest, models.ValidationErrorResponse{
		Message: "Invalid data",
		Errors:  errs,
	})
}

func (a *V1) queueDecrement(username string) {
	a.mu.Lock()
	a.pendingDecrements[username]++
	a.mu.Unlock()
}

// flushPendingDecrements пытается применить накопленные декременты.
// Best-effort: при ошибке оставляет остаток на следующий раз.
func (a *V1) flushPendingDecrements(username string) {
	a.mu.Lock()
	n := a.pendingDecrements[username]
	a.mu.Unlock()
	if n == 0 {
		return
	}

	applied := 0
	for i := 0; i < n; i++ {
		if err := a.loyalty.DecrementReservations(username); err != nil {
			break
		}
		applied++
	}
	if applied == 0 {
		return
	}

	a.mu.Lock()
	a.pendingDecrements[username] -= applied
	if a.pendingDecrements[username] <= 0 {
		delete(a.pendingDecrements, username)
	}
	a.mu.Unlock()
}

// ============================================================
// GET /api/v1/hotels?page=&size=
// ============================================================

func (a *V1) HandleGetHotels(c *gin.Context) {
	pageStr := c.DefaultQuery("page", "1")
	sizeStr := c.DefaultQuery("size", "10")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		writeValidationError(c, models.ErrorDescription{
			Field: "page", Error: "must be a positive integer",
		})
		return
	}
	size, err := strconv.Atoi(sizeStr)
	if err != nil || size < 1 || size > 100 {
		writeValidationError(c, models.ErrorDescription{
			Field: "size", Error: "must be between 1 and 100",
		})
		return
	}

	// reservation-service использует 0-индексную пагинацию
	resp, err := a.reservations.ListHotels(page-1, size)
	if err != nil {
		if errors.Is(err, ErrServiceUnavailable) {
			// Fallback: empty list — graceful degradation for a non-critical read.
			c.JSON(http.StatusOK, models.PaginationResponse{
				Page:          page,
				PageSize:      size,
				TotalElements: 0,
				Items:         []models.HotelResponse{},
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
		return
	}

	// возвращаем клиенту 1-индексный номер страницы, как он и просил
	resp.Page = page
	c.JSON(http.StatusOK, resp)
}

// ============================================================
// GET /api/v1/me
// ============================================================

func (a *V1) HandleGetMe(c *gin.Context) {
	username, ok := getUserName(c)
	if !ok {
		return
	}

	reservations, err := a.reservations.ListReservations(username)
	if err != nil {
		if errors.Is(err, ErrServiceUnavailable) {
			reservations = []models.ReservationResponse{}
		} else {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
			return
		}
	}

	var loyalty *models.LoyaltyInfoResponse
	loyalty, err = a.getLoyalty(username)
	if err != nil {
		if errors.Is(err, ErrServiceUnavailable) {
			loyalty = nil // пусто, но 200
		} else {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, models.MeResponse{
		Reservations: reservations,
		Loyalty:      loyalty,
	})
}

// ============================================================
// GET /api/v1/reservations
// ============================================================

func (a *V1) HandleGetReservations(c *gin.Context) {
	username, ok := getUserName(c)
	if !ok {
		return
	}

	reservations, err := a.reservations.ListReservations(username)
	if err != nil {
		if errors.Is(err, ErrServiceUnavailable) {
			reservations = []models.ReservationResponse{}
		} else {
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, reservations)
}

// ============================================================
// POST /api/v1/reservations
// ============================================================

func (a *V1) HandleCreateReservation(c *gin.Context) {
	username, ok := getUserName(c)
	if !ok {
		return
	}

	var req struct {
		HotelUid  string `json:"hotelUid"`
		StartDate string `json:"startDate"`
		EndDate   string `json:"endDate"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		writeValidationError(c, models.ErrorDescription{Field: "body", Error: err.Error()})
		return
	}

	// валидация входных полей
	errs := []models.ErrorDescription{}
	if req.HotelUid == "" {
		errs = append(errs, models.ErrorDescription{Field: "hotelUid", Error: "hotelUid is required"})
	}
	if req.StartDate == "" {
		errs = append(errs, models.ErrorDescription{Field: "startDate", Error: "startDate is required"})
	}
	if req.EndDate == "" {
		errs = append(errs, models.ErrorDescription{Field: "endDate", Error: "endDate is required"})
	}
	if len(errs) > 0 {
		writeValidationError(c, errs...)
		return
	}

	// 1) лояльность — non-critical source → fallback to default Bronze
	loyalty, err := a.getLoyalty(username)
	if err != nil {
		if errors.Is(err, ErrServiceUnavailable) {
			c.JSON(http.StatusServiceUnavailable,
				models.ErrorResponse{Message: "Loyalty Service unavailable"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
		return
	}
	// 2) отель — critical source
	hotel, err := a.findHotel(req.HotelUid)
	if err != nil {
		if errors.Is(err, ErrServiceUnavailable) {
			c.JSON(http.StatusServiceUnavailable, models.ErrorResponse{Message: "hotel service temporarily unavailable"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
		return
	}
	if hotel == nil {
		writeValidationError(c, models.ErrorDescription{Field: "hotelUid", Error: "hotel not found"})
		return
	}

	// 3) цена
	nights, err := nightsBetween(req.StartDate, req.EndDate)
	if err != nil {
		writeValidationError(c, models.ErrorDescription{Field: "startDate", Error: "invalid date format, expected YYYY-MM-DD"})
		return
	}
	if nights <= 0 {
		writeValidationError(c, models.ErrorDescription{Field: "endDate", Error: "endDate must be after startDate"})
		return
	}
	totalPrice := hotel.Price * nights
	discountedPrice := totalPrice * (100 - loyalty.Discount) / 100

	// 4) платёж — critical source
	payment, err := a.payments.Pay(discountedPrice)
	if err != nil {
		if errors.Is(err, ErrServiceUnavailable) {
			c.JSON(http.StatusServiceUnavailable, models.ErrorResponse{Message: "payment service temporarily unavailable"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
		return
	}

	// 5) бронь — critical source; при ошибке откатываем платёж
	createReq := models.CreateReservationRequest{
		HotelUid:      req.HotelUid,
		StartDate:     req.StartDate,
		EndDate:       req.EndDate,
		Discount:      loyalty.Discount,
		Price:         discountedPrice,
		Status:        models.ReservationStatusPaid,
		PaymentStatus: payment.Status,
	}
	created, err := a.reservations.CreateReservation(username, createReq)
	if err != nil {
		// rollback: reverse the payment
		_ = a.payments.Reverse(payment.PaymentUID)
		if errors.Is(err, ErrServiceUnavailable) {
			c.JSON(http.StatusServiceUnavailable, models.ErrorResponse{Message: "reservation service temporarily unavailable"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
		return
	}

	// 6) инкремент лояльности — critical for the compound operation;
	//    при ошибке откатываем бронь и платёж.
	if err := a.loyalty.IncrementReservations(username); err != nil {
		_ = a.reservations.CancelReservation(username, created.ReservationUid)
		_ = a.payments.Reverse(payment.PaymentUID)
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: "failed to update loyalty, operation rolled back"})
		return
	}

	c.JSON(http.StatusOK, created)
}

// ============================================================
// GET /api/v1/reservations/{reservationUid}
// ============================================================

func (a *V1) HandleGetReservationByUid(c *gin.Context) {
	username, ok := getUserName(c)
	if !ok {
		return
	}
	reservationUid := c.Param("reservationUid")
	if reservationUid == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Message: "reservationUid is required"})
		return
	}

	reservation, err := a.reservations.GetReservation(username, reservationUid)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			c.JSON(http.StatusNotFound, models.ErrorResponse{Message: "Reservation not found"})
			return
		}
		if errors.Is(err, ErrServiceUnavailable) {
			// Fallback: return a minimal object with the UID so the client
			// can still correlate with other systems.
			c.JSON(http.StatusOK, models.ReservationResponse{
				ReservationUid: reservationUid,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
		return
	}

	c.JSON(http.StatusOK, reservation)
}

// ============================================================
// DELETE /api/v1/reservations/{reservationUid}
// ============================================================

func (a *V1) HandleCancelReservation(c *gin.Context) {
	username, ok := getUserName(c)
	if !ok {
		return
	}
	reservationUid := c.Param("reservationUid")
	if reservationUid == "" {
		c.JSON(http.StatusBadRequest, models.ErrorResponse{Message: "reservationUid is required"})
		return
	}

	// 1) отменить бронь — critical source
	err := a.reservations.CancelReservation(username, reservationUid)
	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			c.JSON(http.StatusNotFound, models.ErrorResponse{Message: "Reservation not found"})
		case errors.Is(err, ErrAlreadyCanceled):
			c.JSON(http.StatusBadRequest, models.ErrorResponse{Message: "Reservation already canceled"})
		case errors.Is(err, ErrServiceUnavailable):
			c.JSON(http.StatusServiceUnavailable, models.ErrorResponse{Message: "reservation service temporarily unavailable"})
		default:
			c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
		}
		return
	}

	// 2) декремент лояльности — critical for the compound operation;
	//    при ошибке откатываем отмену брони.
	if err := a.loyalty.DecrementReservations(username); err != nil {
		// loyalty недоступен → запоминаем, применим при следующем успешном контакте
		a.queueDecrement(username)
	}

	c.Status(http.StatusNoContent)
}

// ============================================================
// GET /api/v1/loyalty
// ============================================================
func (a *V1) HandleGetLoyalty(c *gin.Context) {
	username, ok := getUserName(c)
	if !ok {
		return
	}

	loyalty, err := a.getLoyalty(username)
	if err != nil {
		if errors.Is(err, ErrServiceUnavailable) {
			c.JSON(http.StatusServiceUnavailable,
				models.ErrorResponse{Message: "Loyalty Service unavailable"})
			return
		}
		c.JSON(http.StatusInternalServerError, models.ErrorResponse{Message: err.Error()})
		return
	}

	c.JSON(http.StatusOK, loyalty)
}
