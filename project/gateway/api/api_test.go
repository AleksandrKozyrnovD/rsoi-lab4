package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"

	"gateway/models"
)

type meLoyaltyStub struct {
	LoyaltyService
	response *models.LoyaltyInfoResponse
	err      error
}

func (s meLoyaltyStub) GetLoyalty(string) (*models.LoyaltyInfoResponse, error) {
	return s.response, s.err
}

type meReservationsStub struct {
	ReservationService
	reservations []models.ReservationResponse
}

func (s meReservationsStub) ListReservations(string) ([]models.ReservationResponse, error) {
	return s.reservations, nil
}

func TestHandleGetMeLoyalty(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reservations := []models.ReservationResponse{{ReservationUid: "reservation-1"}}
	tests := []struct {
		name        string
		loyalty     *models.LoyaltyInfoResponse
		err         error
		wantStatus  int
		wantLoyalty map[string]any
	}{
		{
			name: "available",
			loyalty: &models.LoyaltyInfoResponse{
				Status: models.StatusGold, Discount: 10, ReservationCount: 25,
			},
			wantStatus: http.StatusOK,
			wantLoyalty: map[string]any{
				"status": "GOLD", "discount": float64(10), "reservationCount": float64(25),
			},
		},
		{
			name: "unavailable",
			err:  ErrServiceUnavailable, wantStatus: http.StatusOK,
			wantLoyalty: map[string]any{},
		},
		{
			name: "wrapped unavailable error",
			err:  fmt.Errorf("loyalty: %w", ErrServiceUnavailable), wantStatus: http.StatusOK,
			wantLoyalty: map[string]any{},
		},
		{
			name: "unexpected error",
			err:  errors.New("unexpected loyalty error"), wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewV1(meLoyaltyStub{response: tt.loyalty, err: tt.err},
				meReservationsStub{reservations: reservations}, nil)
			router := gin.New()
			router.GET("/api/v1/me", handler.HandleGetMe)
			request := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
			request.Header.Set("X-User-Name", "Test Max")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", response.Code, tt.wantStatus, response.Body)
			}
			if tt.wantStatus != http.StatusOK {
				return
			}
			var body struct {
				Reservations []models.ReservationResponse `json:"reservations"`
				Loyalty      map[string]any               `json:"loyalty"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(body.Reservations, reservations) {
				t.Errorf("reservations = %+v, want %+v", body.Reservations, reservations)
			}
			if !reflect.DeepEqual(body.Loyalty, tt.wantLoyalty) {
				t.Errorf("loyalty = %#v, want %#v; body: %s", body.Loyalty, tt.wantLoyalty, response.Body)
			}
		})
	}
}
