// Package src — фасад сервиса «Яблочки.Go».
//
// Service склеивает четыре подсистемы:
//
//   - storage.Storage           — потокобезопасный склад с per-variety мьютексами и B-tree (O(log n)).
//   - parking.Parking           — парковка с семафор-каналом и сессиями разгрузки.
//   - orders.Manager            — размещение заказов и доставка через DeliveryService.
//   - composter.Worker          — фоновая утилизация просроченных яблок.
//
// Сам Service не содержит общего состояния, требующего блокировок: всё делегировано подкомпонентам.
package src

import (
	"final-project/src/api"
	. "final-project/src/common"
	"final-project/src/composter"
	. "final-project/src/external_services"
	"final-project/src/orders"
	"final-project/src/parking"
	"final-project/src/storage"
	"time"
)

// ServiceConfig — параметры сервиса. Поля Now и ComposterInterval опциональны.
type ServiceConfig struct {
	SuppliersParkingSize int
	DeliveryService      DeliveryService
	ComposterService     ComposterService

	// ComposterInterval — частота проверки просрочки. 0 = значение по умолчанию.
	ComposterInterval time.Duration
	// Now — источник времени (для тестов). nil = time.Now.
	Now func() time.Time
}

// Service реализует SupplierApi и ConsumerApi.
type Service struct {
	storage   *storage.Storage
	parking   *parking.Parking
	orders    *orders.Manager
	composter *composter.Worker
}

var _ api.ConsumerApi = (*Service)(nil)
var _ api.SupplierApi = (*Service)(nil)

// NewService создаёт и запускает сервис.
func NewService(config ServiceConfig) *Service {
	now := config.Now
	if now == nil {
		now = time.Now
	}
	st := storage.New()
	pk := parking.New(config.SuppliersParkingSize, st)
	om := orders.New(st, config.DeliveryService, now)

	var cw *composter.Worker
	if config.ComposterService != nil {
		cw = composter.NewWorker(st, config.ComposterService, config.ComposterInterval, now)
		cw.Start()
	}

	return &Service{
		storage:   st,
		parking:   pk,
		orders:    om,
		composter: cw,
	}
}

// --- SupplierApi ---

func (s *Service) BeginUnloading(supplierID int64) (chan<- ApplesWithBestBefore, error) {
	return s.parking.BeginUnloading(supplierID)
}

func (s *Service) FinishUnloading(supplierID int64) error {
	return s.parking.FinishUnloading(supplierID)
}

// --- ConsumerApi ---

func (s *Service) PlaceOrderSimple(order api.SimpleOrder) (int64, error) {
	return s.orders.PlaceSimple(order.CustomerID, order.Request, order.MinAllowedBestBefore)
}

func (s *Service) PlaceOrderMulti(order api.MultiOrder) (int64, error) {
	return s.orders.PlaceMulti(order.CustomerID, order.Request, order.MinAllowedBestBefore)
}

func (s *Service) PlaceOrderAny(order api.AnyApplesOrder) (int64, error) {
	return s.orders.PlaceAny(order.CustomerID, order.Quantity, order.MinAllowedBestBefore)
}

func (s *Service) Cancel(customerID int, orderID int64) error {
	return s.orders.Cancel(customerID, orderID)
}

// Stop корректно останавливает сервис (для тестов и shutdown).
// Останавливает компостер, дожидается завершения текущих доставок, закрывает парковку.
func (s *Service) Stop() {
	if s.composter != nil {
		s.composter.Stop()
	}
	s.orders.Stop()
	s.parking.Stop()
}

// TotalStored — диагностический метод для тестов.
func (s *Service) TotalStored() int {
	return s.storage.TotalQuantity()
}
