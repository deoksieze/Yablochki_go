//   - Глобальный счётчик ID — atomic.Int64.
//   - Реестр активных заказов — map под RWMutex, в котором хранится:
//     customerID, зарезервированные партии, cancel-функция контекста доставки,
//     статус (active / cancelled / delivered / failed).

package orders

import (
	"context"
	"errors"
	. "final-project/src/common"
	"final-project/src/external_services"
	"final-project/src/storage"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrOrderNotFound        = errors.New("order not found")
	ErrOrderAlreadyFinished = errors.New("order already finished")
	ErrDuplicateVariety     = errors.New("multi order contains duplicate variety")
)

type status int

const (
	statusActive status = iota
	statusCancelled
	statusDelivered
	statusFailed
)

type order struct {
	id         int64
	customerID int
	items      []ApplesWithBestBefore
	cancel     context.CancelFunc
	st         status
}

type Manager struct {
	storage         *storage.Storage
	deliveryService external_services.DeliveryService
	now             func() time.Time

	nextID atomic.Int64

	mu     sync.Mutex
	orders map[int64]*order
	wg     sync.WaitGroup
}

func New(st *storage.Storage, ds external_services.DeliveryService, now func() time.Time) *Manager {
	if now == nil {
		now = time.Now
	}
	return &Manager{
		storage:         st,
		deliveryService: ds,
		now:             now,
		orders:          make(map[int64]*order),
	}
}

// PlaceSimple резервирует яблоки одного сорта и запускает доставку.
func (m *Manager) PlaceSimple(customerID int, req Apples, minBestBefore time.Time) (int64, error) {
	if req.Quantity <= 0 {
		return 0, ErrInvalidRequest
	}
	items, ok := m.storage.Reserve(req.Variety, req.Quantity, minBestBefore)
	if !ok {
		return 0, ErrInsufficientQuantity
	}
	return m.registerAndDeliver(customerID, items), nil
}

// PlaceMulti резервирует яблоки нескольких сортов и запускает доставку.
// При недостатке хотя бы по одной позиции откатывает уже взятое.
func (m *Manager) PlaceMulti(customerID int, requests []Apples, minBestBefore time.Time) (int64, error) {
	if len(requests) == 0 {
		return 0, ErrInvalidRequest
	}
	// Проверим уникальность сортов и валидность количеств.
	seen := make(map[AppleCultivar]struct{}, len(requests))
	for _, r := range requests {
		if r.Quantity <= 0 {
			return 0, ErrInvalidRequest
		}
		if _, dup := seen[r.Variety]; dup {
			return 0, ErrDuplicateVariety
		}
		seen[r.Variety] = struct{}{}
	}

	var collected []ApplesWithBestBefore
	for _, r := range requests {
		got, ok := m.storage.Reserve(r.Variety, r.Quantity, minBestBefore)
		if !ok {
			// откатываем уже взятое
			m.storage.Return(collected, m.now())
			return 0, ErrInsufficientQuantity
		}
		collected = append(collected, got...)
	}
	return m.registerAndDeliver(customerID, collected), nil
}

// PlaceAny резервирует qty яблок любых сортов.
func (m *Manager) PlaceAny(customerID int, qty int, minBestBefore time.Time) (int64, error) {
	if qty <= 0 {
		return 0, ErrInvalidRequest
	}
	items, ok := m.storage.ReserveAny(qty, minBestBefore)
	if !ok {
		return 0, ErrInsufficientQuantity
	}
	return m.registerAndDeliver(customerID, items), nil
}

func (m *Manager) Cancel(customerID int, orderID int64) error {
	m.mu.Lock()
	o, ok := m.orders[orderID]
	if !ok {
		m.mu.Unlock()
		return ErrOrderNotFound
	}
	if o.customerID != customerID {
		m.mu.Unlock()
		return ErrOrderNotFound // не светим чужие заказы
	}
	if o.st != statusActive {
		m.mu.Unlock()
		return ErrOrderAlreadyFinished
	}
	o.st = statusCancelled
	cancel := o.cancel
	m.mu.Unlock()

	cancel()
	return nil
}

// registerAndDeliver регистрирует заказ и запускает горутину доставки.
func (m *Manager) registerAndDeliver(customerID int, items []ApplesWithBestBefore) int64 {
	id := m.nextID.Add(1)
	ctx, cancel := context.WithCancel(context.Background())

	o := &order{
		id:         id,
		customerID: customerID,
		items:      items,
		cancel:     cancel,
		st:         statusActive,
	}
	m.mu.Lock()
	m.orders[id] = o
	m.mu.Unlock()

	req := external_services.DeliveryRequest{
		CustomerID: customerID,
		OrderID:    id,
		Items:      items,
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		err := m.deliveryService.DoDelivery(ctx, req)
		m.mu.Lock()
		if o.st == statusActive {
			if err != nil {
				o.st = statusFailed
			} else {
				o.st = statusDelivered
			}
		}
		m.mu.Unlock()

		if err != nil {
			m.storage.Return(items, m.now())
		}
		cancel()
	}()

	return id
}

func (m *Manager) Stop() {
	m.wg.Wait()
}

func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, o := range m.orders {
		if o.st == statusActive {
			n++
		}
	}
	return n
}
