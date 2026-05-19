package src

import (
	"context"
	"errors"
	"final-project/src/api"
	. "final-project/src/common"
	"final-project/src/external_services"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type instantDelivery struct{ delivered atomic.Int64 }

func (d *instantDelivery) DoDelivery(ctx context.Context, req external_services.DeliveryRequest) error {
	d.delivered.Add(1)
	return nil
}

type slowDelivery struct {
	dur       time.Duration
	delivered atomic.Int64
	cancelled atomic.Int64
}

func (d *slowDelivery) DoDelivery(ctx context.Context, req external_services.DeliveryRequest) error {
	select {
	case <-time.After(d.dur):
		d.delivered.Add(1)
		return nil
	case <-ctx.Done():
		d.cancelled.Add(1)
		return ctx.Err()
	}
}

type failingDelivery struct{ failed atomic.Int64 }

func (d *failingDelivery) DoDelivery(ctx context.Context, req external_services.DeliveryRequest) error {
	select {
	case <-time.After(5 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	d.failed.Add(1)
	return errors.New("boom")
}

type chaosDelivery struct {
	delivered atomic.Int64
	failed    atomic.Int64
	cancelled atomic.Int64
}

func (d *chaosDelivery) DoDelivery(ctx context.Context, req external_services.DeliveryRequest) error {
	dur := time.Duration(rand.Intn(5)) * time.Millisecond
	select {
	case <-time.After(dur):
	case <-ctx.Done():
		d.cancelled.Add(1)
		return ctx.Err()
	}
	if rand.Intn(10) == 0 {
		d.failed.Add(1)
		return errors.New("delivery error")
	}
	d.delivered.Add(1)
	return nil
}

func newSvc(t *testing.T, ds external_services.DeliveryService, cs external_services.ComposterService, parking int) *Service {
	t.Helper()
	s := NewService(ServiceConfig{
		SuppliersParkingSize: parking,
		DeliveryService:      ds,
		ComposterService:     cs,
		ComposterInterval:    20 * time.Millisecond,
	})
	t.Cleanup(s.Stop)
	return s
}

func unload(t *testing.T, s *Service, supplierID int64, items ...ApplesWithBestBefore) {
	t.Helper()
	ch, err := s.BeginUnloading(supplierID)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range items {
		ch <- it
	}
	if err := s.FinishUnloading(supplierID); err != nil {
		t.Fatal(err)
	}
}

func TestSimpleOrderHappyPath(t *testing.T) {
	dd := &instantDelivery{}
	s := newSvc(t, dd, nil, 2)
	tm := time.Now().Add(24 * time.Hour)
	unload(t, s, 1, ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 5}, BestBefore: tm})

	id, err := s.PlaceOrderSimple(api.SimpleOrder{
		CustomerID: 1, Request: Apples{Variety: Fuji, Quantity: 3}, MinAllowedBestBefore: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected non-zero id")
	}
	// дождёмся доставки
	s.orders.Stop()
	if dd.delivered.Load() != 1 {
		t.Fatalf("expected 1 delivery, got %d", dd.delivered.Load())
	}
	if s.TotalStored() != 2 {
		t.Fatalf("expected 2 left, got %d", s.TotalStored())
	}
}

func TestSimpleOrderInsufficient(t *testing.T) {
	s := newSvc(t, &instantDelivery{}, nil, 1)
	_, err := s.PlaceOrderSimple(api.SimpleOrder{
		CustomerID: 1, Request: Apples{Variety: Fuji, Quantity: 1}, MinAllowedBestBefore: time.Now(),
	})
	if !errors.Is(err, ErrInsufficientQuantity) {
		t.Fatalf("expected ErrInsufficientQuantity, got %v", err)
	}
}

func TestMultiOrder(t *testing.T) {
	dd := &instantDelivery{}
	s := newSvc(t, dd, nil, 1)
	tm := time.Now().Add(24 * time.Hour)
	unload(t, s, 1,
		ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 3}, BestBefore: tm},
		ApplesWithBestBefore{Apples: Apples{Variety: Gala, Quantity: 4}, BestBefore: tm},
	)
	id, err := s.PlaceOrderMulti(api.MultiOrder{
		CustomerID: 7,
		Request: []Apples{
			{Variety: Fuji, Quantity: 2},
			{Variety: Gala, Quantity: 3},
		},
		MinAllowedBestBefore: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("zero id")
	}
	s.orders.Stop()
	if s.TotalStored() != 2 {
		t.Fatalf("expected 2 left, got %d", s.TotalStored())
	}
}

func TestMultiOrderRollbackOnInsufficient(t *testing.T) {
	s := newSvc(t, &instantDelivery{}, nil, 1)
	tm := time.Now().Add(24 * time.Hour)
	unload(t, s, 1,
		ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 3}, BestBefore: tm},
	)
	_, err := s.PlaceOrderMulti(api.MultiOrder{
		CustomerID: 1,
		Request: []Apples{
			{Variety: Fuji, Quantity: 2},
			{Variety: Gala, Quantity: 3}, // нет такого сорта
		},
		MinAllowedBestBefore: time.Now(),
	})
	if !errors.Is(err, ErrInsufficientQuantity) {
		t.Fatalf("expected insufficient, got %v", err)
	}
	if s.TotalStored() != 3 {
		t.Fatalf("expected rollback to 3, got %d", s.TotalStored())
	}
}

func TestAnyOrder(t *testing.T) {
	dd := &instantDelivery{}
	s := newSvc(t, dd, nil, 1)
	tm := time.Now().Add(24 * time.Hour)
	unload(t, s, 1,
		ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 3}, BestBefore: tm},
		ApplesWithBestBefore{Apples: Apples{Variety: Gala, Quantity: 4}, BestBefore: tm},
	)
	_, err := s.PlaceOrderAny(api.AnyApplesOrder{CustomerID: 1, Quantity: 6, MinAllowedBestBefore: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	s.orders.Stop()
	if s.TotalStored() != 1 {
		t.Fatalf("expected 1 left, got %d", s.TotalStored())
	}
}

func TestCancelReturnsApples(t *testing.T) {
	dd := &slowDelivery{dur: 500 * time.Millisecond}
	s := newSvc(t, dd, nil, 1)
	tm := time.Now().Add(24 * time.Hour)
	unload(t, s, 1, ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 5}, BestBefore: tm})

	id, err := s.PlaceOrderSimple(api.SimpleOrder{CustomerID: 1, Request: Apples{Variety: Fuji, Quantity: 3}, MinAllowedBestBefore: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel(1, id); err != nil {
		t.Fatal(err)
	}
	s.orders.Stop()
	if dd.cancelled.Load() != 1 {
		t.Fatalf("expected 1 cancelled delivery, got %d", dd.cancelled.Load())
	}
	if s.TotalStored() != 5 {
		t.Fatalf("expected apples returned to storage (5), got %d", s.TotalStored())
	}
}

func TestCancelWrongCustomer(t *testing.T) {
	s := newSvc(t, &slowDelivery{dur: 200 * time.Millisecond}, nil, 1)
	tm := time.Now().Add(24 * time.Hour)
	unload(t, s, 1, ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 5}, BestBefore: tm})
	id, err := s.PlaceOrderSimple(api.SimpleOrder{CustomerID: 1, Request: Apples{Variety: Fuji, Quantity: 3}, MinAllowedBestBefore: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel(999, id); err == nil {
		t.Fatal("expected error for wrong customer")
	}
	_ = s.Cancel(1, id)
}

func TestDeliveryFailureReturnsApples(t *testing.T) {
	fd := &failingDelivery{}
	s := newSvc(t, fd, nil, 1)
	tm := time.Now().Add(24 * time.Hour)
	unload(t, s, 1, ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 5}, BestBefore: tm})
	_, err := s.PlaceOrderSimple(api.SimpleOrder{CustomerID: 1, Request: Apples{Variety: Fuji, Quantity: 3}, MinAllowedBestBefore: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	s.orders.Stop()
	if fd.failed.Load() != 1 {
		t.Fatalf("expected 1 failure, got %d", fd.failed.Load())
	}
	if s.TotalStored() != 5 {
		t.Fatalf("expected apples returned, got %d", s.TotalStored())
	}
}

func TestComposterDisposesExpired(t *testing.T) {
	dd := &instantDelivery{}
	cs := external_services.NewComposter()
	s := newSvc(t, dd, cs, 1)
	// Кладём яблоки с уже истекшим сроком.
	past := time.Now().Add(-time.Hour)
	unload(t, s, 1, ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 3}, BestBefore: past})
	// Подождём, чтобы воркер сработал минимум один раз.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.TotalStored() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s.TotalStored() != 0 {
		t.Fatalf("expected composter to consume expired, still %d on storage", s.TotalStored())
	}
}

// --- стресс-тест ---

// TestStress эмулирует много поставщиков и покупателей одновременно.
// Покрытие: BeginUnloading/FinishUnloading, PlaceOrderSimple/Multi/Any, Cancel, фоновый компостер.
// Запускайте `go test -race`.
func TestStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	rand.Seed(1)
	dd := &chaosDelivery{}
	cs := external_services.NewComposter()
	s := newSvc(t, dd, cs, 4)

	const (
		suppliers = 20
		buyers    = 30
		runFor    = 700 * time.Millisecond
	)
	varieties := []AppleCultivar{Fuji, Gala, GrannySmith, GoldenDelicious, RedDelicious}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Поставщики
	for i := int64(1); i <= suppliers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ch, err := s.BeginUnloading(i)
				if err != nil {
					t.Errorf("begin: %v", err)
					return
				}
				n := rand.Intn(3) + 1
				for k := 0; k < n; k++ {
					v := varieties[rand.Intn(len(varieties))]
					qty := rand.Intn(4) + 1
					var bb time.Time
					// иногда кладём с почти-протухшим сроком, чтобы компостер тоже работал
					if rand.Intn(5) == 0 {
						bb = time.Now().Add(time.Duration(rand.Intn(30)) * time.Millisecond)
					} else {
						bb = time.Now().Add(2 * time.Hour)
					}
					ch <- ApplesWithBestBefore{Apples: Apples{Variety: v, Quantity: qty}, BestBefore: bb}
				}
				if err := s.FinishUnloading(i); err != nil {
					t.Errorf("finish: %v", err)
					return
				}
			}
		}()
	}

	// Покупатели
	var (
		orderIDs sync.Map // map[int64]int customerID
	)
	for i := 1; i <= buyers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				kind := rand.Intn(3)
				v := varieties[rand.Intn(len(varieties))]
				min := time.Now().Add(-10 * time.Millisecond)
				switch kind {
				case 0:
					id, err := s.PlaceOrderSimple(api.SimpleOrder{
						CustomerID: i, Request: Apples{Variety: v, Quantity: rand.Intn(3) + 1}, MinAllowedBestBefore: min,
					})
					if err == nil {
						orderIDs.Store(id, i)
					}
				case 1:
					id, err := s.PlaceOrderMulti(api.MultiOrder{
						CustomerID: i,
						Request: []Apples{
							{Variety: varieties[0], Quantity: rand.Intn(2) + 1},
							{Variety: varieties[1], Quantity: rand.Intn(2) + 1},
						},
						MinAllowedBestBefore: min,
					})
					if err == nil {
						orderIDs.Store(id, i)
					}
				case 2:
					id, err := s.PlaceOrderAny(api.AnyApplesOrder{
						CustomerID: i, Quantity: rand.Intn(4) + 1, MinAllowedBestBefore: min,
					})
					if err == nil {
						orderIDs.Store(id, i)
					}
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			orderIDs.Range(func(k, v any) bool {
				if rand.Intn(2) == 0 {
					_ = s.Cancel(v.(int), k.(int64))
					orderIDs.Delete(k)
				}
				return true
			})
			time.Sleep(2 * time.Millisecond)
		}
	}()

	time.Sleep(runFor)
	close(stop)
	wg.Wait()

	// Дожидаемся завершения всех доставок.
	s.orders.Stop()

	t.Logf("stress finished: delivered=%d failed=%d cancelled=%d stored=%d",
		dd.delivered.Load(), dd.failed.Load(), dd.cancelled.Load(), s.TotalStored())
}
