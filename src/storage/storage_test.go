package storage

import (
	. "final-project/src/common"
	"sync"
	"testing"
	"time"
)

func aw(v AppleCultivar, qty int, bb time.Time) ApplesWithBestBefore {
	return ApplesWithBestBefore{Apples: Apples{Variety: v, Quantity: qty}, BestBefore: bb}
}

func TestPutAndReserveSimple(t *testing.T) {
	s := New()
	tm := time.Now().Add(48 * time.Hour)
	s.Put(aw(Fuji, 10, tm))
	s.Put(aw(Fuji, 5, tm.Add(time.Hour)))

	got, ok := s.Reserve(Fuji, 12, time.Now())
	if !ok {
		t.Fatal("expected reserve ok")
	}
	total := 0
	for _, it := range got {
		total += it.Quantity
	}
	if total != 12 {
		t.Fatalf("expected 12 reserved, got %d", total)
	}
	if s.TotalQuantity() != 3 {
		t.Fatalf("expected 3 remaining, got %d", s.TotalQuantity())
	}
}

func TestReserveInsufficient(t *testing.T) {
	s := New()
	tm := time.Now().Add(48 * time.Hour)
	s.Put(aw(Fuji, 3, tm))
	_, ok := s.Reserve(Fuji, 10, time.Now())
	if ok {
		t.Fatal("expected reserve failure")
	}
	if s.TotalQuantity() != 3 {
		t.Fatal("storage must be unchanged on failure")
	}
}

func TestReserveRespectsMinBestBefore(t *testing.T) {
	s := New()
	now := time.Now()
	s.Put(aw(Fuji, 5, now.Add(time.Hour)))
	s.Put(aw(Fuji, 5, now.Add(72*time.Hour)))

	// Просим яблоки с минимальным сроком через 24ч → должны взять только из второй партии.
	got, ok := s.Reserve(Fuji, 5, now.Add(24*time.Hour))
	if !ok {
		t.Fatal("expected reserve ok")
	}
	for _, it := range got {
		if !it.BestBefore.After(now.Add(24 * time.Hour)) {
			t.Fatalf("got too-fresh-cutoff violation: %v", it.BestBefore)
		}
	}
	// Не должно хватить, если попросить больше пяти.
	if _, ok := s.Reserve(Fuji, 1, now.Add(24*time.Hour)); ok {
		t.Fatal("expected failure: nothing fresh enough left")
	}
}

func TestReserveAnyMix(t *testing.T) {
	s := New()
	tm := time.Now().Add(48 * time.Hour)
	s.Put(aw(Fuji, 4, tm))
	s.Put(aw(Gala, 4, tm))
	got, ok := s.ReserveAny(7, time.Now())
	if !ok {
		t.Fatal("expected reserve any ok")
	}
	total := 0
	for _, it := range got {
		total += it.Quantity
	}
	if total != 7 {
		t.Fatalf("expected 7, got %d", total)
	}
	if s.TotalQuantity() != 1 {
		t.Fatalf("expected 1 remaining, got %d", s.TotalQuantity())
	}
}

func TestCollectExpired(t *testing.T) {
	s := New()
	now := time.Now()
	s.Put(aw(Fuji, 3, now.Add(-time.Hour)))   // протухло
	s.Put(aw(Gala, 2, now.Add(-time.Minute))) // протухло
	s.Put(aw(Fuji, 5, now.Add(time.Hour)))    // свежее

	expired := s.CollectExpired(now)
	totalExp := 0
	for _, it := range expired {
		totalExp += it.Quantity
	}
	if totalExp != 5 {
		t.Fatalf("expected 5 expired, got %d", totalExp)
	}
	if s.TotalQuantity() != 5 {
		t.Fatalf("expected 5 remaining fresh, got %d", s.TotalQuantity())
	}
}

func TestReturnDoesNotReadmitExpired(t *testing.T) {
	s := New()
	now := time.Now()
	items := []ApplesWithBestBefore{
		aw(Fuji, 3, now.Add(-time.Hour)),
		aw(Gala, 2, now.Add(time.Hour)),
	}
	s.Return(items, now)
	if s.TotalQuantity() != 2 {
		t.Fatalf("expected 2 (only fresh Gala), got %d", s.TotalQuantity())
	}
}

// TestConcurrentPutReserve проверяет, что параллельные Put/Reserve не теряют яблок.
func TestConcurrentPutReserve(t *testing.T) {
	s := New()
	tm := time.Now().Add(48 * time.Hour)

	var wg sync.WaitGroup
	// 50 поставщиков по 100 яблок Fuji
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				s.Put(aw(Fuji, 10, tm))
			}
		}()
	}
	wg.Wait()
	if s.TotalQuantity() != 50*10*10 {
		t.Fatalf("expected 5000, got %d", s.TotalQuantity())
	}

	// 50 покупателей резервируют по 100
	var reserved int
	var mu sync.Mutex
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, ok := s.Reserve(Fuji, 100, time.Now())
			if !ok {
				return
			}
			total := 0
			for _, it := range got {
				total += it.Quantity
			}
			mu.Lock()
			reserved += total
			mu.Unlock()
		}()
	}
	wg.Wait()
	if s.TotalQuantity()+reserved != 5000 {
		t.Fatalf("apples lost: stored=%d, reserved=%d", s.TotalQuantity(), reserved)
	}
}
