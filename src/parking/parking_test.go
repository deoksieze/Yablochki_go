package parking

import (
	. "final-project/src/common"
	"final-project/src/storage"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleSupplier(t *testing.T) {
	st := storage.New()
	p := New(2, st)
	defer p.Stop()

	ch, err := p.BeginUnloading(1)
	if err != nil {
		t.Fatal(err)
	}
	ch <- ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 5}, BestBefore: time.Now().Add(time.Hour)}
	if err := p.FinishUnloading(1); err != nil {
		t.Fatal(err)
	}
	if st.TotalQuantity() != 5 {
		t.Fatalf("expected 5 stored, got %d", st.TotalQuantity())
	}
}

func TestParkingBlocksWhenFull(t *testing.T) {
	st := storage.New()
	p := New(1, st)
	defer p.Stop()

	if _, err := p.BeginUnloading(1); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		_, err := p.BeginUnloading(2)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		close(done)
	}()
	<-started
	select {
	case <-done:
		t.Fatal("second supplier should be blocked while parking is full")
	case <-time.After(100 * time.Millisecond):
	}

	// Освобождаем место — второй должен пройти.
	if err := p.FinishUnloading(1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second supplier should have proceeded after parking freed")
	}
	if err := p.FinishUnloading(2); err != nil {
		t.Fatal(err)
	}
}

func TestFinishWithoutBegin(t *testing.T) {
	st := storage.New()
	p := New(1, st)
	defer p.Stop()
	if err := p.FinishUnloading(42); err == nil {
		t.Fatal("expected error on FinishUnloading without BeginUnloading")
	}
}

func TestDuplicateBegin(t *testing.T) {
	st := storage.New()
	p := New(2, st)
	defer p.Stop()
	if _, err := p.BeginUnloading(1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.BeginUnloading(1); err == nil {
		t.Fatal("expected duplicate begin error")
	}
	_ = p.FinishUnloading(1)
}

func TestConcurrentSuppliers(t *testing.T) {
	st := storage.New()
	const parkingSize = 4
	p := New(parkingSize, st)
	defer p.Stop()

	var maxOccupied atomic.Int32
	var occupied atomic.Int32
	var wg sync.WaitGroup
	for i := int64(1); i <= 50; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, err := p.BeginUnloading(i)
			if err != nil {
				t.Errorf("begin: %v", err)
				return
			}
			cur := occupied.Add(1)
			if cur > int32(parkingSize) {
				t.Errorf("parking overflow: %d", cur)
			}
			// Зафиксируем максимум
			for {
				prev := maxOccupied.Load()
				if cur <= prev || maxOccupied.CompareAndSwap(prev, cur) {
					break
				}
			}
			for k := 0; k < 5; k++ {
				ch <- ApplesWithBestBefore{Apples: Apples{Variety: Fuji, Quantity: 1}, BestBefore: time.Now().Add(time.Hour)}
			}
			occupied.Add(-1)
			if err := p.FinishUnloading(i); err != nil {
				t.Errorf("finish: %v", err)
			}
		}()
	}
	wg.Wait()
	if maxOccupied.Load() > int32(parkingSize) {
		t.Fatalf("parking overflow: %d", maxOccupied.Load())
	}
	if st.TotalQuantity() != 50*5 {
		t.Fatalf("expected %d apples, got %d", 50*5, st.TotalQuantity())
	}
}
