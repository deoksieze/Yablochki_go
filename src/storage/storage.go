// Архитектура:
//   - Склад поделён на «корзины» (VarietyBucket) — по одной на сорт.
//   - Каждая корзина имеет собственный sync.Mutex; глобального мьютекса на склад нет.
//     Так разные сорта обрабатываются параллельно (mass concurrency).
//   - Внутри корзины партии хранятся в google/btree, ключ — BestBefore + monotonic id.
//     Это даёт O(log n) на вставку, удаление и поиск самой свежей/самой старой партии,
//     что важно для FEFO-резервирования и фоновой утилизации.
package storage

import (
	. "final-project/src/common"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
)

// Batch — одна партия яблок на складе.
// Уникальный id внутри корзины нужен, чтобы две партии с одинаковым BestBefore
// различались как ключи B-tree.
type Batch struct {
	ID         uint64
	Variety    AppleCultivar
	Quantity   int
	BestBefore time.Time
}

// Less упорядочивает партии по сроку годности (по возрастанию: первой утилизируется/выдаётся самая старая).
// При равных датах сравниваем по id для устойчивого порядка.
func (a Batch) Less(b btree.Item) bool {
	o := b.(Batch)
	if !a.BestBefore.Equal(o.BestBefore) {
		return a.BestBefore.Before(o.BestBefore)
	}
	return a.ID < o.ID
}

// VarietyBucket — корзина одного сорта.
type VarietyBucket struct {
	mu      sync.Mutex
	tree    *btree.BTree // ключ — Batch, упорядочен по BestBefore
	total   int          // суммарное количество яблок этого сорта (на складе, без резервов)
	variety AppleCultivar
}

func newVarietyBucket(v AppleCultivar) *VarietyBucket {
	return &VarietyBucket{
		tree:    btree.New(8),
		variety: v,
	}
}

// Storage — потокобезопасный склад.
type Storage struct {
	buckets sync.Map // AppleCultivar -> *VarietyBucket
	nextID  atomic.Uint64
}

func New() *Storage {
	return &Storage{}
}

// bucket возвращает корзину сорта, создавая её при первом обращении.
func (s *Storage) bucket(v AppleCultivar) *VarietyBucket {
	if b, ok := s.buckets.Load(v); ok {
		return b.(*VarietyBucket)
	}
	b, _ := s.buckets.LoadOrStore(v, newVarietyBucket(v))
	return b.(*VarietyBucket)
}

// Put кладёт партию на склад. Если у партии Quantity <= 0, она игнорируется.
func (s *Storage) Put(item ApplesWithBestBefore) {
	if item.Quantity <= 0 {
		return
	}
	b := s.bucket(item.Variety)
	b.mu.Lock()
	defer b.mu.Unlock()

	batch := Batch{
		ID:         s.nextID.Add(1),
		Variety:    item.Variety,
		Quantity:   item.Quantity,
		BestBefore: item.BestBefore,
	}
	b.tree.ReplaceOrInsert(batch)
	b.total += item.Quantity
}

// Reserve пытается зарезервировать qty яблок сорта variety с BestBefore >= minBestBefore.
//
// Сначала отдаём партии с самым близким сроком годности (но не раньше minBestBefore).
// Возвращает срез фактически выданных партий (с тем же BestBefore у каждой) и true при успехе.
// Если на складе нет достаточного количества подходящих яблок — ничего не меняет и возвращает (nil, false).
func (s *Storage) Reserve(variety AppleCultivar, qty int, minBestBefore time.Time) ([]ApplesWithBestBefore, bool) {
	if qty <= 0 {
		return nil, false
	}
	b := s.bucket(variety)
	b.mu.Lock()
	defer b.mu.Unlock()

	suitable := 0
	b.tree.Ascend(func(it btree.Item) bool {
		bt := it.(Batch)
		if bt.BestBefore.Before(minBestBefore) {
			return true
		}
		suitable += bt.Quantity
		return suitable < qty
	})
	if suitable < qty {
		return nil, false
	}

	remaining := qty
	taken := make([]ApplesWithBestBefore, 0, 2)
	var toDelete []Batch
	var toUpdate Batch
	var hasUpdate bool

	b.tree.Ascend(func(it btree.Item) bool {
		if remaining == 0 {
			return false
		}
		bt := it.(Batch)
		if bt.BestBefore.Before(minBestBefore) {
			return true
		}
		take := bt.Quantity
		if take > remaining {
			take = remaining
		}
		taken = append(taken, ApplesWithBestBefore{
			Apples:     Apples{Variety: variety, Quantity: take},
			BestBefore: bt.BestBefore,
		})
		if take == bt.Quantity {
			toDelete = append(toDelete, bt)
		} else {
			toUpdate = bt
			toUpdate.Quantity -= take
			hasUpdate = true
		}
		remaining -= take
		return remaining > 0
	})

	for _, bt := range toDelete {
		b.tree.Delete(bt)
	}
	if hasUpdate {
		b.tree.ReplaceOrInsert(toUpdate)
	}
	b.total -= qty
	return taken, true
}

// ReserveAny пытается зарезервировать qty яблок любых сортов с BestBefore >= minBestBefore.
func (s *Storage) ReserveAny(qty int, minBestBefore time.Time) ([]ApplesWithBestBefore, bool) {
	if qty <= 0 {
		return nil, false
	}
	var varieties []AppleCultivar
	s.buckets.Range(func(k, _ any) bool {
		varieties = append(varieties, k.(AppleCultivar))
		return true
	})

	remaining := qty
	result := make([]ApplesWithBestBefore, 0, 4)
	rollback := func() {
		for _, it := range result {
			s.Put(it)
		}
	}
	for _, v := range varieties {
		if remaining == 0 {
			break
		}
		b := s.bucket(v)
		b.mu.Lock()
		available := 0
		b.tree.Ascend(func(it btree.Item) bool {
			bt := it.(Batch)
			if bt.BestBefore.Before(minBestBefore) {
				return true
			}
			available += bt.Quantity
			return available < remaining
		})
		take := available
		if take > remaining {
			take = remaining
		}
		b.mu.Unlock()
		if take <= 0 {
			continue
		}
		got, ok := s.Reserve(v, take, minBestBefore)
		if !ok {
			continue
		}
		result = append(result, got...)
		remaining -= take
	}
	if remaining > 0 {
		rollback()
		return nil, false
	}
	return result, true
}

// Return возвращает партии на склад (например, при отмене заказа или ошибке доставки).
// Партии с истекшим сроком годности не возвращаются.
func (s *Storage) Return(items []ApplesWithBestBefore, now time.Time) {
	for _, it := range items {
		if it.Quantity <= 0 {
			continue
		}
		if !it.BestBefore.After(now) {
			continue // протухло, на склад не возвращаем
		}
		s.Put(it)
	}
}

// CollectExpired удаляет со склада все партии, у которых BestBefore <= now, и возвращает их.
func (s *Storage) CollectExpired(now time.Time) []ApplesWithBestBefore {
	var out []ApplesWithBestBefore
	s.buckets.Range(func(k, v any) bool {
		b := v.(*VarietyBucket)
		b.mu.Lock()
		var toDelete []Batch
		b.tree.Ascend(func(it btree.Item) bool {
			bt := it.(Batch)
			if bt.BestBefore.After(now) {
				return false // дальше идут только более свежие
			}
			toDelete = append(toDelete, bt)
			return true
		})
		for _, bt := range toDelete {
			b.tree.Delete(bt)
			b.total -= bt.Quantity
			out = append(out, ApplesWithBestBefore{
				Apples:     Apples{Variety: bt.Variety, Quantity: bt.Quantity},
				BestBefore: bt.BestBefore,
			})
		}
		b.mu.Unlock()
		return true
	})
	return out
}

// TotalQuantity возвращает общее количество яблок на складе (для тестов и диагностики).
func (s *Storage) TotalQuantity() int {
	total := 0
	s.buckets.Range(func(_, v any) bool {
		b := v.(*VarietyBucket)
		b.mu.Lock()
		total += b.total
		b.mu.Unlock()
		return true
	})
	return total
}
