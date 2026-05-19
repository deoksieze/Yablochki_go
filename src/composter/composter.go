// Package composter содержит фонового воркера утилизации просроченных яблок.
//
// Алгоритм одного цикла утилизации (требования внешнего сервиса):
//  1. Собираем со склада все просроченные партии.
//  2. Раскладываем их в «штучки» (Quantity == 1) — компостер требует класть по одному.
//  3. OpenComposter() — получаем буферизованный канал ограниченной ёмкости.
//  4. Кладём штучки в канал по одному (если ёмкости буфера не хватает — отправка блокируется до места).
//  5. CloseComposter() — закрывает крышку, при этом канал становится недоступен для записи.
//  6. TurnOnComposter() — переработка (блокирующий вызов; компостер сам опустошает закрытый канал).
//  7. TurnOffComposter().
package composter

import (
	. "final-project/src/common"
	"final-project/src/external_services"
	"final-project/src/storage"
	"sync"
	"time"
)

// Worker — фоновая горутина, периодически утилизирующая просрочку.
type Worker struct {
	storage  *storage.Storage
	service  external_services.ComposterService
	interval time.Duration
	now      func() time.Time

	stopCh chan struct{}
	doneCh chan struct{}
	once   sync.Once
}

// NewWorker создаёт нового воркера. interval — период проверки просрочки.
func NewWorker(st *storage.Storage, svc external_services.ComposterService, interval time.Duration, now func() time.Time) *Worker {
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	if now == nil {
		now = time.Now
	}
	return &Worker{
		storage:  st,
		service:  svc,
		interval: interval,
		now:      now,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start запускает фоновую горутину.
func (w *Worker) Start() {
	go w.run()
}

func (w *Worker) run() {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			// Финальный проход — на случай если что-то протухло прямо к моменту остановки.
			w.processOnce()
			return
		case <-ticker.C:
			w.processOnce()
		}
	}
}

// processOnce выполняет один цикл утилизации просрочки.
func (w *Worker) processOnce() {
	expired := w.storage.CollectExpired(w.now())
	if len(expired) == 0 {
		return
	}
	// Разворачиваем в единичные яблоки.
	ch := w.service.OpenComposter()
	for _, it := range expired {
		for i := 0; i < it.Quantity; i++ {
			ch <- ApplesWithBestBefore{
				Apples:     Apples{Variety: it.Variety, Quantity: 1},
				BestBefore: it.BestBefore,
			}
		}
	}
	w.service.CloseComposter()
	w.service.TurnOnComposter() // блокирующий; компостер опустошит закрытый канал
	w.service.TurnOffComposter()
}

// Stop корректно останавливает фоновую горутину.
func (w *Worker) Stop() {
	w.once.Do(func() {
		close(w.stopCh)
	})
	<-w.doneCh
}
