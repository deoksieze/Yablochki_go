// Package parking управляет парковкой поставщиков и сессиями разгрузки.

// Парковка реализована как семафор-канал (`chan struct{}` ёмкости N):
// поставщик пишет в канал, чтобы «занять место». Если мест нет — операция блокируется до освобождения.

package parking

import (
	"errors"
	. "final-project/src/common"
	"final-project/src/storage"
	"sync"
)

var (
	ErrUnloadingNotStarted  = errors.New("supplier did not start unloading")
	ErrUnloadingAlreadyOpen = errors.New("supplier already has an active unloading session")
	ErrParkingClosed        = errors.New("parking is closed")
)

// Parking реализует ограниченную парковку и сессии разгрузки.
type Parking struct {
	slots   chan struct{} // семафор парковочных мест
	storage *storage.Storage

	mu       sync.Mutex
	sessions map[int64]*session
	closed   bool

	wg sync.WaitGroup
}

type session struct {
	ch     chan ApplesWithBestBefore
	doneCh chan struct{}
}

func New(parkingSize int, st *storage.Storage) *Parking {
	if parkingSize < 1 {
		parkingSize = 1
	}
	return &Parking{
		slots:    make(chan struct{}, parkingSize),
		storage:  st,
		sessions: make(map[int64]*session),
	}
}

// BeginUnloading блокируется, пока на парковке не появится свободное место,
// затем создаёт сессию для поставщика и возвращает канал, куда тот пишет партии.
func (p *Parking) BeginUnloading(supplierID int64) (chan<- ApplesWithBestBefore, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrParkingClosed
	}
	if _, exists := p.sessions[supplierID]; exists {
		p.mu.Unlock()
		return nil, ErrUnloadingAlreadyOpen
	}
	p.mu.Unlock()

	p.slots <- struct{}{}

	p.mu.Lock()
	if p.closed {
		<-p.slots
		p.mu.Unlock()
		return nil, ErrParkingClosed
	}
	if _, exists := p.sessions[supplierID]; exists {
		<-p.slots
		p.mu.Unlock()
		return nil, ErrUnloadingAlreadyOpen
	}
	ch := make(chan ApplesWithBestBefore, 16)
	sess := &session{ch: ch, doneCh: make(chan struct{})}
	p.sessions[supplierID] = sess
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(sess.doneCh)
		for box := range ch {
			p.storage.Put(box)
		}
	}()

	return ch, nil
}

// FinishUnloading завершает сессию поставщика: закрывает его канал, дожидается приёмника
// и освобождает парковочное место.
func (p *Parking) FinishUnloading(supplierID int64) error {
	p.mu.Lock()
	sess, ok := p.sessions[supplierID]
	if !ok {
		p.mu.Unlock()
		return ErrUnloadingNotStarted
	}
	delete(p.sessions, supplierID)
	p.mu.Unlock()

	close(sess.ch)
	<-sess.doneCh

	<-p.slots
	return nil
}

func (p *Parking) Stop() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	sessions := p.sessions
	p.sessions = map[int64]*session{}
	p.mu.Unlock()

	for _, sess := range sessions {
		close(sess.ch)
	}
	p.wg.Wait()
}

func (p *Parking) OccupiedSlots() int {
	return len(p.slots)
}
