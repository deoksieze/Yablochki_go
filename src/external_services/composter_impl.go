package external_services

import (
	. "final-project/src/common"
	"fmt"
	"sync"
)

const defaultCapacity = 1000

// Composter — потокобезопасная реализация компостера.
// Ее нельзя менять, вы должны использовать интерфейс `ComposterService` в своем сервисе
type Composter struct {
	mu       sync.Mutex
	opened   bool
	working  bool
	capacity int
	ch       chan ApplesWithBestBefore
}

var _ ComposterService = (*Composter)(nil)

func NewComposter() *Composter {
	ch := make(chan ApplesWithBestBefore, defaultCapacity)
	close(ch)
	return &Composter{capacity: 1000, ch: ch}
}

func (c *Composter) OpenComposter() chan<- ApplesWithBestBefore {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.working {
		panic("safety first: composter is already working!!!")
	}

	if c.opened {
		panic("composter is already opened")
	}

	c.opened = true

	ch := make(chan ApplesWithBestBefore, c.capacity)
	for item := range c.ch {
		ch <- item
	}
	c.ch = ch

	return c.ch
}

func (c *Composter) CloseComposter() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.working {
		panic("safety first: composter is already working!!!")
	}

	if !c.opened {
		panic("composter is already closed")
	}

	c.opened = false
	close(c.ch)
}

func (c *Composter) TurnOnComposter() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.working {
		panic("safety first: composter is already working!!!")
	}

	if c.opened {
		panic("composter is opened!")
	}

	c.working = true
	for items := range c.ch {
		if items.Apples.Quantity != 1 {
			panic("яблоки надо было класть по одному, компостер сломался")
		}
		fmt.Printf("composting %v\n", items)
	}

}

// TurnOffComposter выключает компостер.
func (c *Composter) TurnOffComposter() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.working {
		panic("composter is already off")
	}

	if c.opened {
		panic("composter is opened!")
	}

	c.working = false
}
