package external_services

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Delivery это реализация сервиса доставки заказов - ее не надо менять. Используйте интерфейс DeliveryService
type Delivery struct {
}

var _ DeliveryService = (*Delivery)(nil)

func NewDelivery() *Delivery {
	return &Delivery{}
}

func (d *Delivery) DoDelivery(ctx context.Context, req DeliveryRequest) error {
	fmt.Printf("Delivering order %d\n", req.OrderID)
	select {
	case <-time.After(time.Duration(rand.Intn(1000)) * time.Millisecond):
		fmt.Printf("Delivered order %d\n", req.OrderID)
		return nil
	case <-ctx.Done():
		fmt.Printf("Delivery of order %d cancelled\n", req.OrderID)
		return ctx.Err()
	}
}
