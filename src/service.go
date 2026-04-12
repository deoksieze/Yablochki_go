package src

import (
	"context"
	"final-project/src/api"
	. "final-project/src/common"
	. "final-project/src/external_services"
	"sort"
	"time"
)

type ServiceConfig struct {
	SuppliersParkingSize int //вместимость парковки для поставщиков
	DeliveryService      DeliveryService
	ComposterService     ComposterService
}

// Service сейчас это очень плохая _частичная_ реализация сервиса заказов, сделанная вашим предшественником.
// В ней очень много проблем и антипаттернов, но она _частично_ реализует необходимый функционал
type Service struct {
	//тут храним все яблоки на складе
	apples []ApplesWithBestBefore

	// каналы для поставщиков - свободные
	freeChan []chan ApplesWithBestBefore
	// каналы для поставщиков - занятые
	idToChan map[int64]chan ApplesWithBestBefore

	//глобальный идентификатор заказа клиента
	nextOrderId int64

	deliveryService  DeliveryService
	composterService ComposterService
}

// наш сервис должен удовлетворять API для покупателей и для поставщиков, специальный Гошный трюк
var _ api.ConsumerApi = (*Service)(nil)
var _ api.SupplierApi = (*Service)(nil)

func NewService(config ServiceConfig) *Service {

	freeChan := make([]chan ApplesWithBestBefore, 0, config.SuppliersParkingSize)
	for i := 0; i < config.SuppliersParkingSize; i++ {
		freeChan = append(freeChan, make(chan ApplesWithBestBefore))
	}

	return &Service{
		freeChan:         freeChan,
		idToChan:         make(map[int64]chan ApplesWithBestBefore),
		nextOrderId:      1,
		deliveryService:  config.DeliveryService,
		composterService: config.ComposterService,
	}
}

func (s *Service) BeginUnloading(SupplierID int64) (chan<- ApplesWithBestBefore, error) {
	// поставщик ждет, пока на парковке появится свободное место
	for len(s.freeChan) == 0 {
		time.Sleep(13 * time.Millisecond)
	}

	ch := s.freeChan[len(s.freeChan)-1]
	s.freeChan = s.freeChan[:len(s.freeChan)-1]

	// запускаем горутину, перекладывающую яблоки от поставщика на склад
	go func() {
		for {
			select {
			case box, ok := <-ch:
				if !ok {
					return
				}
				s.apples = append(s.apples, box)
			default:
				time.Sleep(42 * time.Millisecond)
			}
		}
	}()

	// этот канал теперь занят поставщиком.
	s.idToChan[SupplierID] = ch
	return ch, nil
}

func (s *Service) FinishUnloading(supplierID int64) error {
	// поставщик освободил канал - кладем в лист доступных
	s.freeChan = append(s.freeChan, s.idToChan[supplierID])
	delete(s.idToChan, supplierID)
	return nil
}

func (s *Service) PlaceOrderSimple(order api.SimpleOrder) (int64, error) {

	//упорядочиваем яблоки по дате срока годности
	sort.Slice(s.apples, func(i, j int) bool {
		return s.apples[i].BestBefore.Before(s.apples[j].BestBefore)
	})

	if order.Request.Quantity <= 0 {
		return 0, ErrInvalidRequest
	}

	//сколько яблок осталось зарезервировать
	remaining := order.Request.Quantity

	//тут собираем заказанные яблоки
	toDeliver := make([]ApplesWithBestBefore, 0)

	for i, item := range s.apples {
		if item.Quantity > 0 {
			if item.Variety == order.Request.Variety {
				if item.BestBefore.After(order.MinAllowedBestBefore) {

					selected := minInt(item.Quantity, remaining)
					toDeliver = append(toDeliver, ApplesWithBestBefore{
						Apples: Apples{
							Variety:  item.Variety,
							Quantity: selected,
						},
						BestBefore: item.BestBefore,
					})

					//уменьшаем количество яблок на складе
					s.apples[i].Quantity -= selected
					remaining -= selected

					// собрали весь заказ - отправляем через сервис доставки
					if remaining == 0 {
						return s.startDelivery(order.CustomerID, toDeliver), nil
					}
				}
			}
		}
	}

	//не смогли собрать весь заказ - возвращаем яблоки на склад
	s.revertReservation(toDeliver)
	return 0, ErrInsufficientQuantity
}

func (s *Service) PlaceOrderMulti(order api.MultiOrder) (int64, error) {
	panic("not implemented")
}

func (s *Service) PlaceOrderAny(order api.AnyApplesOrder) (int64, error) {
	panic("not implemented")
}

func (s *Service) Cancel(customerID int, orderID int64) error {
	panic("not implemented")
}

func (s *Service) startDelivery(customerID int, items []ApplesWithBestBefore) int64 {
	orderID := s.nextOrderId
	go s.sendApplesToClient(DeliveryRequest{CustomerID: customerID, OrderID: orderID, Items: items})
	s.nextOrderId++
	return orderID
}

func (s *Service) sendApplesToClient(request DeliveryRequest) {
	err := s.deliveryService.DoDelivery(context.TODO(), request)

	//курьер не смог доставить - возвращаем яблоки на склад
	if err != nil {
		s.revertReservation(request.Items)
	}
}

func (s *Service) revertReservation(items []ApplesWithBestBefore) {
	for _, item := range items {
		if item.Quantity > 0 {
			s.apples = append(s.apples, item)
		}
	}
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
