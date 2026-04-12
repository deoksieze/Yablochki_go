package external_services

import (
	"context"
	. "final-project/src/common"
)

// DeliveryRequest описывает запрос на отправку клиенту его заказа
type DeliveryRequest struct {
	CustomerID int                    // CustomerID — идентификатор клиента
	OrderID    int64                  // OrderID — идентификатор заказа
	Items      []ApplesWithBestBefore // Items — яблоки, заказанные клиентом
}

// DeliveryService это внешний сервис, который отправляет клиенту его заказ. Вы должны взаимодействовать с ним для доставки заказов клиентам.
type DeliveryService interface {
	// DoDelivery пытается отправить клиенту его заказ.
	//Это блокирующий вызов, т.е. пока клиент не получит заказ, вызывающая сторона этой функции будет заблокирована.
	//
	//Возвращает nil, если клиент успешно получил свой заказ, иначе ошибку.
	//Если клиент не получил свой заказ, то яблоки должны быть возвращены на склад
	DoDelivery(ctx context.Context, req DeliveryRequest) error
}
