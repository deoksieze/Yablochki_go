package main

import (
	. "final-project/src"
	"final-project/src/api"
	. "final-project/src/common"
	"final-project/src/external_services"
	"fmt"
	"time"
)

func main() {
	deliveryService := external_services.NewDelivery()
	composterService := external_services.NewComposter()

	myService := NewService(ServiceConfig{
		SuppliersParkingSize: 2,
		DeliveryService:      deliveryService,
		ComposterService:     composterService},
	)

	go runSupplier(201, myService, Antonovka)
	go runSupplier(202, myService, GoldenDelicious)
	go runSupplier(203, myService, Antonovka)

	go runConsumer(101, myService, Antonovka)
	go runConsumer(102, myService, PinkLady)

	time.Sleep(1 * time.Minute)
}

func runSupplier(id int64, supplierAPI api.SupplierApi, cultivar AppleCultivar) {
	for {
		fmt.Println("supplier", id, "started new iteration")
		ch, err := supplierAPI.BeginUnloading(id)
		if err != nil {
			panic(err)
		}
		fmt.Println("supplier", id, "started unloading")

		for range 10 {
			fmt.Println("supplier", id, "unloading 4 apples: ", cultivar)
			ch <- ApplesWithBestBefore{
				Apples:     Apples{Variety: cultivar, Quantity: 4},
				BestBefore: time.Now().Add(time.Hour * 60)}

			time.Sleep(100 * time.Millisecond)
		}

		fmt.Println("supplier", id, "finished unloading")
		err = supplierAPI.FinishUnloading(id)
		if err != nil {
			panic(err)
		}
		time.Sleep(10 * time.Second)
	}
}

func runConsumer(id int64, consumerAPI api.ConsumerApi, cultivar AppleCultivar) {
	for {
		fmt.Println("consumer", id, "started new iteration")

		orderId, err := consumerAPI.PlaceOrderSimple(api.SimpleOrder{
			CustomerID:           int(id),
			Request:              Apples{Variety: cultivar, Quantity: 10},
			MinAllowedBestBefore: time.Now().Add(time.Hour * 10),
		})
		if err != nil {
			fmt.Println("consumer", id, "failed to place order:", err)
			time.Sleep(3 * time.Second)
			continue
		}

		fmt.Print("consumer", id, " placed order with id ", orderId, " for 10 apples: ", cultivar, "\n")
		time.Sleep(4 * time.Second)
	}
}
