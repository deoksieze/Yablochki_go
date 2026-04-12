package external_services

import (
	. "final-project/src/common"
)

// ComposterService определяет интерфейс для взаимодействия с компостером. Изначально компостер закрыт и выключен
type ComposterService interface {

	// OpenComposter открывает крышку компостера. Можно вызывать только если компостер выключен, а крышка закрыта.
	//Возвращает буферезированный канал, в который можно отправлять яблоки (строго по одному!) для компостирования
	OpenComposter() chan<- ApplesWithBestBefore

	// CloseComposter закрывает крышку компостера. Можно вызывать только если компостер выключен, а крышка открыта.
	CloseComposter()

	// TurnOnComposter включает компостер и утилизирует яблоки. Можно вызывать только если компостер выключен, а крышка закрыта
	TurnOnComposter()

	// TurnOffComposter выключает компостер. Можно вызывать только если компостер включен, а крышка закрыта
	TurnOffComposter()
}
