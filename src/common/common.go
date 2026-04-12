package common

import (
	"errors"
	"time"
)

// AppleCultivar это конкретный сорт яблока
type AppleCultivar string

const (
	GrannySmith     AppleCultivar = "granny_smith"
	GoldenDelicious AppleCultivar = "golden_delicious"
	RedDelicious    AppleCultivar = "red_delicious"
	Fuji            AppleCultivar = "fuji"
	Gala            AppleCultivar = "gala"
	Honeycrisp      AppleCultivar = "honeycrisp"
	PinkLady        AppleCultivar = "pink_lady"
	Antonovka       AppleCultivar = "antonovka"
	WolfRiver       AppleCultivar = "wolf_river"
)

// Apples это кортеж: какой-то конкретный сорт яблок + их количество
type Apples struct {
	Variety  AppleCultivar // Сорт яблок
	Quantity int           // Количество яблок данного сорта
}

// ApplesWithBestBefore это кортеж: какой-то конкретный сорт яблок + их количество + дата их годности
type ApplesWithBestBefore struct {
	Apples
	BestBefore time.Time
}

var (
	ErrInsufficientQuantity = errors.New("insufficient quantity of suitable apples to fulfill the order")
	ErrInvalidRequest       = errors.New("invalid request")
	//добавляйте новые типы ошибок сюда, если они понадобятся
)
