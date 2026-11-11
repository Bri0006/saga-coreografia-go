package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Modelo de base de datos para Inventario
type Inventory struct {
	gorm.Model
	ProductID string `gorm:"uniqueIndex" json:"product_id"`
	Stock     int    `json:"stock"`
}

// 1. LO QUE FALTABA: El modelo para la tabla Kardex
type Kardex struct {
	gorm.Model
	OrderID   uint   `json:"order_id"`
	ProductID string `json:"product_id"`
	Movement  int    `json:"movement"`
	Reason    string `json:"reason"`
}

// Eventos que coinciden con el formato de order-service
type OrderCreatedEvent struct {
	OrderID   uint   `json:"order_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type InventoryReservedEvent struct {
	OrderID   uint   `json:"order_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type PaymentFailedEvent struct {
	OrderID   uint   `json:"order_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type StockReleasedEvent struct {
	OrderID uint `json:"order_id"`
}

var DB *gorm.DB
var RabbitConn *amqp.Connection
var RabbitChannel *amqp.Channel

func main() {
	var err error

	// Conexión a PostgreSQL de Inventario (Puerto 5434)
	dsn := "host=localhost user=postgres password=password dbname=inventory_db port=5434 sslmode=disable"
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Error conectando a la base de datos de inventario: %v", err)
	}
	fmt.Println("Conectado exitosamente a PostgreSQL (Inventory DB) 🚀")

	// 2. LO QUE FALTABA: AutoMigrate ahora crea la tabla Kardex
	DB.AutoMigrate(&Inventory{}, &Kardex{})
	seedInitialStock()

	// Conexión a RabbitMQ
	RabbitConn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Error conectando a RabbitMQ: %v", err)
	}
	defer RabbitConn.Close()

	RabbitChannel, err = RabbitConn.Channel()
	if err != nil {
		log.Fatalf("Error abriendo canal en RabbitMQ: %v", err)
	}
	defer RabbitChannel.Close()

	queues := []string{
		"order_created_queue",
		"inventory_reserved_queue",
		"payment_failed_queue",
		"stock_released_queue",
	}
	for _, q := range queues {
		_, err = RabbitChannel.QueueDeclare(q, true, false, false, false, nil)
		if err != nil {
			log.Fatalf("Error declarando cola %s: %v", q, err)
		}
	}

	go listenOrderCreated()
	go listenPaymentFailed()

	fmt.Println("Inventory Service corriendo y escuchando eventos en RabbitMQ 🟢")
	select {}
}

func seedInitialStock() {
	products := []Inventory{
		{ProductID: "PROD-1", Stock: 50},
		{ProductID: "PROD-2", Stock: 30},
	}
	for _, p := range products {
		var existing Inventory
		if err := DB.Where("product_id = ?", p.ProductID).First(&existing).Error; err != nil {
			DB.Create(&p)
			fmt.Printf("📦 Stock inicial creado: %s -> %d unidades\n", p.ProductID, p.Stock)
		}
	}
}

func listenOrderCreated() {
	msgs, err := RabbitChannel.Consume("order_created_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error al consumir order_created_queue: %v", err)
	}

	for d := range msgs {
		var event OrderCreatedEvent
		json.Unmarshal(d.Body, &event)

		fmt.Printf("📥 [OrderCreated] Recibida Orden %d (Producto: %s, Cantidad: %d)\n", event.OrderID, event.ProductID, event.Quantity)

		var item Inventory
		if err := DB.Where("product_id = ?", event.ProductID).First(&item).Error; err != nil {
			item = Inventory{ProductID: event.ProductID, Stock: 20}
			DB.Create(&item)
		}

		// Descontar producto
		item.Stock -= event.Quantity
		DB.Save(&item)
		fmt.Printf("📉 Stock descontado para %s. Stock restante: %d\n", item.ProductID, item.Stock)

		// 3. LO QUE FALTABA: Guardar la reserva (Salida) en la tabla Kardex
		kardex := Kardex{
			OrderID:   event.OrderID,
			ProductID: event.ProductID,
			Movement:  -event.Quantity,
			Reason:    "RESERVA POR COMPRA",
		}
		DB.Create(&kardex)

		reservedEvent := InventoryReservedEvent{
			OrderID:   event.OrderID,
			ProductID: event.ProductID,
			Quantity:  event.Quantity,
		}
		body, _ := json.Marshal(reservedEvent)

		err = RabbitChannel.Publish("", "inventory_reserved_queue", false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
		if err == nil {
			fmt.Printf("📤 Evento InventoryReserved publicado para Orden %d\n", event.OrderID)
		}
	}
}

func listenPaymentFailed() {
	msgs, err := RabbitChannel.Consume("payment_failed_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error al consumir payment_failed_queue: %v", err)
	}

	for d := range msgs {
		var event PaymentFailedEvent
		json.Unmarshal(d.Body, &event)

		fmt.Printf("⚠️ [PaymentFailed] Pago fallido en Orden %d. Ejecutando compensación...\n", event.OrderID)

		var item Inventory
		if err := DB.Where("product_id = ?", event.ProductID).First(&item).Error; err == nil {
			item.Stock += event.Quantity
			DB.Save(&item)
			fmt.Printf("🔄 COMPENSACIÓN: +%d unidades devueltas a %s. Stock actual: %d\n", event.Quantity, item.ProductID, item.Stock)

			// 4. LO QUE FALTABA: Guardar el Registro Compensatorio (Entrada) en la tabla Kardex
			kardex := Kardex{
				OrderID:   event.OrderID,
				ProductID: event.ProductID,
				Movement:  event.Quantity,
				Reason:    "REGISTRO COMPENSATORIO (PAGO FALLIDO)",
			}
			DB.Create(&kardex)
		}

		releasedEvent := StockReleasedEvent{OrderID: event.OrderID}
		body, _ := json.Marshal(releasedEvent)

		err = RabbitChannel.Publish("", "stock_released_queue", false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
		if err == nil {
			fmt.Printf("📤 Evento StockReleased publicado para cancelar la Orden %d\n", event.OrderID)
		}
	}
}
