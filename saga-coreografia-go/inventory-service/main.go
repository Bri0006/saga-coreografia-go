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

	// 1. Conexión a PostgreSQL de Inventario (Puerto 5434)
	dsn := "host=localhost user=postgres password=password dbname=inventory_db port=5434 sslmode=disable"
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Error conectando a la base de datos de inventario: %v", err)
	}
	fmt.Println("Conectado exitosamente a PostgreSQL (Inventory DB) 🚀")

	DB.AutoMigrate(&Inventory{})
	seedInitialStock()

	// 2. Conexión a RabbitMQ
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

	// Declarar todas las colas donde Inventario escucha o publica
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

	// 3. Iniciar consumidores
	go listenOrderCreated()
	go listenPaymentFailed()

	fmt.Println("Inventory Service corriendo y escuchando eventos en RabbitMQ 🟢")
	select {} // Mantener el servicio activo
}

// Crea stock inicial si no existe para poder hacer pruebas directas
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

// Escucha OrderCreated -> Descuenta stock -> Publica InventoryReserved
func listenOrderCreated() {
	msgs, err := RabbitChannel.Consume("order_created_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error al consumir order_created_queue: %v", err)
	}

	for d := range msgs {
		var event OrderCreatedEvent
		json.Unmarshal(d.Body, &event)

		fmt.Printf("📥 [OrderCreated] Recibida Orden %d (Producto: %s, Cantidad: %d)\n",
			event.OrderID, event.ProductID, event.Quantity)

		var item Inventory
		// Si el producto no existe en la BD, lo creamos con stock base de 20 para no bloquear pruebas
		if err := DB.Where("product_id = ?", event.ProductID).First(&item).Error; err != nil {
			item = Inventory{ProductID: event.ProductID, Stock: 20}
			DB.Create(&item)
		}

		// Descontar producto de la base de datos de inventario
		item.Stock -= event.Quantity
		DB.Save(&item)
		fmt.Printf("📉 Stock descontado para %s. Stock restante: %d\n", item.ProductID, item.Stock)

		// Publicar evento InventoryReserved
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

// COMPENSACIÓN: Escucha PaymentFailed -> Devuelve stock -> Publica StockReleased
func listenPaymentFailed() {
	msgs, err := RabbitChannel.Consume("payment_failed_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error al consumir payment_failed_queue: %v", err)
	}

	for d := range msgs {
		var event PaymentFailedEvent
		json.Unmarshal(d.Body, &event)

		fmt.Printf("⚠️ [PaymentFailed] Pago fallido en Orden %d. Ejecutando compensación de inventario...\n", event.OrderID)

		var item Inventory
		if err := DB.Where("product_id = ?", event.ProductID).First(&item).Error; err == nil {
			item.Stock += event.Quantity
			DB.Save(&item)
			fmt.Printf("🔄 COMPENSACIÓN: +%d unidades devueltas a %s. Stock actual: %d\n",
				event.Quantity, item.ProductID, item.Stock)
		}

		// Publicar evento StockReleased hacia el Order Service de tu compañera
		releasedEvent := StockReleasedEvent{
			OrderID: event.OrderID,
		}
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
