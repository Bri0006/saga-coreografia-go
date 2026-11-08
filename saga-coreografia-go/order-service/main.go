package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/streadway/amqp"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Order struct {
	gorm.Model
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Total     float64 `json:"total"`
	Status    string  `json:"status"` // Pending, Completed, Cancelled
}

type OrderCreatedEvent struct {
	OrderID   uint   `json:"order_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type PaymentProcessedEvent struct {
	OrderID uint `json:"order_id"`
}

type StockReleasedEvent struct {
	OrderID uint `json:"order_id"`
}

var DB *gorm.DB
var RabbitConn *amqp.Connection
var RabbitChannel *amqp.Channel

func main() {
	var err error

	// 1. Conexión a PostgreSQL (Docker con puerto 5433)
	dsn := "host=localhost user=postgres password=password dbname=orders_db port=5433 sslmode=disable"
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Error conectando a la base de datos: %v", err)
	}
	fmt.Println("Conectado exitosamente a PostgreSQL 🚀")

	DB.AutoMigrate(&Order{})

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

	// Declarar colas necesarias
	queues := []string{"order_created_queue", "payment_processed_queue", "stock_released_queue"}
	for _, q := range queues {
		_, err = RabbitChannel.QueueDeclare(q, true, false, false, false, nil)
		if err != nil {
			log.Fatalf("Error declarando cola %s: %v", q, err)
		}
	}

	// 3. Iniciar escuchas (Consumidores) en segundo plano para el Cierre del Saga
	go listenPaymentProcessed()
	go listenStockReleased()

	// 4. Configurar API REST con Gin
	r := gin.Default()

	r.POST("/orders", func(c *gin.Context) {
		var input struct {
			ProductID string  `json:"product_id"`
			Quantity  int     `json:"quantity"`
			Total     float64 `json:"total"`
		}

		if err := c.ShouldBindJSON(&input); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		order := Order{
			ProductID: input.ProductID,
			Quantity:  input.Quantity,
			Total:     input.Total,
			Status:    "Pending",
		}

		if result := DB.Create(&order); result.Error != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "No se pudo crear la orden"})
			return
		}

		// Publicar OrderCreated
		event := OrderCreatedEvent{
			OrderID:   order.ID,
			ProductID: order.ProductID,
			Quantity:  order.Quantity,
		}
		body, _ := json.Marshal(event)

		err = RabbitChannel.Publish("", "order_created_queue", false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Orden creada pero falló al publicar el evento"})
			return
		}

		c.JSON(http.StatusCreated, gin.H{
			"message": "Orden creada exitosamente y evento publicado 📦",
			"order":   order,
		})
	})

	fmt.Println("Order Service corriendo en el puerto 8080 🟢")
	r.Run(":8080")
}

// Escucha si el pago fue procesado con éxito para confirmar la orden
func listenPaymentProcessed() {
	msgs, err := RabbitChannel.Consume("payment_processed_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Printf("Error al consumir payment_processed_queue: %v", err)
		return
	}

	for d := range msgs {
		var event PaymentProcessedEvent
		json.Unmarshal(d.Body, &event)

		var order Order
		if result := DB.First(&order, event.OrderID); result.Error == nil {
			order.Status = "Completed"
			DB.Save(&order)
			fmt.Printf("✅ Orden %d CONFIRMADA (Pago exitoso)\n", order.ID)
		}
	}
}

// Escucha si algo falló para cancelar la orden (Compensación)
func listenStockReleased() {
	msgs, err := RabbitChannel.Consume("stock_released_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Printf("Error al consumir stock_released_queue: %v", err)
		return
	}

	for d := range msgs {
		var event StockReleasedEvent
		json.Unmarshal(d.Body, &event)

		var order Order
		if result := DB.First(&order, event.OrderID); result.Error == nil {
			order.Status = "Cancelled"
			DB.Save(&order)
			fmt.Printf("❌ Orden %d CANCELADA (Compensación ejecutada)\n", order.ID)
		}
	}
}