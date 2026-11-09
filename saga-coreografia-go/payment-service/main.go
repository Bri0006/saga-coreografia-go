package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

type InventoryReservedEvent struct {
	OrderID   uint   `json:"order_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type PaymentProcessedEvent struct {
	OrderID uint `json:"order_id"`
}

type PaymentFailedEvent struct {
	OrderID   uint   `json:"order_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

var RabbitConn *amqp.Connection
var RabbitChannel *amqp.Channel

func main() {
	rand.Seed(time.Now().UnixNano())
	var err error

	// 1. Conexión a RabbitMQ
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
	queues := []string{
		"inventory_reserved_queue",
		"payment_processed_queue",
		"payment_failed_queue",
	}
	for _, q := range queues {
		_, err = RabbitChannel.QueueDeclare(q, true, false, false, false, nil)
		if err != nil {
			log.Fatalf("Error declarando cola %s: %v", q, err)
		}
	}

	// 2. Escuchar evento InventoryReserved
	go listenInventoryReserved()

	fmt.Println("Payment Service corriendo y esperando reservas de inventario 💳🟢")
	select {}
}

func listenInventoryReserved() {
	msgs, err := RabbitChannel.Consume("inventory_reserved_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error al consumir inventory_reserved_queue: %v", err)
	}

	for d := range msgs {
		var event InventoryReservedEvent
		json.Unmarshal(d.Body, &event)

		fmt.Printf("💳 Procesando cobro para la Orden %d...\n", event.OrderID)
		time.Sleep(1 * time.Second) // Simular latencia de pasarela de pago

		// Simulación de error aleatorio del 30% (números de 0 a 99: si es < 30, falla)
		if rand.Intn(100) < 30 {
			fmt.Printf("❌ Pago RECHAZADO (Tarjeta declinada) para la Orden %d\n", event.OrderID)

			failedEvent := PaymentFailedEvent{
				OrderID:   event.OrderID,
				ProductID: event.ProductID,
				Quantity:  event.Quantity,
			}
			body, _ := json.Marshal(failedEvent)

			RabbitChannel.Publish("", "payment_failed_queue", false, false, amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			})
			fmt.Printf("📤 Evento PaymentFailed publicado para Orden %d\n", event.OrderID)
		} else {
			fmt.Printf("✅ Pago EXITOSO para la Orden %d\n", event.OrderID)

			successEvent := PaymentProcessedEvent{
				OrderID: event.OrderID,
			}
			body, _ := json.Marshal(successEvent)

			RabbitChannel.Publish("", "payment_processed_queue", false, false, amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			})
			fmt.Printf("📤 Evento PaymentProcessed publicado para Orden %d\n", event.OrderID)
		}
	}
}
