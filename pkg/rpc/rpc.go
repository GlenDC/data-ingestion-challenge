package rpc

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"

	"github.com/streadway/amqp"

	"github.com/glendc/data-ingestion-challenge/pkg/log"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchangeName = flag.String("exchange", "mc_events", "AMQP exchange name")
)

// Channel creates a RabbitMQ channel used for RPC in this system
func Channel(cb func(*amqp.Channel)) {
	log.Infof("dialing AMPQ @ %s", *uri)
	conn, err := amqp.Dial(*uri)
	if err != nil {
		log.Errorf("couldn't dial AMPQ @ %s: %q", *uri, err)
	}
	defer conn.Close()

	log.Infof("opening channel & declaring exchange (%s)", *exchangeName)
	ch, err := conn.Channel()
	if err != nil {
		log.Errorf("couldn't open channel: %q", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		*exchangeName, // name
		"fanout",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Errorf("couldn't declare exchange: %q", err)
	}

	cb(ch)
}

// Dispatch produces and published data to the exchange
func Dispatch(ch *amqp.Channel, data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return ch.Publish(
		*exchangeName, // exchange
		"",            // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(bytes),
		})
}

// ConsumerConfig can be used to configure a consumer
type ConsumerConfig struct {
	Name string
}

// NewConsumerConfig returns a configuration for a consumer using sane defaults
func NewConsumerConfig() *ConsumerConfig {
	return new(ConsumerConfig)
}

// WithName overrides the default consumer name
func (cfg *ConsumerConfig) WithName(name string) *ConsumerConfig {
	cfg.Name = name
	return cfg
}

// DeliveryChannel is a channel used for delivery
type DeliveryChannel <-chan amqp.Delivery

// Consumer creates a channel and queue to be used for consumption
func Consumer(cfg *ConsumerConfig, cb func(DeliveryChannel)) {
	Channel(func(ch *amqp.Channel) {
		log.Infof("declaring and binding queue (%s)", cfg.Name)
		q, err := ch.QueueDeclare(
			cfg.Name, // name
			false,    // durable
			false,    // delete when usused
			true,     // exclusive
			false,    // no-wait
			nil,      // arguments
		)
		if err != nil {
			log.Errorf("failed to declare a queue: %q", err)
		}

		err = ch.QueueBind(
			q.Name,      // queue name
			"",          // routing key
			"mc_events", // exchange
			false,
			nil)
		if err != nil {
			log.Errorf("failed to bind a queue: %q", err)
		}

		log.Infof("declaring consumer for queue %s", q.Name)
		consumer, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			log.Errorf("failed to declare a consumer: %q", err)
		}

		cb(consumer)
	})
}

// Consume an AMQP delivery, rejecting automatically when an error occurs
func Consume(data amqp.Delivery, out interface{}) error {
	if data.ContentType != "application/json" {
		data.Reject(false) // no requeue needed, as its content type is not recognised
		return errors.New("invalid content type")
	}

	if err := json.Unmarshal(data.Body, out); err != nil {
		data.Reject(false) // no requeue needed, couldn't unpack value
		return fmt.Errorf("couldn't unpack delivery: %q", err)
	}

	return nil
}
