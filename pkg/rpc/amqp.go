package rpc

import (
	"encoding/json"
	"flag"
	"time"

	"github.com/streadway/amqp"

	"github.com/glendc/data-ingestion-challenge/pkg"
	"github.com/glendc/data-ingestion-challenge/pkg/log"
)

// RabbitMQ specific flags
var (
	uri          string
	exchangeName string
	messageTTL   time.Duration
)

// Exchange Constants
const (
	// exchanges will survive server restarts
	// and remain declared when there are no remaining bindings
	exchangeDurable     = true
	exchangeAutoDeleted = false
)

// NewAMQPChannel creates an open AMQP channel,
// using a newly created open RabbitMQ connection
// NOTE: Always make sure to Close a created AMQPChannel!
func NewAMQPChannel() (*AMQPChannel, error) {
	log.Infof("dialing AMPQ @ %s", uri)
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, err
	}

	log.Infof("opening channel & declaring exchange (%s)", exchangeName)
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName,        // name
		"fanout",            // type
		exchangeDurable,     // durable
		exchangeAutoDeleted, // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return nil, err
	}

	return &AMQPChannel{
		connection: conn,
		channel:    ch,
	}, nil
}

// AMQPChannel stores an open RabbitMQ connection and channel
type AMQPChannel struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// Close the open RabbitMQ channel and connection
// stored in this AMQPChannel instance
func (ch *AMQPChannel) Close() (err error) {
	if err = ch.channel.Close(); err != nil {
		log.Warningf("couldn't close AMQP channel: %q", err)
	}
	if err = ch.connection.Close(); err != nil {
		log.Warningf("couldn't close AMQP connection: %q", err)
	}
	return
}

// NewAMQPProducer creates a Producer ready for
// dispatching data to the RabbitMQ system.
// NOTE: Always make sure to Close a created Producer!
func NewAMQPProducer() (Producer, error) {
	ch, err := NewAMQPChannel()
	if err != nil {
		return nil, err
	}

	return &AMQPProducer{ch: ch}, nil
}

// AMQPProducer is the Producer implementation for a RabbitMQ based system
type AMQPProducer struct {
	ch *AMQPChannel
}

// Close the open RabbitMQ channel and connection,
// using a producer after it has been closed results in an error.
func (prod *AMQPProducer) Close() error {
	return prod.ch.Close()
}

// Dispatch (publish) data to the RabbitMQ exchange declared on its open channel
func (prod *AMQPProducer) Dispatch(data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	log.Infof("dispatching application/json data to exchange %q", exchangeName)
	return prod.ch.channel.Publish(
		exchangeName, // exchange
		// no routing key is used here, or in the queue decleration
		"", // routing key
		// keep deliveries in queue, even if no consumer is active
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(bytes),
		})
}

// AMQPConsConfig can be used to configure an amqp consumer
type AMQPConsConfig struct {
	Name string
}

// NewAMQPConsConfig returns a configuration for an ampq consumer,
// using sane defaults
func NewAMQPConsConfig() *AMQPConsConfig {
	return new(AMQPConsConfig)
}

// WithName overrides the default queue name
func (cfg *AMQPConsConfig) WithName(name string) *AMQPConsConfig {
	cfg.Name = name
	return cfg
}

// Queue Constants
const (
	// will survive server restarts and
	// remain declared when there are no remaining bindings
	queueAutoDeleted = false
	queueDurable     = true
	// non-exclusive queue
	queueExclusive = false
	// default routing-key used
	queueRoutingKey = ""
)

// NewAMQPConsumer creates a Consumer ready for consumption of
// data delivered via the RabbitMQ system.
// Also declares a queue created using the given configuration.
// NOTE: Always make sure to Close a created Consumer!
func NewAMQPConsumer(cfg *AMQPConsConfig) (Consumer, error) {
	ch, err := NewAMQPChannel()
	if err != nil {
		return nil, err
	}

	q, err := ch.channel.QueueDeclare(
		cfg.Name,         // name
		queueDurable,     // durable
		queueAutoDeleted, // delete when unused
		queueExclusive,   // exclusive
		false,            // no-wait
		amqp.Table{
			// message TTL in ms
			// more information: https://www.rabbitmq.com/ttl.html
			// this to ensure that our durable queues don't
			// grow to the max size of our hardware limits
			"x-message-ttl": messageTTL.Nanoseconds() / 1000000,
		}, // arguments
	)
	if err != nil {
		ch.Close() // ensure channel is closed
		return nil, err
	}

	err = ch.channel.QueueBind(
		q.Name,          // queue name
		queueRoutingKey, // routing key
		exchangeName,    // exchange
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		ch.Close() // ensure channel is closed
		return nil, err
	}

	consumer, err := ch.channel.Consume(
		q.Name, // queue
		"",     // consumer
		// deliveries need excplit acknowledgement from users
		// see AMQPConsumer::ListenAndConsume for more information
		false,          // auto-ack
		queueExclusive, // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	if err != nil {
		ch.Close() // ensure channel is closed
		return nil, err
	}

	return &AMQPConsumer{
		ch:              ch,
		deliveryChannel: consumer,
	}, nil
}

// AMQPConsumer is the Consumer implementation for a RabbitMQ based system
type AMQPConsumer struct {
	ch              *AMQPChannel
	deliveryChannel <-chan amqp.Delivery
}

// Close the open RabbitMQ channel and connection
func (cons *AMQPConsumer) Close() error {
	return cons.ch.Close()
}

// ListenAndConsume any data received from the RabbitMQ system
// The actual processing of the received data is done by the given callback (cb)
// Acknowledgement of deliveries is explicitely done using Reject/Ack
func (cons *AMQPConsumer) ListenAndConsume(cb ConsumeCallback) {
	forever := make(chan bool)

	go func() {
		var consumeError *ConsumeError
		var unmarshalError error
		var event pkg.Event

		for data := range cons.deliveryChannel {
			if data.ContentType != "application/json" {
				data.Reject(false) // no requeue needed, as its content type is not recognised
				log.Warningf("event was rejected: %q", unmarshalError)
				continue
			}

			unmarshalError = json.Unmarshal(data.Body, &event)
			if unmarshalError != nil {
				data.Reject(false) // no requeue needed, as the data is invalid
				log.Warningf("event was rejected: %q", unmarshalError)
				continue
			}
			unmarshalError = event.Validate()
			if unmarshalError != nil {
				data.Reject(false) // no requeue needed, as the data is invalid
				log.Warningf("event was rejected: %q", unmarshalError)
				continue
			}

			if consumeError = cb(&event); consumeError != nil {
				data.Reject(consumeError.Requeue) // consumer defines if we should requeue
				log.Warningf("event was rejected by consumer: %q", consumeError)
				continue
			}

			// acknowledge event as received successfully
			data.Ack(false)
		}
	}()

	log.Infof("Listening to events. To exit press CTRL+C")
	<-forever
}

func init() {
	flag.StringVar(&uri, "uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	flag.StringVar(&exchangeName, "exchange", "metric-collector", "AMQP exchange name")
	flag.DurationVar(&messageTTL, "message-ttl", time.Duration(time.Hour*24), "Message Time To Live (TTL)")
}
