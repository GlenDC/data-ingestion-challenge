package rpc

import (
	"encoding/json"
	"flag"

	"github.com/streadway/amqp"

	"github.com/glendc/data-ingestion-challenge/pkg/log"
)

// TODO
//  + Ensure that data is not lost in case a specific type of consumer is not active.
//    Meaning that even if no accountName worker is active, the events not yet tracked by
//    a accountName worker should still be enqueued, until one is active;
//  + Ensure that events are persistent in case of failure;

// RabbitMQ specific flags
var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchangeName = flag.String("exchange", "mc_events", "AMQP exchange name")
)

// NewAMQPChannel creates an open AMQP channel,
// using a newly created open RabbitMQ connection
// NOTE: Always make sure to Close a created AMQPChannel!
func NewAMQPChannel() (*AMQPChannel, error) {
	log.Infof("dialing AMPQ @ %s", *uri)
	conn, err := amqp.Dial(*uri)
	if err != nil {
		return nil, err
	}

	log.Infof("opening channel & declaring exchange (%s)", *exchangeName)
	ch, err := conn.Channel()
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

	log.Infof("dispatching application/json data to exchange %q", *exchangeName)
	return prod.ch.channel.Publish(
		*exchangeName, // exchange
		"",            // routing key
		false,         // mandatory
		false,         // immediate
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
		cfg.Name, // name
		true,     // durable
		false,    // delete when unused
		true,     // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		ch.Close() // ensure channel is closed
		return nil, err
	}

	err = ch.channel.QueueBind(
		q.Name,        // queue name
		"",            // routing key
		*exchangeName, // exchange
		false,
		nil)
	if err != nil {
		ch.Close() // ensure channel is closed
		return nil, err
	}

	consumer, err := ch.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
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
func (cons *AMQPConsumer) ListenAndConsume(cb ConsumeCallback) {
	forever := make(chan bool)

	go func() {
		var err *ConsumeError

		for data := range cons.deliveryChannel {
			if data.ContentType != "application/json" {
				data.Reject(false) // no requeue needed, as its content type is not recognised
				continue
			}

			if err = cb(data.Body); err != nil {
				data.Reject(err.Requeue) // consumer defines if we should requeue
				log.Warningf("event was rejected: %q", err)
				continue
			}

			// acknowledge event as received successfully
			data.Ack(false)
		}
	}()

	log.Infof("Listening to events. To exit press CTRL+C")
	<-forever
}

// UnmarshalConsumption takes in raw JSON data freshly delivered via RabbitMQ,
// and unwraps it into a given interface,
// returning a non-requeue consumeError in case the data could not be unwrapped.
func UnmarshalConsumption(raw []byte, data interface{}) *ConsumeError {
	if err := json.Unmarshal(raw, data); err != nil {
		return &ConsumeError{
			Requeue: false, // No Requeue needed as package is not recognised by consumer
			err:     err,
		}
	}

	return nil
}
