package messaging

import (
	"context"
	"encoding/json"
	"time"

	logging "github.com/mitz-it/golang-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type IProducer interface {
	Produce(message any, configure ConfigureProducer)
}

type Producer struct {
	connectionString string
	connection       *amqp.Connection
	channel          *amqp.Channel
	logger           *logging.Logger
}

func (producer *Producer) Produce(message any, configure ConfigureProducer) {
	config := configureProducer(configure, message)

	declareExchange(producer.logger, producer.channel, config.ExchangeConfig)

	queue := declareQueue(producer.logger, producer.channel, config.QueueConfig)

	args := config.toArgumentsTable()

	config.bindQueueToExchange(
		producer.channel,
		queue,
		args,
	)

	body, err := json.Marshal(message)

	failOnError(producer.logger, err, "Failed to serialize message")

	ctx, cancel := context.WithTimeout(context.Background(), config.timeOut*time.Second)
	defer cancel()

	key := config.getKey(queue)

	exchange := config.getExchange()

	err = producer.channel.PublishWithContext(
		ctx,
		exchange,
		key,
		config.mandatory,
		config.immediate,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  string(config.contentType),
			Body:         body,
		},
	)

	failOnError(producer.logger, err, "Failed to publish message")
}

func NewProducer(logger *logging.Logger, connectionString string) IProducer {
	producer := &Producer{
		connectionString: connectionString,
		connection:       new(amqp.Connection),
		channel:          new(amqp.Channel),
		logger:           logger,
	}

	producer.connect()

	return producer
}
