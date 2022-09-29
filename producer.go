package messaging

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	logging "github.com/mitz-it/golang-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type IProducer interface {
	Produce(ctx context.Context, message any, configure ConfigureProducer)
}

type Producer struct {
	connectionString string
	connection       *amqp.Connection
	channel          *amqp.Channel
	logger           *logging.Logger
}

func (producer *Producer) Produce(ctx context.Context, message any, configure ConfigureProducer) {
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

	ctx, cancel := context.WithTimeout(ctx, config.timeOut*time.Second)
	defer cancel()

	key := config.getKey(queue)

	exchange := config.getExchange()

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  string(config.contentType),
		Body:         body,
		MessageId:    uuid.New().String(),
	}

	amqpContext, headers := producer.createPublishContext(ctx, config, msg)

	msg.Headers = headers

	err = producer.channel.PublishWithContext(
		amqpContext,
		exchange,
		key,
		config.mandatory,
		config.immediate,
		msg,
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
