package messaging

import (
	"context"

	logging "github.com/mitz-it/golang-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OnMessageReceived func(ctx context.Context, message []byte)

type IConsumer interface {
	Consume(configure ConfigureConsumer, onMessageReceived OnMessageReceived)
}

type Consumer struct {
	connectionString string
	connection       *amqp.Connection
	channel          *amqp.Channel
	logger           *logging.Logger
}

func (consumer *Consumer) Consume(configure ConfigureConsumer, onMessageReceived OnMessageReceived) {
	config := configureConsumer(configure)

	declareExchange(consumer.logger, consumer.channel, config.ExchangeConfig)

	queue := declareQueue(consumer.logger, consumer.channel, config.QueueConfig)

	args := config.toArgumentsTable()

	config.bindQueueToExchange(
		consumer.channel,
		queue,
		args,
	)

	config.configureQoS(consumer.channel, consumer.logger)

	key := config.getKey(queue)

	messages, err := consumer.channel.Consume(
		key,
		config.consumerIdentity,
		config.autoAck,
		config.exclusive,
		config.noLocal,
		config.noWait,
		args,
	)

	failOnError(consumer.logger, err, "Failed to register a consumer")

	var forever chan struct{}

	go consumer.handleMessages(messages, onMessageReceived, config.autoAck, config, key)

	consumer.logger.Standard.Info().Msg("Waiting for messages")
	<-forever
}

func (consumer *Consumer) handleMessages(messages <-chan amqp.Delivery, onMessageReceived OnMessageReceived, autoAck bool, config *ConsumerConfiguration, key string) {
	for message := range messages {
		ctx := consumer.createConsumeContext(context.Background(), config, message, key)
		onMessageReceived(ctx, message.Body)
		if !autoAck {
			message.Ack(false)
		}
	}
}

func NewConsumer(logger *logging.Logger, connectionString string) IConsumer {
	consumer := &Consumer{
		connectionString: connectionString,
		connection:       new(amqp.Connection),
		channel:          new(amqp.Channel),
		logger:           logger,
	}

	consumer.connect()

	return consumer
}
