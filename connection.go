package messaging

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func (producer *Producer) connect() (err error) {
	producer.connection, err = amqp.Dial(producer.connectionString)

	failOnError(producer.logger, err, "Producer: Failed to open AMQP channel")

	go producer.observeConnection()

	producer.channel, err = producer.connection.Channel()

	failOnError(producer.logger, err, "Producer: Failed to open channel")

	return nil
}

func (producer *Producer) observeConnection() {
	<-producer.connection.NotifyClose(make(chan *amqp.Error))
	for err := producer.connect(); err != nil; err = producer.connect() {
		producer.logger.Standard.Error().AnErr("producer-reconnection", err)
	}
}

func (consumer *Consumer) connect() (err error) {
	consumer.connection, err = amqp.Dial(consumer.connectionString)

	failOnError(consumer.logger, err, "Consumer: Failed to open AMQP channel")

	go consumer.observeConnection()

	consumer.channel, err = consumer.connection.Channel()

	failOnError(consumer.logger, err, "Consumer: Failed to open channel")

	return nil
}

func (consumer *Consumer) observeConnection() {
	<-consumer.connection.NotifyClose(make(chan *amqp.Error))
	for err := consumer.connect(); err != nil; err = consumer.connect() {
		consumer.logger.Standard.Error().AnErr("consumer-reconnection", err)
	}
}
