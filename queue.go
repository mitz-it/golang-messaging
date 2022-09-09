package messaging

import (
	logging "github.com/mitz-it/golang-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type queueConfiguration struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	arguments  *Arguments
}

func NewQueueConfiguration() *queueConfiguration {
	return &queueConfiguration{
		name:       "",
		durable:    defaultDurable,
		autoDelete: defaultAutoDelete,
		exclusive:  defaultExclusive,
		noWait:     defaultNoWait,
		arguments:  nil,
	}
}

func declareQueue(logger *logging.Logger, channel *amqp.Channel, config *queueConfiguration) *amqp.Queue {
	if config == nil {
		return nil
	}

	args := toArgumentsTable(config.arguments)

	queue, err := channel.QueueDeclare(
		config.name,
		config.durable,
		config.autoDelete,
		config.exclusive,
		config.noWait,
		args,
	)

	failOnError(logger, err, "Failed do declare queue")

	return &queue
}

func (config *queueConfiguration) Name(name string) *queueConfiguration {
	config.name = name
	return config
}

func (config *queueConfiguration) Durable(durable bool) *queueConfiguration {
	config.durable = durable
	return config
}

func (config *queueConfiguration) AutoDelete(autoDelete bool) *queueConfiguration {
	config.autoDelete = autoDelete
	return config
}

func (config *queueConfiguration) Exclusive(exclusive bool) *queueConfiguration {
	config.exclusive = exclusive
	return config
}

func (config *queueConfiguration) NoWait(noWait bool) *queueConfiguration {
	config.noWait = noWait
	return config
}

func (config *queueConfiguration) AddArguments(args *Arguments) *queueConfiguration {
	config.arguments = args
	return config
}
