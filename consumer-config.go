package messaging

import (
	logging "github.com/mitz-it/golang-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerConfiguration struct {
	QueueConfig      *queueConfiguration
	ExchangeConfig   *exchangeConfiguration
	QosConfig        *qosConfiguration
	consumerIdentity string
	routingKey       string
	autoAck          bool
	exclusive        bool
	noLocal          bool
	noWait           bool
	arguments        *Arguments
}

type ConfigureConsumer func(config *ConsumerConfiguration)

func newConsumerConfiguration() *ConsumerConfiguration {
	return &ConsumerConfiguration{
		ExchangeConfig:   nil,
		QueueConfig:      NewQueueConfiguration(),
		QosConfig:        NewQosConfiguration(),
		consumerIdentity: defaultConsumerIdentity,
		routingKey:       defaultRoutingKey,
		autoAck:          defaultAutoAck,
		exclusive:        defaultExclusive,
		noLocal:          defaultNoLocal,
		noWait:           defaultNoWait,
		arguments:        nil,
	}
}

func configureConsumer(configure ConfigureConsumer) *ConsumerConfiguration {
	config := newConsumerConfiguration()

	configure(config)

	return config
}

func (config *ConsumerConfiguration) getKey(queue *amqp.Queue) string {
	if queue == nil {
		return config.routingKey
	}

	return queue.Name
}

func (config *ConsumerConfiguration) bindQueueToExchange(channel *amqp.Channel, queue *amqp.Queue, args amqp.Table) {
	if config.ExchangeConfig == nil || config.QueueConfig == nil {
		return
	}

	channel.QueueBind(
		queue.Name,
		config.routingKey,
		config.ExchangeConfig.name,
		config.ExchangeConfig.noWait,
		args,
	)
}

func (config *ConsumerConfiguration) configureQoS(channel *amqp.Channel, logger *logging.Logger) {
	if config.QosConfig == nil {
		return
	}

	err := channel.Qos(
		config.QosConfig.prefetchCount,
		config.QosConfig.prefetchSize,
		config.QosConfig.global,
	)

	failOnError(logger, err, "Failed to set QoS")
}

func (config *ConsumerConfiguration) ConsumerIdentity(identity string) *ConsumerConfiguration {
	config.consumerIdentity = identity
	return config
}

func (config *ConsumerConfiguration) RoutingKey(routingKey string) *ConsumerConfiguration {
	config.routingKey = routingKey
	return config
}

func (config *ConsumerConfiguration) AutoAck(autoAck bool) *ConsumerConfiguration {
	config.autoAck = autoAck
	return config
}

func (config *ConsumerConfiguration) Exclusive(exclusive bool) *ConsumerConfiguration {
	config.exclusive = exclusive
	return config
}

func (config *ConsumerConfiguration) NoLocal(noLocal bool) *ConsumerConfiguration {
	config.noLocal = noLocal
	return config
}

func (config *ConsumerConfiguration) NoWait(noWait bool) *ConsumerConfiguration {
	config.noWait = noWait
	return config
}

func (config *ConsumerConfiguration) AddArguments(args *Arguments) *ConsumerConfiguration {
	config.arguments = args
	return config
}
