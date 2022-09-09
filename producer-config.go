package messaging

import (
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ProducerConfiguration struct {
	ExchangeConfig *exchangeConfiguration
	QueueConfig    *queueConfiguration
	routingKey     string
	contentType    ContentType
	mandatory      bool
	immediate      bool
	timeOut        time.Duration
}

type ConfigureProducer func(config *ProducerConfiguration)

func newProducerConfiguration() *ProducerConfiguration {
	return &ProducerConfiguration{
		ExchangeConfig: nil,
		QueueConfig:    nil,
		contentType:    ApplicationJson,
		mandatory:      defaultMandatory,
		immediate:      defaultImmediate,
		routingKey:     defaultRoutingKey,
		timeOut:        defaultContextTimeOut,
	}
}

func configureProducer(configure ConfigureProducer, message any) *ProducerConfiguration {
	config := newProducerConfiguration()

	configure(config)

	return config
}

func (config *ProducerConfiguration) getKey(queue *amqp.Queue) string {
	if queue == nil {
		return config.routingKey
	}

	if config.ExchangeConfig == nil && !strings.HasPrefix(queue.Name, "amqp") {
		return queue.Name
	}

	if config.ExchangeConfig == nil && strings.HasPrefix(queue.Name, "amqp") {
		return config.routingKey
	}

	return queue.Name
}

func (config *ProducerConfiguration) getExchange() string {
	if config.ExchangeConfig == nil {
		return emptyExchangeName
	}
	return config.ExchangeConfig.name
}

func (config *ProducerConfiguration) bindQueueToExchange(channel *amqp.Channel, queue *amqp.Queue, args amqp.Table) {
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

func (config *ProducerConfiguration) RoutingKey(routingKey string) *ProducerConfiguration {
	config.routingKey = routingKey
	return config
}

func (config *ProducerConfiguration) ContentType(contentType ContentType) *ProducerConfiguration {
	config.contentType = contentType
	return config
}

func (config *ProducerConfiguration) Mandatory(mandatory bool) *ProducerConfiguration {
	config.mandatory = mandatory
	return config
}

func (config *ProducerConfiguration) Immediate(immediate bool) *ProducerConfiguration {
	config.immediate = immediate
	return config
}

func (config *ProducerConfiguration) Timeout(timeout time.Duration) *ProducerConfiguration {
	config.timeOut = timeout
	return config
}
