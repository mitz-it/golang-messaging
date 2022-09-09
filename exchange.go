package messaging

import (
	logging "github.com/mitz-it/golang-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeKind int

const (
	Direct ExchangeKind = iota
	Fanout
	Topic
	Headers
)

type exchangeConfiguration struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	arguments  *Arguments
}

func (kind ExchangeKind) ToString() string {
	exchangeKinds := []string{"direct", "fanout", "topic", "headers"}
	return exchangeKinds[kind]
}

func parseKind(kind ExchangeKind) string {
	return kind.ToString()
}

func NewExchangeConfiguration() *exchangeConfiguration {
	kind := parseKind(Fanout)
	config := &exchangeConfiguration{
		name:       "",
		kind:       kind,
		durable:    defaultDurable,
		autoDelete: defaultAutoDelete,
		internal:   defaultInternal,
		noWait:     defaultNoWait,
		arguments:  nil,
	}

	return config
}

func declareExchange(logger *logging.Logger, channel *amqp.Channel, config *exchangeConfiguration) {
	if config == nil {
		return
	}

	args := toArgumentsTable(config.arguments)

	err := channel.ExchangeDeclare(
		config.name,
		config.kind,
		config.durable,
		config.autoDelete,
		config.internal,
		config.noWait,
		args,
	)

	failOnError(logger, err, "Failed to declare exchange")
}

func (config *exchangeConfiguration) Name(name string) *exchangeConfiguration {
	config.name = name
	return config
}

func (config *exchangeConfiguration) Kind(exchangeKind ExchangeKind) *exchangeConfiguration {
	kind := exchangeKind.ToString()
	config.kind = kind
	return config
}

func (config *exchangeConfiguration) Durable(durable bool) *exchangeConfiguration {
	config.durable = durable
	return config
}

func (config *exchangeConfiguration) AutoDelete(autoDelete bool) *exchangeConfiguration {
	config.autoDelete = autoDelete
	return config
}

func (config *exchangeConfiguration) Internal(internal bool) *exchangeConfiguration {
	config.internal = internal
	return config
}

func (config *exchangeConfiguration) NoWait(noWait bool) *exchangeConfiguration {
	config.noWait = noWait
	return config
}

func (config *exchangeConfiguration) AddArguments(args *Arguments) *exchangeConfiguration {
	config.arguments = args
	return config
}
