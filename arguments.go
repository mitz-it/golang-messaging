package messaging

import amqp "github.com/rabbitmq/amqp091-go"

type Arguments map[string]interface{}

func toArgumentsTable(arguments *Arguments) amqp.Table {
	if arguments != nil {
		args := amqp.Table(*arguments)
		return args
	} else {
		return nil
	}
}

func (config *ConsumerConfiguration) toArgumentsTable() amqp.Table {
	if config.arguments != nil {
		args := amqp.Table(*config.arguments)
		return args
	} else {
		return nil
	}
}

func (config *ProducerConfiguration) toArgumentsTable() amqp.Table {
	if config.QueueConfig == nil {
		return nil
	}

	if config.QueueConfig.arguments != nil {
		args := amqp.Table(*config.QueueConfig.arguments)
		return args
	} else {
		return nil
	}
}
