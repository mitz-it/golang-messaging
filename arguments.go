package messaging

import amqp "github.com/rabbitmq/amqp091-go"

type Arguments map[string]interface{}

func (config *ConsumerConfiguration) toArgumentsTable() amqp.Table {
	return toArgumentsTable(config.arguments)
}

func (config *ProducerConfiguration) toArgumentsTable() amqp.Table {
	if config.QueueConfig == nil {
		return nil
	}

	return toArgumentsTable(config.QueueConfig.arguments)
}

func toArgumentsTable(arguments *Arguments) amqp.Table {
	if arguments != nil {
		args := amqp.Table(*arguments)
		return args
	} else {
		return nil
	}
}
