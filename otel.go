package messaging

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const consumerOperation string = "receive"
const producerOperation string = "send"
const temporaryDestination string = "(temporary)"

func (producer *Producer) createProducerContext(ctx context.Context, config *ProducerConfiguration, queue *amqp.Queue, message amqp.Publishing) (context.Context, map[string]interface{}) {
	tracer := otel.Tracer("amqp")
	destination := producer.buildProducerDestination(config, queue, message)
	spanName := producer.buildProducerSpanName(destination, producerOperation)
	producerContext, span := tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindProducer))

	span.SetAttributes(attribute.KeyValue{Key: "messaging.system", Value: attribute.StringValue("rabbitmq")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.operation", Value: attribute.StringValue(producerOperation)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination", Value: attribute.StringValue(destination)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination_kind", Value: attribute.StringValue("queue")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol", Value: attribute.StringValue("AMQP")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol_version", Value: attribute.StringValue("0.9.1")})

	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_id", Value: attribute.StringValue(message.MessageId)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_payload_size_bytes", Value: attribute.IntValue(len(message.Body))})

	if isAMQPConnectionString(producer.connectionString) {
		sanatizedConnectionString := sanatizeConnectionString(producer.connectionString)
		span.SetAttributes(attribute.KeyValue{Key: "messaging.url", Value: attribute.StringValue(sanatizedConnectionString)})

		setNetworkTags(span, producer.connectionString)
	}

	if config.routingKey != "" {
		span.SetAttributes(attribute.KeyValue{Key: "messaging.rabbitmq.routing_key", Value: attribute.StringValue(config.routingKey)})
	}

	defer span.End()

	headers := producer.InjectAMQPHeaders(producerContext)

	return producerContext, headers
}

func (producer *Producer) buildProducerDestination(config *ProducerConfiguration, queue *amqp.Queue, message amqp.Publishing) string {
	if config.QueueConfig != nil && config.QueueConfig.name != "" {
		return config.QueueConfig.name
	}

	exchange := config.getExchange()

	if exchange != emptyExchangeName {
		return exchange
	}

	return temporaryDestination
}

func (producer *Producer) buildProducerSpanName(destination, operation string) string {
	return fmt.Sprintf("%s %s", destination, operation)
}

func (consumer *Consumer) createConsumeContext(ctx context.Context, config *ConsumerConfiguration, message amqp.Delivery, key string) context.Context {
	amqpContext := consumer.ExtractAMQPHeader(context.Background(), message.Headers)
	tracer := otel.Tracer("amqp")
	destination := consumer.buildConsumerDestination(config, message, key)
	spanName := consumer.buildConsumerSpanName(destination, consumerOperation)
	consumerContext, span := tracer.Start(amqpContext, spanName, trace.WithSpanKind(trace.SpanKindConsumer))

	span.SetAttributes(attribute.KeyValue{Key: "messaging.system", Value: attribute.StringValue("rabbitmq")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.operation", Value: attribute.StringValue(consumerOperation)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination", Value: attribute.StringValue(destination)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination_kind", Value: attribute.StringValue("queue")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol", Value: attribute.StringValue("AMQP")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol_version", Value: attribute.StringValue("0.9.1")})

	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_id", Value: attribute.StringValue(message.MessageId)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_payload_size_bytes", Value: attribute.IntValue(len(message.Body))})

	if isAMQPConnectionString(consumer.connectionString) {
		sanatizedConnectionString := sanatizeConnectionString(consumer.connectionString)
		span.SetAttributes(attribute.KeyValue{Key: "messaging.url", Value: attribute.StringValue(sanatizedConnectionString)})

		setNetworkTags(span, consumer.connectionString)
	}

	if config.routingKey != "" {
		span.SetAttributes(attribute.KeyValue{Key: "messaging.rabbitmq.routing_key", Value: attribute.StringValue(config.routingKey)})
	}

	defer span.End()

	return consumerContext
}

func (consumer *Consumer) buildConsumerDestination(config *ConsumerConfiguration, message amqp.Delivery, key string) string {
	if config.QueueConfig != nil && config.QueueConfig.name != "" {
		return config.QueueConfig.name
	}

	if config.ExchangeConfig != nil && message.Exchange != emptyExchangeName {
		return message.Exchange
	}

	if key != "" {
		return key
	}

	return temporaryDestination
}

func (consumer *Consumer) buildConsumerSpanName(destination, operation string) string {
	spanName := fmt.Sprintf("%s %s", destination, operation)
	return spanName
}

func setNetworkTags(span trace.Span, connectionString string) {
	hostOrIp := hostOrIPFromConnectionString(connectionString)
	port := getPortFromConnectionString(connectionString)
	hostName := getHostname()

	setHostnameTag(hostName, span)
	setPeerPortTag(port, span)

	if match, ip := isIPAddress(hostOrIp); match {
		peerNames := getPeerNames(ip)
		peerAddresses := getPeerAddresses(peerNames[0])
		peer_name_value := joinNetworkTagValus(peerNames)
		peer_addr_value := joinNetworkTagValus(peerAddresses)

		setPeerNameTag(peer_name_value, span)
		setPeerAddressTag(peer_addr_value, span)
		return
	}

	if match, host := isIPAddress(hostOrIp); !match {
		peerAddresses := getPeerAddresses(host)
		peerNames := getPeerNames(peerAddresses[0])
		peer_name_value := joinNetworkTagValus(peerNames)
		peer_addr_value := joinNetworkTagValus(peerAddresses)

		setPeerNameTag(peer_name_value, span)
		setPeerAddressTag(peer_addr_value, span)
		return
	}
}

func setHostnameTag(hostName string, span trace.Span) {
	if hostName != "" {
		span.SetAttributes(attribute.KeyValue{Key: "net.name", Value: attribute.StringValue(hostName)})
	}
}

func setPeerPortTag(port int, span trace.Span) {
	if port != 0 {
		span.SetAttributes(attribute.KeyValue{Key: "net.sock.peer.port", Value: attribute.IntValue(port)})
	}
}

func setPeerNameTag(peer_name_value string, span trace.Span) {
	if peer_name_value != "" {
		span.SetAttributes(attribute.KeyValue{Key: "net.sock.peer.name", Value: attribute.StringValue(peer_name_value)})
	}
}

func setPeerAddressTag(peer_addr_value string, span trace.Span) {
	if peer_addr_value != "" {
		span.SetAttributes(attribute.KeyValue{Key: "net.sock.peer.addr", Value: attribute.StringValue(peer_addr_value)})
	}
}
