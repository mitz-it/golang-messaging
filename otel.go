package messaging

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (producer *Producer) createPublishContext(ctx context.Context, config *ProducerConfiguration, queue *amqp.Queue, message amqp.Publishing) (context.Context, map[string]interface{}) {
	tracer := otel.Tracer("amqp")

	amqpContext, span := tracer.Start(ctx, "amqp - produce", trace.WithSpanKind(trace.SpanKindProducer))

	destination := config.getExchange()

	if destination == emptyExchangeName {
		destination = queue.Name
	}

	peer_addr := producer.connection.LocalAddr().String()

	span.SetAttributes(attribute.KeyValue{Key: "messaging.system", Value: attribute.StringValue("rabbitmq")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination", Value: attribute.StringValue(destination)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination_kind", Value: attribute.StringValue("queue")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol", Value: attribute.StringValue("AMQP")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol_version", Value: attribute.StringValue("0.9.1")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.url", Value: attribute.StringValue(producer.connectionString)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_id", Value: attribute.StringValue(message.MessageId)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_payload_size_bytes", Value: attribute.IntValue(len(message.Body))})
	span.SetAttributes(attribute.KeyValue{Key: "net.sock.peer.addr", Value: attribute.StringValue(peer_addr)})

	if config.routingKey != "" {
		span.SetAttributes(attribute.KeyValue{Key: "messaging.rabbitmq.routing_key", Value: attribute.StringValue(config.routingKey)})
	}

	defer span.End()

	headers := producer.InjectAMQPHeaders(amqpContext)

	return amqpContext, headers
}

func (consumer *Consumer) createConsumeContext(ctx context.Context, config *ConsumerConfiguration, message amqp.Delivery, key string) context.Context {
	amqpContext := consumer.ExtractAMQPHeader(context.Background(), message.Headers)
	tracer := otel.Tracer("amqp")
	consumerContext, span := tracer.Start(amqpContext, "amqp - consume", trace.WithSpanKind(trace.SpanKindConsumer))

	destination := key

	if destination == emptyExchangeName {
		destination = message.Exchange
	}

	peer_addr := consumer.connection.LocalAddr().String()

	span.SetAttributes(attribute.KeyValue{Key: "messaging.system", Value: attribute.StringValue("rabbitmq")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination", Value: attribute.StringValue(destination)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination_kind", Value: attribute.StringValue("queue")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol", Value: attribute.StringValue("AMQP")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol_version", Value: attribute.StringValue("0.9.1")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.url", Value: attribute.StringValue(consumer.connectionString)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_id", Value: attribute.StringValue(message.MessageId)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_payload_size_bytes", Value: attribute.IntValue(len(message.Body))})
	span.SetAttributes(attribute.KeyValue{Key: "net.sock.peer.addr", Value: attribute.StringValue(peer_addr)})

	if config.routingKey != "" {
		span.SetAttributes(attribute.KeyValue{Key: "messaging.rabbitmq.routing_key", Value: attribute.StringValue(config.routingKey)})
	}
	defer span.End()

	return consumerContext
}
