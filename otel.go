package messaging

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

func (producer *Producer) createPublishContext(ctx context.Context, config *ProducerConfiguration, message amqp.Publishing) (context.Context, map[string]interface{}) {
	tracer := otel.Tracer("amqp")

	amqpContext, span := tracer.Start(ctx, "amqp - publish")
	span.SetAttributes(attribute.KeyValue{Key: "messaging.system", Value: attribute.StringValue("rabbitmq")})
	exchange := config.getExchange()
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination", Value: attribute.StringValue(exchange)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination_kind", Value: attribute.StringValue("queue")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol", Value: attribute.StringValue("AMQP")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol_version", Value: attribute.StringValue("0.9.1")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.url", Value: attribute.StringValue(producer.connectionString)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_id", Value: attribute.StringValue(message.MessageId)})
	if message.CorrelationId == "" {
		span.SetAttributes(attribute.KeyValue{Key: "messaging.conversation_id", Value: attribute.StringValue(message.CorrelationId)})
	}
	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_payload_size_bytes", Value: attribute.IntValue(len(message.Body))})

	peer_addr := producer.connection.LocalAddr().String()
	span.SetAttributes(attribute.KeyValue{Key: "net.sock.peer.addr", Value: attribute.StringValue(peer_addr)})

	if config.routingKey != "" {
		span.SetAttributes(attribute.KeyValue{Key: "messaging.rabbitmq.routing_key", Value: attribute.StringValue(config.routingKey)})
	}

	defer span.End()

	headers := producer.InjectAMQPHeaders(amqpContext)

	return amqpContext, headers
}
