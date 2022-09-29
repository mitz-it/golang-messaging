package messaging

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

func (producer *Producer) createPublishContext(ctx context.Context, config *ProducerConfiguration, queue *amqp.Queue, message amqp.Publishing) (context.Context, map[string]interface{}) {
	tracer := otel.Tracer("amqp")

	amqpContext, span := tracer.Start(ctx, "amqp - publish")
	span.SetAttributes(attribute.KeyValue{Key: "messaging.system", Value: attribute.StringValue("rabbitmq")})
	destination := config.getExchange()
	if destination == emptyExchangeName {
		destination = queue.Name
	}
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination", Value: attribute.StringValue(destination)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.destination_kind", Value: attribute.StringValue("queue")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol", Value: attribute.StringValue("AMQP")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.protocol_version", Value: attribute.StringValue("0.9.1")})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.url", Value: attribute.StringValue(producer.connectionString)})
	span.SetAttributes(attribute.KeyValue{Key: "messaging.message_id", Value: attribute.StringValue(message.MessageId)})

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
