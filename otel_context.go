package messaging

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const otel_tracer_name string = "amqp"

const consumerOperation string = "receive"
const producerOperation string = "send"
const temporaryDestination string = "(temporary)"

func (producer *Producer) createProducerContext(ctx context.Context, config *ProducerConfiguration, queue *amqp.Queue, message amqp.Publishing) (context.Context, map[string]interface{}) {
	tracer := otel.Tracer(otel_tracer_name)
	destination := producer.buildProducerDestination(config, queue, message)
	spanName := producer.buildProducerSpanName(destination, producerOperation)
	producerContext, span := tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindProducer))

	payloadSize := len(message.Body)

	setMessagingTags(span, destination, producerOperation, producer.connectionString, message.MessageId, config.routingKey, payloadSize)

	setNetTags(span, producer.connectionString)

	defer span.End()

	headers := producer.InjectAMQPHeaders(producerContext)

	return producerContext, headers
}

func (consumer *Consumer) createConsumeContext(ctx context.Context, config *ConsumerConfiguration, message amqp.Delivery, key string) context.Context {
	amqpContext := consumer.ExtractAMQPHeader(context.Background(), message.Headers)

	tracer := otel.Tracer(otel_tracer_name)
	destination := consumer.buildConsumerDestination(config, message, key)
	spanName := consumer.buildConsumerSpanName(destination, consumerOperation)
	consumerContext, span := tracer.Start(amqpContext, spanName, trace.WithSpanKind(trace.SpanKindConsumer))

	payloadSize := len(message.Body)

	setMessagingTags(span, destination, consumerOperation, consumer.connectionString, message.MessageId, config.routingKey, payloadSize)

	setNetTags(span, consumer.connectionString)

	defer span.End()

	return consumerContext
}
