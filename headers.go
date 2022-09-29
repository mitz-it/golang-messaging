package messaging

import (
	"context"

	"go.opentelemetry.io/otel"
)

type AmqpHeadersCarrier map[string]interface{}

func (carrier AmqpHeadersCarrier) Get(key string) string {
	value, ok := carrier[key]
	if !ok {
		return ""
	}
	return value.(string)
}

func (carrier AmqpHeadersCarrier) Set(key string, value string) {
	carrier[key] = value
}

func (carrier AmqpHeadersCarrier) Keys() []string {
	index := 0
	keys := make([]string, len(carrier))

	for key, _ := range carrier {
		keys[index] = key
		index++
	}

	return keys
}

func (producer Producer) InjectAMQPHeaders(ctx context.Context) map[string]interface{} {
	carrier := make(AmqpHeadersCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

func (consumer Consumer) ExtractAMQPHeader(ctx context.Context, headers map[string]interface{}) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, AmqpHeadersCarrier(headers))
}
