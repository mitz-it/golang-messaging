package messaging

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

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

func (consumer *Consumer) buildConsumerSpanName(destination, operation string) string {
	spanName := fmt.Sprintf("%s %s", destination, operation)
	return spanName
}

func setMessagingTags(span trace.Span, destination, operation, connectionString, messageId, routingKey string, payloadSize int) {
	span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_system_key), Value: attribute.StringValue(tag_messaging_system_value)})
	span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_operation_key), Value: attribute.StringValue(operation)})
	span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_destination_key), Value: attribute.StringValue(destination)})
	span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_destination_kind_key), Value: attribute.StringValue(tag_messaging_destination_kind_value)})
	span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_protocol_key), Value: attribute.StringValue(tag_messaging_protocol_value)})
	span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_protocol_version_key), Value: attribute.StringValue(tag_messaging_protocol_version_value)})

	span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_message_id_key), Value: attribute.StringValue(messageId)})
	span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_message_payload_size_bytes_key), Value: attribute.IntValue(payloadSize)})

	if isAMQPConnectionString(connectionString) {
		sanatizedConnectionString := sanatizeConnectionString(connectionString)
		span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_url_key), Value: attribute.StringValue(sanatizedConnectionString)})
	}

	if routingKey != "" {
		span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_messaging_rabbitmq_routing_key_key), Value: attribute.StringValue(routingKey)})
	}
}

func setNetTags(span trace.Span, connectionString string) {

	if !isAMQPConnectionString(connectionString) {
		return
	}

	port := getPortFromConnectionString(connectionString)
	setPeerPortTag(port, span)

	span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_net_sock_family_key), Value: attribute.StringValue(tag_net_sock_family_value)})

	hostOrIp := hostOrIPFromConnectionString(connectionString)

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

func setPeerPortTag(port int, span trace.Span) {
	if port != 0 {
		span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_net_sock_peer_port_key), Value: attribute.IntValue(port)})
	}
}

func setPeerNameTag(peer_name_value string, span trace.Span) {
	if peer_name_value != "" {
		span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_net_sock_peer_name_key), Value: attribute.StringValue(peer_name_value)})
	}
}

func setPeerAddressTag(peer_addr_value string, span trace.Span) {
	if peer_addr_value != "" {
		span.SetAttributes(attribute.KeyValue{Key: attribute.Key(tag_net_sock_peer_addr_key), Value: attribute.StringValue(peer_addr_value)})
	}
}
