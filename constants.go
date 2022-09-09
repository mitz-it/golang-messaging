package messaging

import "time"

const defaultDurable bool = false
const defaultExclusive bool = false
const defaultNoWait bool = false
const defaultNoLocal bool = false
const defaultAutoDelete bool = false
const defaultInternal bool = false
const defaultMandatory bool = false
const defaultImmediate bool = false
const defaultConsumerIdentity string = ""
const defaultAutoAck bool = true
const defaultRoutingKey string = ""
const defaultPrefetchCount int = 1
const defaultPrefetchSize int = 0
const defaultGlobalQos bool = false

const emptyExchangeName string = ""

const defaultContextTimeOut time.Duration = 30

type ContentType string

const ApplicationJson ContentType = "application/json"
const TextPlain ContentType = "text/plain"
