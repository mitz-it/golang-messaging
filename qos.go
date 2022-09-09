package messaging

type qosConfiguration struct {
	prefetchCount int
	prefetchSize  int
	global        bool
}

func NewQosConfiguration() *qosConfiguration {
	return &qosConfiguration{
		prefetchCount: defaultPrefetchCount,
		prefetchSize:  defaultPrefetchSize,
		global:        defaultGlobalQos,
	}
}

func (config *qosConfiguration) PrefetchCount(count int) *qosConfiguration {
	config.prefetchCount = count
	return config
}

func (config *qosConfiguration) PrefetchSize(size int) *qosConfiguration {
	config.prefetchCount = size
	return config
}

func (config *qosConfiguration) Global(global bool) *qosConfiguration {
	config.global = global
	return config
}
