package config

import "time"

type Builder struct {
	kafkaHost           []string
	kafkaGroup          string
	sourceTopics        []string
	retryIntervals      []int
	dBHost              string
	dBPort              int
	dBSchema            string
	dBUser              string
	dBPass              string
	dBDriver            string
	useDbForRetries     bool
	maintenanceInterval time.Duration
	topicNameGenerator  topicNameGenerator
	tlsEnable           bool
	tlsSkipVerifyPeer   bool
}

func NewBuilder() *Builder {
	return &Builder{
		topicNameGenerator:  defaultTopicNameGenerator,
		maintenanceInterval: defaultMaintenanceInterval,
		dBPort:              5432,
		dBDriver:            "postgres",
	}
}

func (cb *Builder) SetKafkaHost(host []string) *Builder {
	cb.kafkaHost = host
	return cb
}

func (cb *Builder) SetKafkaGroup(group string) *Builder {
	cb.kafkaGroup = group
	return cb
}

func (cb *Builder) SetSourceTopics(topics []string) *Builder {
	cb.sourceTopics = topics
	return cb
}

func (cb *Builder) SetRetryIntervals(intervals []int) *Builder {
	cb.retryIntervals = intervals
	return cb
}

func (cb *Builder) SetDBHost(host string) *Builder {
	cb.dBHost = host
	return cb
}

func (cb *Builder) SetDBPort(port int) *Builder {
	cb.dBPort = port
	return cb
}

func (cb *Builder) SetDBSchema(schema string) *Builder {
	cb.dBSchema = schema
	return cb
}

func (cb *Builder) SetDBUser(user string) *Builder {
	cb.dBUser = user
	return cb
}

func (cb *Builder) SetDBPass(pass string) *Builder {
	cb.dBPass = pass
	return cb
}

func (cb *Builder) UseDbForRetries(useDbForRetries bool) *Builder {
	cb.useDbForRetries = useDbForRetries
	return cb
}

func (cb *Builder) SetMaintenanceInterval(interval time.Duration) *Builder {
	cb.maintenanceInterval = interval
	return cb
}

func (cb *Builder) EnableTLS(tlsEnable bool) *Builder {
	cb.tlsEnable = tlsEnable
	return cb
}

func (cb *Builder) SkipTLSVerifyPeer(tlsSkipVerifyPeer bool) *Builder {
	cb.tlsSkipVerifyPeer = tlsSkipVerifyPeer
	return cb
}

func (cb *Builder) SetTopicNameGenerator(tng topicNameGenerator) *Builder {
	cb.topicNameGenerator = tng
	return cb
}

func (cb *Builder) Config() (*Config, error) {
	c := &Config{
		services: map[string]interface{}{},
	}
	if err := c.loadFromBuilder(cb); err != nil {
		return nil, err
	}

	return c, nil
}
