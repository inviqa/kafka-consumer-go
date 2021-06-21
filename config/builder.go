package config

type Builder struct {
	topicNameGenerator topicNameGenerator
}

func NewBuilder() *Builder {
	return &Builder{
		topicNameGenerator: defaultTopicNameGenerator,
	}
}

func (cb *Builder) SetTopicNameDecorator(tng topicNameGenerator) *Builder {
	cb.topicNameGenerator = tng
	return cb
}

func (cb *Builder) Config() (*Config, error) {
	return buildConfig(Config{
		topicNameGenerator: cb.topicNameGenerator,
	})
}
