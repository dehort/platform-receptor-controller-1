package queue

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

const (
	ENV_PREFIX = "RECEPTOR_CONTROLLER"

	BROKERS = "Kafka_Brokers"

	JOBS_TOPIC           = "Kafka_Jobs_Topic"
	JOBS_GROUP_ID        = "Kafka_Jobs_Group_Id"
	JOBS_CONSUMER_OFFSET = "Kafka_Jobs_Consumer_Offset"

	RESPONSES_TOPIC = "Kafka_Responses_Topic"

	DEFAULT_BROKER_ADDRESS = "kafka:29092"

	BATCH_SIZE    = "Kafka_Producer_Batch_Size"
	BATCH_TIMEOUT = "Kafka_Producer_Batch_Timeout"
)

type ConsumerConfig struct {
	Brokers        []string
	Topic          string
	GroupID        string
	ConsumerOffset int64
}

func (cc *ConsumerConfig) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s\n", BROKERS, cc.Brokers)
	fmt.Fprintf(&b, "%s: %s\n", JOBS_TOPIC, cc.Topic)
	fmt.Fprintf(&b, "%s: %s\n", JOBS_GROUP_ID, cc.GroupID)
	fmt.Fprintf(&b, "%s: %d", JOBS_CONSUMER_OFFSET, cc.ConsumerOffset)
	return b.String()
}

type ProducerConfig struct {
	Brokers      []string
	Topic        string
	BatchSize    int
	BatchTimeout time.Duration
}

func (pc *ProducerConfig) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s\n", BROKERS, pc.Brokers)
	fmt.Fprintf(&b, "%s: %s\n", RESPONSES_TOPIC, pc.Topic)
	fmt.Fprintf(&b, "%s: %d\n", BATCH_SIZE, pc.BatchSize)
	fmt.Fprintf(&b, "%s: %s", BATCH_TIMEOUT, pc.BatchTimeout)
	return b.String()
}

func GetConsumer() *ConsumerConfig {
	options := viper.New()
	options.SetDefault(BROKERS, []string{DEFAULT_BROKER_ADDRESS})
	options.SetDefault(JOBS_TOPIC, "platform.receptor-controller.jobs")
	options.SetDefault(JOBS_GROUP_ID, "receptor-controller")
	options.SetDefault(JOBS_CONSUMER_OFFSET, -1)
	options.SetEnvPrefix(ENV_PREFIX)
	options.AutomaticEnv()

	return &ConsumerConfig{
		Brokers:        options.GetStringSlice(BROKERS),
		Topic:          options.GetString(JOBS_TOPIC),
		GroupID:        options.GetString(JOBS_GROUP_ID),
		ConsumerOffset: options.GetInt64(JOBS_CONSUMER_OFFSET),
	}
}

func GetProducer() *ProducerConfig {
	options := viper.New()
	options.SetDefault(BROKERS, []string{DEFAULT_BROKER_ADDRESS})
	options.SetDefault(RESPONSES_TOPIC, "platform.receptor-controller.responses")
	options.SetDefault(BATCH_SIZE, 0)
	options.SetDefault(BATCH_TIMEOUT, 0)
	options.SetEnvPrefix(ENV_PREFIX)
	options.AutomaticEnv()

	return &ProducerConfig{
		Brokers:      options.GetStringSlice(BROKERS),
		Topic:        options.GetString(RESPONSES_TOPIC),
		BatchSize:    options.GetInt(BATCH_SIZE),
		BatchTimeout: options.GetDuration(BATCH_TIMEOUT) * time.Second,
	}
}
