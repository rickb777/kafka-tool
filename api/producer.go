package api

import (
	"github.com/Shopify/sarama"
)

func InitKafkaProducer(brokers []string) (producer sarama.AsyncProducer, err error) {
	return sarama.NewAsyncProducer(brokers, nil)
}
