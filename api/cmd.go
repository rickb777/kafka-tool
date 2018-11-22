package api

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"regexp"
)

const (
	CMD_CONSUMER    = "consumer"
	CMD_COPY        = "copy"
	CMD_RESETOFFSET = "resetOffset"
	CMD_OFFSET      = "offset"
)

var (
	ERR_MISSING_HOST   = errors.New(`brokerlist or topic is empty, please set it with the "host" parameter`)
	ERR_MISSING_REMOTE = errors.New(`dstbrokerlist or dsttopic is empty, please set it with the "remote" parameter`)
	ERR_MISSING_GROUP  = errors.New(`group is empty, please set it with the "group" parameter`)
)

type KafkaTool struct {
	Command       string
	PrintMessages bool
	Begin         bool
	Brokers       []string
	DstBrokers    []string
	Group         string
	Topic         string
	DstTopic      string
	Partition     int
	KeyFilter     string

	keyFilterReg *regexp.Regexp
	consumer     sarama.Consumer
	producer     sarama.AsyncProducer
}

func (k *KafkaTool) Init() (err error) {
	if k.KeyFilter != "" {
		k.keyFilterReg, err = regexp.Compile(k.KeyFilter)
		if err != nil {
			return
		}
	}
	return
}

func (k *KafkaTool) Start() (err error) {
	err = k.Init()
	if err != nil {
		return
	}

	switch k.Command {
	case CMD_CONSUMER:
		err = k.StartKafkaConsumer()
	case CMD_COPY:
		err = k.StartTopicCopy()
	case CMD_RESETOFFSET:
		err = k.SetOffsetToNewest()
	case CMD_OFFSET:
		err = k.GetOffset()
	default:
		err = errors.New("unknown command: " + k.Command)
	}

	return
}

func (k *KafkaTool) StartKafkaConsumer() (err error) {
	//check
	if len(k.Brokers) == 0 || k.Topic == "" {
		return ERR_MISSING_HOST
	}

	k.consumer, err = InitKafkaConsumer(k.Brokers)
	if err != nil {
		return
	}
	defer k.consumer.Close()

	return StartConsumer(k.consumer, k.Topic, k.Partition, true, k.keyFilterReg, k.Begin)
}

func (k *KafkaTool) StartTopicCopy() (err error) {
	//check
	if len(k.Brokers) == 0 || k.Topic == "" {
		return ERR_MISSING_HOST
	}

	if len(k.DstBrokers) == 0 || k.DstTopic == "" {
		return ERR_MISSING_REMOTE
	}

	//init
	k.consumer, k.producer, err = InitKafkaCopy(k.Brokers, k.DstBrokers)
	if err != nil {
		return
	}
	defer k.consumer.Close()
	defer k.producer.Close()

	//start
	err = StartTopicCopy(k.consumer, k.producer, k.Topic, k.DstTopic, k.PrintMessages, k.keyFilterReg, k.Begin)
	if err != nil {
		return
	}

	return
}

func (k *KafkaTool) SetOffsetToNewest() error {
	if len(k.Brokers) == 0 || k.Topic == "" {
		return ERR_MISSING_HOST
	}

	if k.Group == "" {
		return ERR_MISSING_GROUP
	}

	return SetOffsetToNewest(k.Brokers, k.Group, k.Topic)
}

func (k *KafkaTool) getNewestOffset() error {
	if len(k.Brokers) == 0 || k.Topic == "" {
		return ERR_MISSING_HOST
	}

	offsets, err := GetNewestOffset(k.Brokers, k.Topic)
	if err != nil {
		return err
	}

	fmt.Printf("TOPIC                PARTITION OFFSET\n")
	for pid, offset := range offsets {
		fmt.Printf("%-20s %-9d %d\n", k.Topic, pid, offset)
	}

	return nil
}

func (k *KafkaTool) getGroupOffset() error {
	if k.Group == "" {
		return k.getNewestOffset()
	}

	if len(k.Brokers) == 0 {
		return ERR_MISSING_HOST
	}

	offsets, err := GetGroupOffset(k.Brokers, k.Group)
	if err != nil {
		return err
	}

	fmt.Printf("GROUP        TOPIC                PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG   OWNER\n")
	for topic, partitions := range offsets {
		newestOffsets, err := GetNewestOffset(k.Brokers, topic)
		if err != nil {
			return err
		}

		for id, newestOffset := range newestOffsets {
			pid := int32(id)
			currentOffset := partitions[pid].Offset
			lag := newestOffset - currentOffset

			owner := partitions[pid].Metadata
			if owner == "" {
				owner = "-"
			}

			fmt.Printf("%-12s %-20s %-9d %-14d %-14d %-5d %s\n", k.Group, topic, pid, currentOffset, newestOffset, lag, owner)
		}
	}

	return nil
}

func (k *KafkaTool) GetOffset() error {
	if k.Group == "" {
		return k.getNewestOffset()
	}
	return k.getGroupOffset()
}
