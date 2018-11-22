package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"regexp"
	"sync"
	"time"
)

func InitKafkaConsumer(brokers []string) (consumer sarama.Consumer, err error) {
	return sarama.NewConsumer(brokers, nil)
}

func StartConsumer(consumer sarama.Consumer, srcTopic string, wantedPartition int, printMessages bool, keyFilterReg *regexp.Regexp, begin bool) error {
	//partitions
	partitions, err := consumer.Partitions(srcTopic)
	if err != nil {
		return err
	}

	//exit signal
	exitchan := getSignalChan(len(partitions))

	//offset
	var offsettype int64
	if begin {
		offsettype = sarama.OffsetOldest
	} else {
		offsettype = sarama.OffsetNewest
	}

	wg := &sync.WaitGroup{}

	for _, pid := range partitions {
		if wantedPartition < 0 || pid == int32(wantedPartition) {
			wg.Add(1)

			partitionConsumer, err := consumer.ConsumePartition(srcTopic, pid, offsettype)
			if err != nil {
				log.Fatalf("Consume %s partition %d err: %v\n", srcTopic, pid, err)
				return nil
			}

			go consumePartition(wg, exitchan, pid, srcTopic, printMessages, keyFilterReg, partitionConsumer)
		}
	}

	wg.Wait()
	return nil
}

func consumePartition(wg *sync.WaitGroup, exitchan <-chan bool, pid int32, topic string, printMessages bool, keyFilterReg *regexp.Regexp, partitionConsumer sarama.PartitionConsumer) {
	defer wg.Done()

	var consumed int64
	var offset int64
	running := true

	for running {
		select {
		case msg := <-partitionConsumer.Messages():
			if keyFilterReg == nil || keyFilterReg.MatchString(string(msg.Key)) {
				if printMessages {
					key := decodeJsonKey(msg.Key)
					body := string(msg.Value)
					fmt.Printf("[%d %5d] %s %s %v\n", pid, msg.Offset, body, timeStr(msg.Timestamp), key)
				}
			}

			offset = msg.Offset
			consumed++
		case <-exitchan:
			running = false
		}
	}

	log.Printf("Consumed %d %s messages from partition %d. The offset is now %d.\n", consumed, topic, pid, offset)
}

func timeStr(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02T15:04:05")
}

func decodeJsonKey(kb []byte) string {
	key := make(map[string]interface{})
	err := json.NewDecoder(bytes.NewBuffer(kb)).Decode(&key)
	if err != nil {
		return string(kb)
	}
	// use Sprintf to get a terse form, and then remove leading "map"
	return fmt.Sprintf("%v", key)[3:]
}
