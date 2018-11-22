package api

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"regexp"
	"sync"
)

func InitKafkaCopy(brokers, dstbrokers []string) (consumer sarama.Consumer, producer sarama.AsyncProducer, err error) {
	//consumer
	consumer, err = InitKafkaConsumer(brokers)
	if err != nil {
		return
	}

	//producer
	producer, err = InitKafkaProducer(dstbrokers)
	if err != nil {
		return
	}

	return
}

func StartTopicCopy(consumer sarama.Consumer, producer sarama.AsyncProducer, srcTopic, destTopic string, ifPrint bool, keyFilterReg *regexp.Regexp, begin bool) error {
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
		// all partitions are handled
		wg.Add(1)

		pconsumer, err := consumer.ConsumePartition(srcTopic, pid, offsettype)
		if err != nil {
			log.Fatalf("Consume %s partition %d err: %v\n", srcTopic, pid, err)
			return nil
		}

		go copyMessages(wg, exitchan, pid, producer, srcTopic, destTopic, ifPrint, keyFilterReg, pconsumer)
	}

	wg.Wait()
	return nil
}

func copyMessages(wg *sync.WaitGroup, exitchan <-chan bool, pid int32, producer sarama.AsyncProducer, srcTopic, destTopic string, ifPrint bool, keyFilterReg *regexp.Regexp, pconsumer sarama.PartitionConsumer) {
	defer wg.Done()

	var consumed, enqueued, offset int64
	running := true

	for running {
		select {
		case msg := <-pconsumer.Messages():
			if keyFilterReg == nil || keyFilterReg.MatchString(string(msg.Key)) {
				if ifPrint {
					key := decodeJsonKey(msg.Key)
					body := string(msg.Value)
					fmt.Printf("[%d %5d] %s %s %v\n", pid, msg.Offset, body, timeStr(msg.Timestamp), key)
				}

				producer.Input() <- &sarama.ProducerMessage{
					Topic: destTopic,
					Key:   sarama.ByteEncoder(msg.Key),
					Value: sarama.ByteEncoder(msg.Value),
					// Headers: msg.Headers, -- not yet handled
				}
				enqueued++
			}

			offset = msg.Offset
			consumed++
		case <-exitchan:
			running = false
		}
	}

	log.Printf("Consumed %d %s messages from partition %d; sent %d message to %s. The offset is now %d.\n",
		consumed, srcTopic, pid,
		enqueued, destTopic, offset)
}
