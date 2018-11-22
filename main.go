package main

import (
	"flag"
	"github.com/rickb777/kafka-tool/api"
	"log"
	"os"
	"strings"
)

var (
	command, host, remote, filter, group *string
	print, begin                         *bool
	partition                            *int
)

func init() {
	command = flag.String("cmd", "consumer", `command: one of consumer, copy, offset, resetOffset`)
	host = flag.String("host", "", "brokerlist/topic (brokerlist can be comma-separated)")
	remote = flag.String("remote", "", `brokerlist/topic, this is the target when using "copy"`)
	filter = flag.String("filter", "", "show only messages with keys that match this regex (optional)")
	group = flag.String("group", "", "the consumer group (optional)")
	print = flag.Bool("print", false, `enable verbose output (during "copy")`)
	begin = flag.Bool("begin", false, `consume from the beginning`)
	partition = flag.Int("partition", -1, `required partition number`)
}

func main() {
	log.SetOutput(os.Stderr)
	flag.Parse()

	kafkaTool := &api.KafkaTool{
		Command:       *command,
		PrintMessages: *print,
		Begin:         *begin,
		Group:         *group,
		Partition:     *partition,
		KeyFilter:     *filter,
	}

	//host
	brokers_topic := strings.SplitN(*host, "/", 2)
	if len(brokers_topic) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	kafkaTool.Brokers = strings.Split(brokers_topic[0], ",")
	if len(brokers_topic) == 2 {
		kafkaTool.Topic = brokers_topic[1]
	}

	//remote
	if *remote != "" {
		dstbrokers_topic := strings.SplitN(*remote, "/", 2)
		if len(dstbrokers_topic) != 2 {
			flag.Usage()
			os.Exit(1)
		}
		kafkaTool.DstBrokers = strings.Split(dstbrokers_topic[0], ",")
		kafkaTool.DstTopic = dstbrokers_topic[1]
	}

	err := kafkaTool.Start()
	if err != nil {
		log.Fatalln(err)
	}
}
