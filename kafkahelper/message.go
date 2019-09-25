package kafkahelper

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
)

func Message(args []string) {
	var subCmd string

	Usage := func() {
		fmt.Print(`Usage of message:

        ./kafkaHelper message <command> [arguments]

        send       produce message to topic.
        fetch      get message from offset.

`)
	}

	if len(args) < 1 {
		Usage()
		return
	} else {
		subCmd = args[0]

		if len(args) > 1 {
			args = args[1:]
		} else {
			args = []string{}
		}
	}

	switch subCmd {
	case "fetch":
		getMessage(args)
	case "send":
		pushMessage(args)
	default:
		Usage()
	}
}

func pushMessage(args []string) {
	var (
		partition               int
		topic, item, configPath string
		debug                   bool
		message                 []string
	)

	flags := flag.NewFlagSet("get message from special offset", flag.ExitOnError)

	flags.StringVar(&item, "i", "", "item")
	flags.IntVar(&partition, "p", -1, "partition id")
	flags.BoolVar(&debug, "debug", false, "print sarama logs")
	flags.StringVar(&configPath, "c", "", "config path")
	flags.StringVar(&topic, "t", "", "topic")

	cmd := "./kafkahelper message send -c Config.json -p partition [ -t topic ] [ -i item ] [-debug] <message>"
	flags.Usage = func() {
		commonUsage(flags, cmd)
	}

	if err := flags.Parse(args); err != nil || configPath == "" || partition == -1 {
		flags.Usage()
		os.Exit(0)
	}

	conf := getConfig(configPath)

	if topic == "" {
		log.Fatal("cannot item from Config file")
	}
	if len(flags.Args()) > 0 {
		message = flags.Args()
	}
	if item != "" {
		if val, ok := conf.Topics[item]; ok {
			topic = val.Topic
		}
	}
	sendMessage(conf.Broker, topic, int32(partition), message)
}

func sendMessage(broker []string, topic string, partition int32, message []string) {
	producer, err := sarama.NewSyncProducerFromClient(getClient(broker))
	noError(err)
	var producerMessage []*sarama.ProducerMessage

	for _, msg := range message {
		producerMessage = append(producerMessage, &sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Value:     sarama.StringEncoder(msg),
		})
	}

	noError(producer.SendMessages(producerMessage))
	noError(producer.Close())
}

func getMessage(args []string) {
	var partition int
	var offset int64
	var topic, item, configPath string
	var debug bool
	var limit int

	flags := flag.NewFlagSet("get message from special offset", flag.ExitOnError)

	flags.StringVar(&item, "i", "", "item")
	flags.Int64Var(&offset, "o", -3, "offset (support special offset: -1 newest, -2 oldest")
	flags.IntVar(&partition, "p", -1, "partition id")
	flags.BoolVar(&debug, "debug", false, "print sarama logs")
	flags.StringVar(&configPath, "c", "", "config path")
	flags.StringVar(&topic, "t", "", "topic")
	flags.IntVar(&limit, "limit", 1, "numbers of show message. (default: 1)")

	cmd := "./kafkahelper message fetch -c Config.json -o offset -p partition [ -t topic ] [ -i item ] [-debug]"
	flags.Usage = func() {
		commonUsage(flags, cmd)
	}

	if err := flags.Parse(args); err != nil || configPath == "" || partition == -1 || offset == -3 {
		flags.Usage()
		os.Exit(0)
	}

	conf := getConfig(configPath)

	if item != "" {
		if val, ok := conf.Topics[item]; ok {
			topic = val.Topic
		}
	}
	if topic == "" {
		log.Fatal("cannot item from Config file")
	}

	fetchMessage(conf.Broker, topic, int32(partition), offset, limit)
}

func fetchMessage(broker []string, topic string, partition int32, offset int64, limit int) {
	client := getClient(broker)
	consumer, err := sarama.NewConsumerFromClient(client)
	noError(err)

	if offset == sarama.OffsetOldest || offset == sarama.OffsetNewest {
		offset = getCommonOffset(client, topic, partition, offset)
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	noError(err)
	if limit == -1 {
		for {
			message := <-partitionConsumer.Messages()
			fmt.Println(message.Offset)
			fmt.Println(string(message.Value))
		}
	} else {
		for limit > 0 {
			message := <-partitionConsumer.Messages()
			fmt.Println(string(message.Value))
			limit--
		}
	}
	err = consumer.Close()
	noError(err)
	err = client.Close()
	noError(err)
}
