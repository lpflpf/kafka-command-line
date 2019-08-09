package kafkahelper

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/qiniu/log"
	"os"
	"strconv"
	"strings"
)

type OffsetManager struct {
	client sarama.Client
	broker []string
	topic  string
	group  string
}

func getCommonOffset(client sarama.Client, topic string, partition int32, time int64) int64 {
	offset, err := client.GetOffset(topic, partition, time)
	noError(err)
	return offset
}

func (m *OffsetManager) getAllOffset() {
	var newest, oldest int64
	allPartition, _ := m.client.Partitions(m.topic)
	resp, err := getAdmin(m.broker).ListConsumerGroupOffsets(m.group, map[string][]int32{m.topic: allPartition})
	if err != nil {
		log.Error(err)
	}
	sepLine := fmt.Sprintf("+%s+%s+%s+%s+\n", strings.Repeat("-", 12), strings.Repeat("-", 12), strings.Repeat("-", 12), strings.Repeat("-", 12))
	fmt.Print(sepLine)
	fmt.Printf("|%-12s|%-12s|%-12s|%-12s|\n", "index", "oldest", "newest", "current")
	fmt.Print(sepLine)
	for _, partition := range allPartition {
		newest = getCommonOffset(m.client, m.topic, partition, sarama.OffsetNewest)
		oldest = getCommonOffset(m.client, m.topic, partition, sarama.OffsetOldest)
		fmt.Printf("|%-12d|%-12d|%-12d|%-12d|\n", partition, oldest, newest, resp.GetBlock(m.topic, partition).Offset)
	}
	fmt.Print(sepLine)
}

func (m *OffsetManager) setPartitionOffset(partition int32, newOffset int64) {
	var offset int64
	var err error
	if newOffset == sarama.OffsetOldest || newOffset == sarama.OffsetNewest {
		offset, err = m.client.GetOffset(m.topic, partition, newOffset)
		noError(err)
	} else {
		offset = newOffset
	}
	offsetManager, err := sarama.NewOffsetManagerFromClient(m.group, m.client)
	noError(err)
	partitions, err := m.client.Partitions(m.topic)
	noError(err)

	// if partition not in topic, echo failed.
	if len(partitions)-1 < int(partition) {
		var strPartitions []string
		for _, partition := range partitions {
			strPartitions = append(strPartitions, strconv.Itoa(int(partition)))
		}
		log.Fatalf("topic %s partition is [%s], please check arguments!\n", m.topic, strings.Join(strPartitions, ","))
	}

	partitionOffsetManager, err := offsetManager.ManagePartition(m.topic, partition)
	noError(err)
	currentOffset, _ := partitionOffsetManager.NextOffset()

	// select a method to update offset
	if offset <= currentOffset {
		partitionOffsetManager.ResetOffset(offset, "")
	} else {
		partitionOffsetManager.MarkOffset(offset, "")
	}

	noError(partitionOffsetManager.Close())
	noError(offsetManager.Close())
	fmt.Printf("update partition %d to offset %d\n", partition, offset)

}

func (m *OffsetManager) setAllPartitionOffset(newOffset int64) {
	if newOffset != sarama.OffsetNewest && newOffset != sarama.OffsetOldest {
		log.Fatal("update all partition cannot be set to same offset, it must be oldest or newest.")
	}

	client := getClusterClient(m.broker)

	offsetManager, err := sarama.NewOffsetManagerFromClient(m.group, client)
	noError(err)

	partitions, err := client.Partitions(m.topic)
	noError(err)

	// visit all partition
	for _, partition := range partitions {
		offset, err := client.GetOffset(m.topic, int32(partition), newOffset)
		if err != nil {
			log.Fatalf("get group: %s, topic:%s, partition:%d offset failed", m.group, m.topic, partition)
			noError(err)
		}

		partitionOffsetManager, err := offsetManager.ManagePartition(m.topic, int32(partition))
		noError(err)

		currentOffset, _ := partitionOffsetManager.NextOffset()
		if offset <= currentOffset {
			partitionOffsetManager.ResetOffset(offset, "")
		} else {
			partitionOffsetManager.MarkOffset(offset, "")
		}

		noError(partitionOffsetManager.Close())
		fmt.Printf("partition %d update to %d\n", partition, offset)
	}
	noError(offsetManager.Close())
}
func Offset(args []string) {
	var subCmd string

	Usage := func() {
		fmt.Print(`Usage of offset:

        ./kafkaHelper offset <command> [arguments]

        set        update consumer group offset.
        get        get consumer group offset.

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
	case "get":
		getOffset(args)
	case "set":
		setOffset(args)
	default:
		Usage()
	}
}

func getOffset(args []string) {
	var configPath, item string
	var debug bool
	flags := flag.NewFlagSet("get consumer offset", flag.ExitOnError)
	flags.StringVar(&configPath, "c", "", "Config path")
	flags.StringVar(&item, "i", "", "item")
	flags.BoolVar(&debug, "debug", false, "print sarama logs")
	cmd := "./kafkahelper offset get -c Config.json -i item [ -debug ]"
	flags.Usage = func() {
		commonUsage(flags, cmd)
	}

	if err := flags.Parse(args); err != nil || configPath == "" || item == "" {
		flags.Usage()
		os.Exit(0)
	}

	conf := getConfig(configPath)
	if debug {
		setDebug()
	}

	if val, ok := conf.Topics[item]; !ok {
		log.Error("cannot item from Config file")
	} else {
		manager := OffsetManager{
			client: getClient(conf.Broker),
			broker: conf.Broker,
			group:  val.Group,
			topic:  val.Topic,
		}
		manager.getAllOffset()
	}
}

func setOffset(args []string) {
	var configPath, item string
	var partitionId int
	var offset int64
	var debug bool
	var cmd = "./kafkahelper offset set -c Config.json -i item -o offset [ -p partitionId ] [ -debug ]"

	flags := flag.NewFlagSet("set consumer group offset ", flag.ExitOnError)
	flags.StringVar(&configPath, "c", "", "Config path")
	flags.StringVar(&item, "i", "", "item")
	flags.IntVar(&partitionId, "p", -1, "partition id")
	flags.Int64Var(&offset, "o", -3, "offset (support special offset: -1 newest, -2 oldest)")
	flags.BoolVar(&debug, "debug", false, "print sarama logs")

	flags.Usage = func() {
		commonUsage(flags, cmd)
	}

	if err := flags.Parse(args); err != nil || configPath == "" || item == "" || offset == -3 {
		flags.Usage()
		os.Exit(0)
	}

	conf := getConfig(configPath)

	if debug {
		setDebug()
	}

	if val, ok := conf.Topics[item]; !ok {
		log.Error("cannot find item from Config file")
	} else {
		manager := OffsetManager{
			client: getClient(conf.Broker),
			broker: conf.Broker,
			group:  val.Group,
			topic:  val.Topic,
		}

		if partitionId == -1 {
			manager.setAllPartitionOffset(offset)
		} else {
			manager.setPartitionOffset(int32(partitionId), offset)
		}
	}
}
