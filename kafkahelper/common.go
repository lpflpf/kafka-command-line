package kafkahelper

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

var Logger = log.New(os.Stderr, "", log.LstdFlags)

type Config struct {
	Broker []string `json:"brokers"`
	Topics map[string]struct {
		Topic string `json:"topic"`
		Group string `json:"group"`
	} `json:"topics"`
}

func noError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func getConfig(filename string) Config {
	if data, err := ioutil.ReadFile(filename); err != nil {
		log.Fatal(err)
	} else {
		conf := Config{}
		if err = json.Unmarshal(data, &conf); err != nil {
			log.Fatal(err)
		}
		return conf
	}
	return Config{}
}

func getClient(broker []string) sarama.Client {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0
	config.Producer.Partitioner = sarama.NewManualPartitioner
	if client, err := sarama.NewClient(broker, config); err != nil {
		log.Fatal(err)
	} else {
		return client
	}
	return nil
}

func getAdmin(broker []string) sarama.ClusterAdmin {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_0_0

	if admin, err := sarama.NewClusterAdmin(broker, config); err != nil {
		log.Fatal(err)
	} else {
		return admin
	}
	return nil
}

func param2Str(f *flag.FlagSet) string {
	usage := ""

	width := 1

	var outputs []string
	var usages []string
	var param string

	f.VisitAll(func(i *flag.Flag) {
		s := fmt.Sprintf("-%s", i.Name)
		name, iUsage := flag.UnquoteUsage(i)
		s += " " + name

		if name != "" {
			param = fmt.Sprintf("%s %s", i.Name, name)
		} else {
			param = i.Name
		}

		if len(param) > width {
			width = len(param)
		}
		outputs = append(outputs, param)
		usages = append(usages, iUsage)
	})

	for i := 0; i < len(outputs); i++ {
		usage += fmt.Sprintf("        -%-"+strconv.Itoa(width)+"s %s\n", outputs[i], usages[i])
	}
	return usage
}

func commonUsage(flags *flag.FlagSet, cmd string) {
	usage := fmt.Sprintf("Usage of %s :\n\n        %s\n\n%s", flags.Name(), cmd, param2Str(flags))
	fmt.Println(usage)
}

func setDebug() {
	sarama.Logger = log.New(os.Stderr, "[sarama] ", log.LstdFlags)
}
