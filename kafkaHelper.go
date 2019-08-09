package main

import (
	"fmt"
	"github.com/lpflpf/kafka-command-line/kafkahelper"
	"os"
)

func main() {
	if len(os.Args) == 1 {
		Usage()
	}
	var arguments []string

	if len(os.Args) > 2 {
		arguments = os.Args[2:]
	}

	switch os.Args[1] {
	case "offset":
		kafkahelper.Offset(arguments)
	case "message":
		kafkahelper.Message(arguments)
	}
}

func Usage() {
	fmt.Print(`
Usage:
        kafkaHelper <command> [arguments]

The commands are:

        offset      offset commands
        message     message commands

`)
	os.Exit(0)
}
