package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/rafaeljusto/gomaxscale"
)

var (
	host     = flag.String("host", "127.0.0.1:4001", "MaxScale CDC listener address")
	database = flag.String("database", "", "Database name")
	table    = flag.String("table", "", "Table name")
	user     = flag.String("user", "", "Username for authenticating to MaxScale")
	password = flag.String("password", "", "Password for authenticating to MaxScale")
)

func main() {
	flag.Parse()

	if *database == "" || *table == "" {
		fmt.Fprintln(os.Stderr, "You must specify database and table")
		flag.Usage()
		os.Exit(1)
	}

	consumer := gomaxscale.NewConsumer(*host, *database, *table,
		gomaxscale.WithAuth(*user, *password),
	)

	dataStream, err := consumer.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	fmt.Printf("started consuming events from database '%s' table '%s'\n",
		dataStream.Database, dataStream.Table)

	done := make(chan bool)
	go func() {
		consumer.Process(func(event gomaxscale.Event) {
			fmt.Printf("event '%s' detected\n", event.Type)
		})
		done <- true
	}()

	signalChanel := make(chan os.Signal, 1)
	signal.Notify(signalChanel, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signalChanel:
	case <-done:
	}

	fmt.Println("terminating")
}
