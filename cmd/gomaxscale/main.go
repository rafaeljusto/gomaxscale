package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/rafaeljusto/gomaxscale/v2"
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

	err := consumer.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	fmt.Println("start consuming events")

	done := make(chan bool)
	go func() {
		consumer.Process(func(event gomaxscale.CDCEvent) {
			switch e := event.(type) {
			case gomaxscale.DDLEvent:
				fmt.Printf("ddl event detected on database '%s' and table '%s'\n",
					e.Database, e.Table)
			case gomaxscale.DMLEvent:
				fmt.Printf("dml '%s' event detected\n", e.Type)
			}
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
