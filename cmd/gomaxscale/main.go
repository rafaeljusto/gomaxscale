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
	maxScaleHost = flag.String(
		"maxscale-host",
		func() string {
			if os.Getenv("GOMAXSCALE_MAXSCALE_HOST") != "" {
				return os.Getenv("GOMAXSCALE_MAXSCALE_HOST")
			}
			return "127.0.0.1:4001"
		}(),
		"MaxScale CDC listener address",
	)
	maxScaleUser = flag.String(
		"maxscale-user",
		os.Getenv("GOMAXSCALE_MAXSCALE_USER"),
		"Username for authenticating to MaxScale",
	)
	maxScalePassword = flag.String(
		"maxscale-password",
		os.Getenv("GOMAXSCALE_MAXSCALE_PASSWORD"),
		"Password for authenticating to MaxScale",
	)
	databaseName = flag.String(
		"database-name",
		os.Getenv("GOMAXSCALE_DATABASE_NAME"),
		"Database name",
	)
	databaseTable = flag.String(
		"database-table",
		os.Getenv("GOMAXSCALE_DATABASE_TABLE"),
		"Table name",
	)
)

func main() {
	flag.Parse()

	if *databaseName == "" || *databaseTable == "" {
		fmt.Fprintln(os.Stderr, "You must specify database and table")
		flag.Usage()
		os.Exit(1)
	}

	consumer := gomaxscale.NewConsumer(*maxScaleHost, *databaseName, *databaseTable,
		gomaxscale.WithAuth(*maxScaleUser, *maxScalePassword),
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
