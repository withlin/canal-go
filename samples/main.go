package main

import (
	"canal-go/client"
)

func main() {

	connector := client.NewSimpleCanalConnector("127.0.0.1", 11111, "", "", "example", 60000, 60*60*1000)
	connector.Connect()
	connector.Subscribe(".*\\\\..*")
}
