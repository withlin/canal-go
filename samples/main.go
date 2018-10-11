package main

import (

	"canal-go/client"
)

func main() {

	cli :=client.NewSimpleCanalConnector("192.168.199.17",11111,"","","example",60000,60 * 60 * 1000)
	cli.Connect()
}
