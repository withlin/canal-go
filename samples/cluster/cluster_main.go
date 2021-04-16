package main

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/withlin/canal-go/client"
	pbe "github.com/withlin/canal-go/protocol/entry"
	"log"
	"os"
	"time"
)

var conn *client.ClusterCanalConnector

func main() {
	conn = createConnection()
	subscribe(conn, ".*\\..*")

	fmt.Println("canal start listening...")
	listen()
	err := conn.DisConnection()
	if err != nil {
		fmt.Println(err)
	}
}

func listen() {
	for {
		message, err := conn.Get(100, nil, nil)
		if err != nil {
			fmt.Println(err)
			return
		}

		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		printEntry(message.Entries)
	}
}

func subscribe(conn *client.ClusterCanalConnector, str string) {
	err := conn.Subscribe(str)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func createConnection() *client.ClusterCanalConnector {
	cn, err := client.NewCanalClusterNode("example", []string{"192.168.0.201:2181", "192.168.0.202:2181", "192.168.0.203:2181"}, time.Second*10)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	canalConnector, err := client.NewClusterCanalConnector(cn, "canal", "canal", "example", 60000, 60*60*1000)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	err = canalConnector.Connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	return canalConnector
}

func printEntry(entrys []pbe.Entry) {

	for _, entry := range entrys {
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN || entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)

		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		checkError(err)
		if rowChange != nil {
			eventType := rowChange.GetEventType()
			header := entry.GetHeader()
			fmt.Println(fmt.Sprintf("================> binlog[%s : %d],name[%s,%s], eventType: %s", header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(), header.GetTableName(), header.GetEventType()))

			for _, rowData := range rowChange.GetRowDatas() {
				if eventType == pbe.EventType_DELETE {
					printColumn(rowData.GetBeforeColumns())
				} else if eventType == pbe.EventType_INSERT {
					printColumn(rowData.GetAfterColumns())
				} else {
					fmt.Println("-------> before")
					printColumn(rowData.GetBeforeColumns())
					fmt.Println("-------> after")
					printColumn(rowData.GetAfterColumns())
				}
			}
		}
	}
}

func printColumn(columns []*pbe.Column) {
	for _, col := range columns {
		fmt.Println(fmt.Sprintf("%s : %s  update= %t", col.GetName(), col.GetValue(), col.GetUpdated()))
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
