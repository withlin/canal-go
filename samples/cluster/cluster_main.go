package main

import (
	"fmt"
	"log"
	"time"

	"github.com/CanalClient/canal-go/client"
	protocol "github.com/CanalClient/canal-go/protocol"
)

func main() {

	fmt.Println("cluster main")
	conn, _ := createConn()
	conn.Subscribe(".*\\\\..*")
	n := 0
	for {
		message, err := conn.Get(100, nil, nil)
		if err != nil {
			log.Println(err)
			continue
		}
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(3000 * time.Millisecond)
			fmt.Println("===没有数据了===")
			n++
			if n > 100 {
				break
			}
			continue
		}
		printEntry(message.Entries)
	}

	conn.DisConnection()
}

func createConn() (conn *client.ClusterCanalConnector, err error) {

	cn, err := client.NewCanalClusterNode("example", []string{"zookeeper:2181"}, time.Second*10)
	fmt.Printf("err=%v,cn=%+v\n\n", err, cn)

	addr, port, err := cn.GetNode()
	fmt.Printf("addr=%s, port=%d, err=%v", addr, port, err)

	conn = client.NewClusterCanalConnector(cn, "", "", "example", 60000, 60*60*1000)
	err = conn.Connect()
	fmt.Printf("err=%v,cluCanalConn=%+vn\n", err, conn)
	return
}

func printEntry(entrys []protocol.Entry) {
	for _, entry := range entrys {
		fmt.Printf("entry type:%d\n", entry.GetEntryType())
	}
}
