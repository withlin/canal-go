package client

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type ServerRunningData struct {
	Cid     int64
	Address string
	Active  bool
}

type CanalClusterNode struct {
	destination    string
	clusterAddress []string
	runningServer  *ServerRunningData
	clusterEvent   <-chan zk.Event
	serverEvent    <-chan zk.Event
}

const (
	cluster_path = "/otter/canal/destinations/%s/cluster"
	running_path = "/otter/canal/destinations/%s/running"
)

func NewCanalClusterNode(destination string, zkServer []string, timeout time.Duration) (canalNode *CanalClusterNode, err error) {
	var (
		zkClient   *zk.Conn
		b          []byte
		cluster    []string
		clusterEV  <-chan zk.Event
		serverEV   <-chan zk.Event
		serverInfo ServerRunningData
	)

	if zkClient, _, err = zk.Connect(zkServer, timeout); err != nil {
		log.Printf("zk.Connect err:%v", err)
		return
	}
	if cluster, _, clusterEV, err = zkClient.ChildrenW(fmt.Sprintf(cluster_path, destination)); err != nil {
		log.Printf("zkClient.ChildrenW err:%v", err)
		return
	}

	if b, _, serverEV, err = zkClient.GetW(fmt.Sprintf(running_path, destination)); err != nil {
		log.Printf("zkClient.GetW err:%v", err)
		return
	}

	if err = json.Unmarshal(b, &serverInfo); err != nil {
		log.Printf("json.Unmarshal err:%v", err)
		return
	}

	canalNode = &CanalClusterNode{
		destination:   destination,
		runningServer: &serverInfo,
		clusterEvent:  clusterEV,
		serverEvent:   serverEV,
	}

	canalNode.InitClusters(cluster)
	return
}

func (canalNode *CanalClusterNode) InitClusters(addressList []string) {
	rand.Shuffle(len(addressList), func(a, b int) {
		addressList[a], addressList[b] = addressList[b], addressList[a]
	})
	canalNode.clusterAddress = addressList
}

func (canalNode *CanalClusterNode) GetNode() (addr string, port int, err error) {
	server := ""
	if canalNode.runningServer != nil {
		server = canalNode.runningServer.Address
	}

	if server == "" && len(canalNode.clusterAddress) > 0 {
		server = canalNode.clusterAddress[0]
	}

	if server != "" {
		s := strings.Split(server, ":")
		if len(s) == 2 {
			if port, err = strconv.Atoi(s[1]); err == nil {
				addr = s[0]
			}
		}
	} else {
		return "", 0, fmt.Errorf("no alive canal server for %s", canalNode.destination)
	}
	if addr == "" {
		return "", 0, fmt.Errorf("error canal cluster server %s", server)
	}
	return
}
