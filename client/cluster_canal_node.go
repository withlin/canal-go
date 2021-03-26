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

/**
修改说明：去掉CanalClusterNode里的ServerRunningData和Event，不再监听，改为随用随取，因为不能确定取到值后就进行链接，会造成数据不一致，
		 如果当前Server挂了，连接会断开，到时候直接重连就可以了
*/

type ServerRunningData struct {
	Cid     int64
	Address string
	Active  bool
}

type CanalClusterNode struct {
	zkClient *zk.Conn
	destination    string
	clusterAddress []string
	clusterEvent   <-chan zk.Event
}

const (
	cluster_path = "/otter/canal/destinations/%s/cluster"
	running_path = "/otter/canal/destinations/%s/running"
)

func NewCanalClusterNode(destination string, zkServer []string, timeout time.Duration) (canalNode *CanalClusterNode, err error) {
	var (
		zkClient   *zk.Conn
		cluster    []string
		clusterEV  <-chan zk.Event
	)

	if zkClient, _, err = zk.Connect(zkServer, timeout); err != nil {
		log.Printf("zk.Connect err:%v", err)
		return
	}
	if cluster, _, clusterEV, err = zkClient.ChildrenW(fmt.Sprintf(cluster_path, destination)); err != nil {
		log.Printf("zkClient.ChildrenW err:%v", err)
		return
	}

	canalNode = &CanalClusterNode{
		zkClient:zkClient,
		destination:   destination,
		clusterEvent:  clusterEV,
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

	serverRunningData, err := canalNode.getRunningServer()
	if err != nil {
		return "", 0, err
	}

	s := strings.Split(serverRunningData.Address, ":")
	if len(s) == 2 && s[0]!=""{
		port, err = strconv.Atoi(s[1])
		if  err != nil {
			return "",0, fmt.Errorf("error canal cluster server %s", serverRunningData.Address)
		}

		addr = s[0]
		return
	}else {
		return "", 0, fmt.Errorf("error canal cluster server %s", serverRunningData.Address)
	}
}

func (canalNode *CanalClusterNode) getRunningServer() (ServerRunningData,error) {
	serverInfo := ServerRunningData{}

	body, _, err := canalNode.zkClient.Get(fmt.Sprintf(running_path, canalNode.destination))
	if err != nil {
		log.Printf("zkClient.GetW err:%v", err)
		return serverInfo,err
	}

	err = json.Unmarshal(body, &serverInfo);
	if err != nil {
		log.Printf("json.Unmarshal err:%v", err)
		return serverInfo,err
	}

	return serverInfo,nil
}