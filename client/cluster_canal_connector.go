package client

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"

	pb "github.com/withlin/canal-go/protocol"
)

type ClusterCanalConnector struct {
	conn                            *SimpleCanalConnector
	canalNode                       *CanalClusterNode
	username, password, destination string
	soTimeOut, idleTimeOut          int32
	filter                          string

	RetryTimes      int32
	currentSequence string
	zkVersion       int32

	Path string
}

const (
	path           = "/canal-consumer"
	runningFlag    = byte(0)
	notRunningFlag = byte(0)
)

func NewClusterCanalConnector(canalNode *CanalClusterNode, username string, password string, destination string,
	soTimeOut int32, idleTimeOut int32) (*ClusterCanalConnector, error) {

	destinationPath := fmt.Sprintf("%s/%s", path, destination)

	err := checkRootPath(canalNode.zkClient, destinationPath)
	if err != nil {
		return nil, err
	}

	currentSequence, err := createEphemeralSequence(canalNode.zkClient, destinationPath)
	if err != nil {
		return nil, err
	}

	cluster := &ClusterCanalConnector{
		canalNode:       canalNode,
		username:        username,
		password:        password,
		destination:     destination,
		soTimeOut:       soTimeOut,
		idleTimeOut:     idleTimeOut,
		RetryTimes:      0,
		currentSequence: currentSequence,
		zkVersion:       0,
		Path:            destinationPath,
	}

	return cluster, nil
}

func (cc *ClusterCanalConnector) Connect() error {
	log.Println("connecting canal server...")
	err := cc.reconnect()
	return err
}

func (cc *ClusterCanalConnector) reconnect() error {
	err := cc.doConnect()
	if err != nil {
		log.Println("connect canal-server failed ", err)
		cc.RetryTimes++
		if cc.RetryTimes < 5 {
			time.Sleep(5 * time.Second)
			return cc.reconnect()
		}
		return err
	}
	cc.RetryTimes = 0

	return nil
}

func (cc *ClusterCanalConnector) doConnect() error {

	log.Println("connecting...")

	//等待成为最小的节点，最小的节点去运行
	err := cc.waitBecomeFirst()
	if err != nil {
		return fmt.Errorf("error wait become first zk node %s", err.Error())
	}

	_, err = cc.canalNode.zkClient.Set(cc.Path+"/"+cc.currentSequence, []byte{runningFlag}, cc.zkVersion)
	if err != nil {
		return fmt.Errorf("error set running flag %s", err.Error())
	}
	cc.zkVersion++

	addr, port, err := cc.canalNode.GetNode()
	if err != nil {
		return fmt.Errorf("error get zk current node path %s", err.Error())
	}

	connector := NewSimpleCanalConnector(addr, port, cc.username, cc.password, cc.destination, cc.soTimeOut, cc.idleTimeOut)
	cc.conn = connector

	err = connector.Connect()
	if err != nil {
		return err
	}

	log.Println("connected to ", addr, " success")

	return nil
}

func (cc *ClusterCanalConnector) DisConnection() error {
	if cc.conn != nil {
		cc.conn.DisConnection()
		_, stat, _ := cc.canalNode.zkClient.Get(cc.Path + "/" + cc.currentSequence)
		err := cc.canalNode.zkClient.Delete(cc.Path+"/"+cc.currentSequence, stat.Version)
		if err != nil {
			return fmt.Errorf("error delete temp consumer path %s", err.Error())
		}
		cc.conn = nil
	}

	return nil
}

func (cc *ClusterCanalConnector) Subscribe(filter string) error {
	err := cc.conn.Subscribe(filter)
	if err != nil {
		err = cc.reconnect()
		if err != nil {
			return err
		}
		return cc.Subscribe(filter)
	}

	return nil
}

func (cc *ClusterCanalConnector) UnSubscribe() error {
	err := cc.conn.UnSubscribe()
	if err != nil {
		err = cc.reconnect()
		if err != nil {
			return err
		}
		return cc.UnSubscribe()
	}

	return nil
}

func (cc *ClusterCanalConnector) GetWithOutAck(batchSize int32, timeOut *int64, units *int32) (msg *pb.Message, err error) {
	msg, err = cc.conn.GetWithOutAck(batchSize, timeOut, units)
	if err != nil {
		err = cc.reconnect()
		if err != nil {
			return nil, err
		}
		return cc.conn.GetWithOutAck(batchSize, timeOut, units)
	}

	return
}

func (cc *ClusterCanalConnector) Get(batchSize int32, timeOut *int64, units *int32) (msg *pb.Message, err error) {
	msg, err = cc.conn.Get(batchSize, timeOut, units)
	if err != nil {
		err = cc.reconnect()
		if err != nil {
			return nil, err
		}
		return cc.conn.Get(batchSize, timeOut, units)
	}

	return
}

func (cc *ClusterCanalConnector) Ack(batchId int64) error {
	err := cc.conn.Ack(batchId)
	if err != nil {
		err = cc.reconnect()
		if err != nil {
			return err
		}
		return cc.Ack(batchId)
	}

	return nil
}

func (cc *ClusterCanalConnector) RollBack(batchId int64) error {
	err := cc.conn.RollBack(batchId)
	if err != nil {
		err = cc.reconnect()
		if err != nil {
			return err
		}
		return cc.RollBack(batchId)
	}

	return nil
}

func createEphemeralSequence(zkClient *zk.Conn, destinationPath string) (string, error) {
	node, err := zkClient.Create(destinationPath+"/", []byte{notRunningFlag}, zk.FlagEphemeral|zk.FlagSequence,
		zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", err
	}
	split := strings.Split(node, "/")
	currentSequence := split[len(split)-1]
	return currentSequence, nil
}

func checkRootPath(zkClient *zk.Conn, destinationPath string) error {
	rootExists, _, err := zkClient.Exists(path)
	if err != nil {
		return err
	}
	if !rootExists {
		_, err := zkClient.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	exists, _, err := zkClient.Exists(destinationPath)
	if err != nil {
		return err
	}
	if !exists {
		_, err := zkClient.Create(destinationPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}

func (cc *ClusterCanalConnector) waitBecomeFirst() error {
	zkClient := cc.canalNode.zkClient
	children, _, err := zkClient.Children(cc.Path)
	if err != nil {
		return err
	}

	if len(children) == 0 {
		return errors.New("没有子节点")
	}

	sort.Strings(children)

	if cc.currentSequence != children[0] {
		noSelf := true

		for i, child := range children {
			if cc.currentSequence == child {
				noSelf = false
				previousPath := cc.Path + "/" + children[i-1]
				//阻塞等待上一个比他小的节点删除
				log.Println("waiting")
				err := waitDelete(zkClient, previousPath)
				if err != nil {
					return err
				}
				log.Println("waited")

				return cc.waitBecomeFirst()
			}
		}

		if noSelf {
			//以防万一
			cc.currentSequence, err = createEphemeralSequence(zkClient, cc.Path)
			if err != nil {
				return err
			}
			// 再次创建临时节点成功后将版本设置为0，不然后续更新节点会出现版本冲突
			cc.zkVersion = 0
			return cc.waitBecomeFirst()
		}
	}

	return nil
}

// 等待上一个比他小的节点失联，失联后等待10秒，10秒后还没恢复则确认已被删除
func waitDelete(zkClient *zk.Conn, previousPath string) error {
	existsW, _, events, err := zkClient.ExistsW(previousPath)
	if err != nil {
		return err
	}

	if existsW {
		event := <-events
		if event.Type != zk.EventNodeDeleted {
			return waitDelete(zkClient, previousPath)
		} else {
			//等待10秒再查看监听的节点是否确实不存在了，以防只是网络延迟造成的掉线
			time.Sleep(10 * time.Second)
			return waitDelete(zkClient, previousPath)
		}
	}

	return nil
}
