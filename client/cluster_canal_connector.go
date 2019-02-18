package client

import (
	"errors"
	"log"
	"time"

	pb "github.com/CanalClient/canal-go/protocol"
)

type ClusterCanalConnector struct {
	conn                            *SimpleCanalConnector
	canalNode                       *CanalClusterNode
	username, password, destination string
	soTimeOut, idleTimeOut          int32
	filter                          string

	RetryTimes int32
}

func NewClusterCanalConnector(canalNode *CanalClusterNode, username string, password string, destination string,
	soTimeOut int32, idleTimeOut int32) *ClusterCanalConnector {

	cc := &ClusterCanalConnector{
		canalNode: canalNode, username: username, password: password,
		destination: destination, soTimeOut: soTimeOut, idleTimeOut: idleTimeOut,
		RetryTimes: 3,
	}

	return cc
}

// 重试失败后重新连接
func (cc *ClusterCanalConnector) reTryWithConn(name string, do func() error) error {
	return cc.reTry(name, func() error {
		if cc.conn == nil {
			cc.Connect()
		}
		if cc.conn == nil {
			return errors.New("canal connect fail")
		}
		if err := do(); err != nil {
			cc.Connect()
			return err
		}
		return nil
	})
}

func (cc *ClusterCanalConnector) reTry(name string, do func() error) (err error) {
	for times := int32(0); times < cc.RetryTimes; times++ {
		if err = do(); err != nil {
			log.Printf("%s err:%v, reTry", name, err)
			time.Sleep(time.Second * 5)
		} else {
			return nil
		}
	}
	return
}

func (cc *ClusterCanalConnector) Connect() error {
	log.Printf("Connect")

	return cc.reTry("Connect", func() error {
		var (
			addr string
			port int
			err  error
		)
		cc.DisConnection()
		if addr, port, err = cc.canalNode.GetNode(); err != nil {
			log.Printf("canalNode.GetNode addr=%s, port=%d, err=%v\n", addr, port, err)
			return err
		}

		conn := NewSimpleCanalConnector(addr, port, cc.username, cc.password,
			cc.destination, cc.soTimeOut, cc.idleTimeOut)

		if cc.filter != "" {
			conn.Filter = cc.filter
		}

		if err = conn.Connect(); err != nil {
			log.Printf("conn.Connect err:%v", err)
			conn.DisConnection()
			return err
		}
		cc.conn = conn
		return nil
	})
}

func (cc *ClusterCanalConnector) DisConnection() {
	if cc.conn != nil {
		cc.conn.DisConnection()
		cc.conn = nil
	}
}

func (cc *ClusterCanalConnector) Subscribe(filter string) error {
	return cc.reTryWithConn("Subscribe", func() (err error) {
		if err = cc.conn.Subscribe(filter); err == nil {
			cc.filter = filter
		}
		return
	})
}

func (cc *ClusterCanalConnector) UnSubscribe() error {
	return cc.reTryWithConn("UnSubscribe", func() error {
		return cc.conn.UnSubscribe()
	})
}

func (cc *ClusterCanalConnector) GetWithOutAck(batchSize int32, timeOut *int64, units *int32) (msg *pb.Message, err error) {
	_ = cc.reTryWithConn("GetWithOutAck", func() error {
		msg, err = cc.conn.GetWithOutAck(batchSize, timeOut, units)
		return err
	})
	return
}

func (cc *ClusterCanalConnector) Get(batchSize int32, timeOut *int64, units *int32) (msg *pb.Message, err error) {
	_ = cc.reTryWithConn("Get", func() error {
		msg, err = cc.conn.Get(batchSize, timeOut, units)
		return err
	})
	return
}

func (cc *ClusterCanalConnector) Ack(batchId int64) error {
	return cc.reTryWithConn("Ack", func() error {
		return cc.conn.Ack(batchId)
	})
}

func (cc *ClusterCanalConnector) RollBack(batchId int64) error {
	return cc.reTryWithConn("RollBack", func() error {
		return cc.conn.RollBack(batchId)
	})
}
