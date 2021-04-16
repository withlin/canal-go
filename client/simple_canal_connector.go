// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	pb "github.com/withlin/canal-go/protocol"
	"io"
	"net"
	"strconv"
	"sync"

	pbp "github.com/withlin/canal-go/protocol/packet"

	"github.com/golang/protobuf/proto"
)

type SimpleCanalConnector struct {
	Address           string
	Port              int
	UserName          string
	PassWord          string
	SoTime            int32
	IdleTimeOut       int32
	ClientIdentity    pb.ClientIdentity
	Connected         bool
	Running           bool
	Filter            string
	RollbackOnConnect bool
	LazyParseEntry    bool
}

var (
	conn  net.Conn
	mutex sync.Mutex
)

const (
	versionErr   string = "unsupported version at this client."
	handshakeErr        = "expect handshake but found other type."
	packetAckErr        = "unexpected packet type when ack is expected"
)

//NewSimpleCanalConnector 创建SimpleCanalConnector实例
func NewSimpleCanalConnector(address string, port int, username string, password string, destination string, soTimeOut int32, idleTimeOut int32) *SimpleCanalConnector {
	s := &SimpleCanalConnector{
		Address:           address,
		Port:              port,
		UserName:          username,
		PassWord:          password,
		ClientIdentity:    pb.ClientIdentity{Destination: destination, ClientId: 1001},
		SoTime:            soTimeOut,
		IdleTimeOut:       idleTimeOut,
		RollbackOnConnect: true,
	}
	return s

}

//Connect 连接Canal-server
func (c *SimpleCanalConnector) Connect() error {
	if c.Connected {
		return nil
	}

	if c.Running {
		return nil
	}

	err := c.doConnect()
	if err != nil {
		return err
	}
	if c.Filter != "" {
		c.Subscribe(c.Filter)
	}

	if c.RollbackOnConnect {
		c.waitClientRunning()

		c.RollBack(0)
	}

	c.Connected = true
	return nil

}

//quitelyClose 优雅关闭
func quitelyClose() {
	if conn != nil {
		conn.Close()
	}
}

//DisConnection 关闭连接
func (c *SimpleCanalConnector) DisConnection() error {
	if c.RollbackOnConnect && c.Connected == true {
		c.RollBack(0)
	}
	c.Connected = false
	quitelyClose()
	return nil
}

//doConnect 去连接Canal-Server
func (c SimpleCanalConnector) doConnect() error {
	address := c.Address + ":" + fmt.Sprintf("%d", c.Port)
	con, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	conn = con

	p := new(pbp.Packet)
	data, err := readNextPacket()
	if err != nil {
		return err
	}
	err = proto.Unmarshal(data, p)
	if err != nil {
		return err
	}
	if p != nil {
		if p.GetVersion() != 1 {
			return fmt.Errorf(versionErr)
		}

		if p.GetType() != pbp.PacketType_HANDSHAKE {
			return fmt.Errorf(handshakeErr)
		}

		handshake := &pbp.Handshake{}
		seed := &handshake.Seeds
		err = proto.Unmarshal(p.GetBody(), handshake)
		if err != nil {
			return err
		}
		bytePas := []byte(c.PassWord)
		pas := []byte(ByteSliceToHexString(Scramble411(&bytePas, seed)))
		ca := &pbp.ClientAuth{
			Username:               c.UserName,
			Password:               pas,
			NetReadTimeoutPresent:  &pbp.ClientAuth_NetReadTimeout{NetReadTimeout: c.IdleTimeOut},
			NetWriteTimeoutPresent: &pbp.ClientAuth_NetWriteTimeout{NetWriteTimeout: c.IdleTimeOut},
		}
		caByteArray, _ := proto.Marshal(ca)
		packet := &pbp.Packet{
			Type: pbp.PacketType_CLIENTAUTHENTICATION,
			Body: caByteArray,
		}

		packArray, _ := proto.Marshal(packet)

		WriteWithHeader(packArray)

		pp, err := readNextPacket()
		if err != nil {
			return err
		}
		pk := &pbp.Packet{}

		err = proto.Unmarshal(pp, pk)
		if err != nil {
			return err
		}

		if pk.Type != pbp.PacketType_ACK {
			return fmt.Errorf(packetAckErr)
		}

		ackBody := &pbp.Ack{}
		err = proto.Unmarshal(pk.GetBody(), ackBody)
		if err != nil {
			return err
		}
		if ackBody.GetErrorCode() > 0 {

			return fmt.Errorf("something goes wrong when doing authentication:%s", ackBody.GetErrorMessage())
		}

		c.Connected = true

	}
	return nil

}

//GetWithOutAck 获取数据不Ack
func (c *SimpleCanalConnector) GetWithOutAck(batchSize int32, timeOut *int64, units *int32) (*pb.Message, error) {
	c.waitClientRunning()
	if !c.Running {
		return nil, nil
	}
	var size int32

	if batchSize < 0 {
		size = 1000
	} else {
		size = batchSize
	}
	var time *int64
	var t int64
	t = -1
	if timeOut == nil {
		time = &t
	} else {
		time = timeOut
	}
	var i int32
	i = -1
	if units == nil {
		units = &i
	}
	get := new(pbp.Get)
	get.AutoAckPresent = &pbp.Get_AutoAck{AutoAck: false}
	get.Destination = c.ClientIdentity.Destination
	get.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)
	get.FetchSize = size
	get.TimeoutPresent = &pbp.Get_Timeout{Timeout: *time}
	get.UnitPresent = &pbp.Get_Unit{Unit: *units}

	getBody, err := proto.Marshal(get)
	if err != nil {
		return nil, err
	}
	packet := new(pbp.Packet)
	packet.Type = pbp.PacketType_GET
	packet.Body = getBody
	pa, err := proto.Marshal(packet)
	if err != nil {
		return nil, err
	}
	WriteWithHeader(pa)
	message, err := c.receiveMessages()
	if err != nil {
		return nil, err
	}
	return message, nil
}

//Get 获取数据并且Ack数据
func (c *SimpleCanalConnector) Get(batchSize int32, timeOut *int64, units *int32) (*pb.Message, error) {
	message, err := c.GetWithOutAck(batchSize, timeOut, units)
	if err != nil {
		return nil, err
	}
	err = c.Ack(message.Id)
	if err != nil {
		return nil, err
	}
	return message, nil
}

//UnSubscribe 取消订阅
func (c *SimpleCanalConnector) UnSubscribe() error {
	c.waitClientRunning()
	if c.Running {
		return nil
	}

	us := new(pbp.Unsub)
	us.Destination = c.ClientIdentity.Destination
	us.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)

	unSub, err := proto.Marshal(us)
	if err != nil {
		return err
	}

	pa := new(pbp.Packet)
	pa.Type = pbp.PacketType_UNSUBSCRIPTION
	pa.Body = unSub

	pack, err := proto.Marshal(pa)
	WriteWithHeader(pack)

	p, err := readNextPacket()
	if err != nil {
		return err
	}
	pa = nil
	err = proto.Unmarshal(p, pa)
	if err != nil {
		return err
	}
	ack := new(pbp.Ack)
	err = proto.Unmarshal(pa.Body, ack)
	if err != nil {
		return err
	}
	if ack.GetErrorCode() > 0 {
		errMsg := ack.GetErrorMessage()
		return fmt.Errorf("failed to unSubscribe with reason:%s", errMsg)
	}
	return nil
}

//receiveMessages 接收Canal-Server返回的消息体
func (c *SimpleCanalConnector) receiveMessages() (*pb.Message, error) {
	data, err := readNextPacket()
	if err != nil {
		return nil, err
	}
	return pb.Decode(data, c.LazyParseEntry)
}

//Ack Ack Canal-server的数据（就是昨晚某些逻辑操作后删除canal-server端的数据）
func (c *SimpleCanalConnector) Ack(batchId int64) error {
	c.waitClientRunning()
	if !c.Running {
		return nil
	}

	ca := new(pbp.ClientAck)
	ca.Destination = c.ClientIdentity.Destination
	ca.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)
	ca.BatchId = batchId

	clientAck, err := proto.Marshal(ca)
	if err != nil {
		return err
	}
	pa := new(pbp.Packet)
	pa.Type = pbp.PacketType_CLIENTACK
	pa.Body = clientAck
	pack, err := proto.Marshal(pa)
	if err != nil {
		return err
	}
	WriteWithHeader(pack)
	return nil

}

//RollBack 回滚操作
func (c *SimpleCanalConnector) RollBack(batchId int64) error {
	c.waitClientRunning()
	cb := new(pbp.ClientRollback)
	cb.Destination = c.ClientIdentity.Destination
	cb.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)
	cb.BatchId = batchId

	clientBollBack, err := proto.Marshal(cb)
	if err != nil {
		return err
	}

	pa := new(pbp.Packet)
	pa.Type = pbp.PacketType_CLIENTROLLBACK
	pa.Body = clientBollBack
	pack, err := proto.Marshal(pa)
	if err != nil {
		return err
	}
	WriteWithHeader(pack)
	return nil
}

// readHeaderLength 读取protobuf的header字节，该字节存取了你要读的package的长度
func readHeaderLength() int {
	buf := make([]byte, 4)
	conn.Read(buf)
	bytesBuffer := bytes.NewBuffer(buf)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}

//readNextPacket 通过长度去读取数据包
func readNextPacket() ([]byte, error) {
	mutex.Lock()
	defer func() {
		mutex.Unlock()
	}()
	rdr := bufio.NewReader(conn)
	data := make([]byte, 0, 4*1024)
	n, err := io.ReadFull(rdr, data[:4])
	if err != nil {
		return nil, err
	}
	data = data[:n]
	dataLen := binary.BigEndian.Uint32(data)
	if uint64(dataLen) > uint64(cap(data)) {
		data = make([]byte, 0, dataLen)
	}
	n, err = io.ReadFull(rdr, data[:dataLen])
	if err != nil {
		return nil, err
	}
	data = data[:n]
	return data, nil
}

//WriteWithHeader 写数据包的header+body
func WriteWithHeader(body []byte) {
	mutex.Lock()
	lenth := len(body)
	bytes := getWriteHeaderBytes(lenth)
	conn.Write(bytes)
	conn.Write(body)
	mutex.Unlock()
}

// getWriteHeaderBytes 获取要写数据的长度
func getWriteHeaderBytes(lenth int) []byte {
	x := int32(lenth)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//Subscribe 订阅
func (c *SimpleCanalConnector) Subscribe(filter string) error {
	c.waitClientRunning()
	if !c.Running {
		return nil
	}
	body, _ := proto.Marshal(&pbp.Sub{Destination: c.ClientIdentity.Destination, ClientId: strconv.Itoa(c.ClientIdentity.ClientId), Filter: filter})
	pack := new(pbp.Packet)
	pack.Type = pbp.PacketType_SUBSCRIPTION
	pack.Body = body

	packet, _ := proto.Marshal(pack)
	WriteWithHeader(packet)

	p := new(pbp.Packet)

	paBytes, err := readNextPacket()
	if err != nil {
		return err
	}
	err = proto.Unmarshal(paBytes, p)
	if err != nil {
		return err
	}
	ack := new(pbp.Ack)
	err = proto.Unmarshal(p.Body, ack)
	if err != nil {
		return err
	}

	if ack.GetErrorCode() > 0 {
		return fmt.Errorf("failed to subscribe with reason:%s", ack.GetErrorMessage())
	}

	c.Filter = filter

	return nil
}

//waitClientRunning 等待客户端跑
func (c *SimpleCanalConnector) waitClientRunning() {
	c.Running = true
}
