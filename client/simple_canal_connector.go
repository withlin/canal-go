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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"

	pb "github.com/CanalClient/canal-go/protocol"

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
func (c *SimpleCanalConnector) Connect() {
	if c.Connected {
		return
	}

	if c.Running {
		return
	}

	c.doConnect()
	if c.Filter != "" {
		c.Subscribe(c.Filter)
	}

	if c.RollbackOnConnect {
		c.waitClientRunning()

		c.RollBack(0)
	}

	c.Connected = true

}

//quitelyClose 安静关闭
func quitelyClose() {
	if conn != nil {
		conn.Close()
	}
}

//DisConnection 关闭连接
func (c *SimpleCanalConnector) DisConnection() {
	if c.RollbackOnConnect && c.Connected == true {
		c.RollBack(0)
	}
	c.Connected = false
	quitelyClose()
}

//doConnect 去连接Canal-Server
func (c SimpleCanalConnector) doConnect() {
	address := c.Address + ":" + fmt.Sprintf("%d", c.Port)
	con, err := net.Dial("tcp", address)
	checkError(err)
	conn = con

	p := new(pb.Packet)
	data := readNextPacket()
	err = proto.Unmarshal(data, p)
	checkError(err)
	if p != nil {
		if p.GetVersion() != 1 {
			panic("unsupported version at this client.")
		}

		if p.GetType() != pb.PacketType_HANDSHAKE {
			panic("expect handshake but found other type.")
		}

		handshake := &pb.Handshake{}
		err = proto.Unmarshal(p.GetBody(), handshake)
		checkError(err)
		pas := []byte(c.PassWord)
		ca := &pb.ClientAuth{
			Username:               c.UserName,
			Password:               pas,
			NetReadTimeoutPresent:  &pb.ClientAuth_NetReadTimeout{NetReadTimeout: c.IdleTimeOut},
			NetWriteTimeoutPresent: &pb.ClientAuth_NetWriteTimeout{NetWriteTimeout: c.IdleTimeOut},
		}
		caByteArray, _ := proto.Marshal(ca)
		packet := &pb.Packet{
			Type: pb.PacketType_CLIENTAUTHENTICATION,
			Body: caByteArray,
		}

		packArray, _ := proto.Marshal(packet)

		WriteWithHeader(packArray)

		pp := readNextPacket()
		pk := &pb.Packet{}

		err = proto.Unmarshal(pp, pk)
		checkError(err)

		if pk.Type != pb.PacketType_ACK {
			panic("unexpected packet type when ack is expected")
		}

		ackBody := &pb.Ack{}
		err = proto.Unmarshal(pk.GetBody(), ackBody)

		if ackBody.GetErrorCode() > 0 {

			panic(errors.New(fmt.Sprintf("something goes wrong when doing authentication:%s", ackBody.GetErrorMessage())))
		}

		c.Connected = true

	}

}

//GetWithOutAck 获取数据不Ack
func (c *SimpleCanalConnector) GetWithOutAck(batchSize int32, timeOut *int64, units *int32) *pb.Message {
	c.waitClientRunning()
	if !c.Running {
		return nil
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
	get := new(pb.Get)
	get.AutoAckPresent = &pb.Get_AutoAck{AutoAck: false}
	get.Destination = c.ClientIdentity.Destination
	get.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)
	get.FetchSize = size
	get.TimeoutPresent = &pb.Get_Timeout{Timeout: *time}
	get.UnitPresent = &pb.Get_Unit{Unit: *units}

	getBody, err := proto.Marshal(get)
	checkError(err)
	packet := new(pb.Packet)
	packet.Type = pb.PacketType_GET
	packet.Body = getBody
	pa, err := proto.Marshal(packet)
	checkError(err)
	WriteWithHeader(pa)
	message := c.receiveMessages()
	return message
}

//Get 获取数据并且Ack数据
func (c *SimpleCanalConnector) Get(batchSize int32, timeOut *int64, units *int32) *pb.Message {
	message := c.GetWithOutAck(batchSize, timeOut, units)
	c.Ack(message.Id)
	return message
}

//UnSubscribe 取消订阅
func (c *SimpleCanalConnector) UnSubscribe() {
	c.waitClientRunning()
	if c.Running {
		return
	}

	us := new(pb.Unsub)
	us.Destination = c.ClientIdentity.Destination
	us.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)

	unSub, err := proto.Marshal(us)
	checkError(err)

	pa := new(pb.Packet)
	pa.Type = pb.PacketType_UNSUBSCRIPTION
	pa.Body = unSub

	pack, err := proto.Marshal(pa)
	WriteWithHeader(pack)

	p := readNextPacket()
	pa = nil
	err = proto.Unmarshal(p, pa)
	checkError(err)
	ack := new(pb.Ack)
	err = proto.Unmarshal(pa.Body, ack)
	checkError(err)
	if ack.GetErrorCode() > 0 {
		panic(errors.New(fmt.Sprintf("failed to unSubscribe with reason:%s", ack.GetErrorMessage())))
	}
}

//receiveMessages 接收Canal-Server返回的消息体
func (c *SimpleCanalConnector) receiveMessages() *pb.Message {
	data := readNextPacket()
	p := new(pb.Packet)
	err := proto.Unmarshal(data, p)
	checkError(err)
	messages := new(pb.Messages)
	message := new(pb.Message)

	length := len(messages.Messages)
	message.Entries = make([]pb.Entry, length)
	ack := new(pb.Ack)
	var items []pb.Entry
	var entry pb.Entry
	switch p.Type {
	case pb.PacketType_MESSAGES:
		if !(p.GetCompression() == pb.Compression_NONE) {
			panic("compression is not supported in this connector")
		}
		err := proto.Unmarshal(p.Body, messages)
		checkError(err)
		if c.LazyParseEntry {
			message.RawEntries = messages.Messages
		} else {

			for _, value := range messages.Messages {
				err := proto.Unmarshal(value, &entry)
				checkError(err)
				items = append(items, entry)
			}
		}
		message.Entries = items
		message.Id = messages.GetBatchId()
		return message

	case pb.PacketType_ACK:
		err := proto.Unmarshal(p.Body, ack)
		checkError(err)
		panic(errors.New(fmt.Sprintf("something goes wrong with reason:%s", ack.GetErrorMessage())))
	default:
		panic(errors.New(fmt.Sprintf("unexpected packet type:%s", p.Type)))

	}
}

//Ack Ack Canal-server的数据（就是昨晚某些逻辑操作后删除canal-server端的数据）
func (c *SimpleCanalConnector) Ack(batchId int64) {
	c.waitClientRunning()
	if !c.Running {
		return
	}

	ca := new(pb.ClientAck)
	ca.Destination = c.ClientIdentity.Destination
	ca.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)
	ca.BatchId = batchId

	clientAck, err := proto.Marshal(ca)
	checkError(err)
	pa := new(pb.Packet)
	pa.Type = pb.PacketType_CLIENTACK
	pa.Body = clientAck
	pack, err := proto.Marshal(pa)
	checkError(err)
	WriteWithHeader(pack)

}

//RollBack 回滚操作
func (c *SimpleCanalConnector) RollBack(batchId int64) {
	c.waitClientRunning()
	cb := new(pb.ClientRollback)
	cb.Destination = c.ClientIdentity.Destination
	cb.ClientId = strconv.Itoa(c.ClientIdentity.ClientId)
	cb.BatchId = batchId

	clientBollBack, err := proto.Marshal(cb)
	checkError(err)

	pa := new(pb.Packet)
	pa.Type = pb.PacketType_CLIENTROLLBACK
	pa.Body = clientBollBack
	pack, err := proto.Marshal(pa)
	checkError(err)
	WriteWithHeader(pack)
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
func readNextPacket() []byte {
	mutex.Lock()
	rdr := bufio.NewReader(conn)
	data := make([]byte, 0, 4*1024)
	n, err := io.ReadFull(rdr, data[:4])
	checkError(err)
	data = data[:n]
	dataLen := binary.BigEndian.Uint32(data)
	if uint64(dataLen) > uint64(cap(data)) {
		data = make([]byte, 0, dataLen)
	}
	n, err = io.ReadFull(rdr, data[:dataLen])
	checkError(err)
	data = data[:n]
	mutex.Unlock()
	return data
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
func (c *SimpleCanalConnector) Subscribe(filter string) {
	c.waitClientRunning()
	if !c.Running {
		return
	}
	body, _ := proto.Marshal(&pb.Sub{Destination: c.ClientIdentity.Destination, ClientId: strconv.Itoa(c.ClientIdentity.ClientId), Filter: c.Filter})
	pack := new(pb.Packet)
	pack.Type = pb.PacketType_SUBSCRIPTION
	pack.Body = body

	packet, _ := proto.Marshal(pack)
	WriteWithHeader(packet)

	p := new(pb.Packet)

	paBytes := readNextPacket()
	err := proto.Unmarshal(paBytes, p)
	checkError(err)
	ack := new(pb.Ack)
	err = proto.Unmarshal(p.Body, ack)
	checkError(err)

	if ack.GetErrorCode() > 0 {

		panic(errors.New(fmt.Sprintf("failed to subscribe with reason::%s", ack.GetErrorMessage())))
	}

	c.Filter = filter

}

//waitClientRunning 等待客户端跑
func (c *SimpleCanalConnector) waitClientRunning() {
	c.Running = true
}

//checkError 检查错误
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
