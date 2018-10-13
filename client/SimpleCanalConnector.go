package client

import (
	"bufio"
	"bytes"
	protocol "canal-go/protocol"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"

	// "bytes"
	// "encoding/binary"

	"github.com/golang/protobuf/proto"
)

type SimpleCanalConnector struct {
	Address            string
	Port               int
	UserName           string
	PassWord           string
	SoTime             int32
	IdleTimeOut        int32
	ClientIdentity     protocol.ClientIdentity
	Connected          bool
	Running            bool
	Filter             string
	RrollbackOnConnect bool
	reader             *bufio.Reader //读取
}

var (
	samplecanal SimpleCanalConnector
	conn        net.Conn
)

func NewSimpleCanalConnector(address string, port int, username string, password string, destination string, soTimeOut int32, idleTimeOut int32) *SimpleCanalConnector {
	sampleCanalConnector := &SimpleCanalConnector{}
	sampleCanalConnector.Address = address
	sampleCanalConnector.Port = port
	sampleCanalConnector.UserName = username
	sampleCanalConnector.PassWord = password
	sampleCanalConnector.ClientIdentity = protocol.ClientIdentity{Destination: destination, ClientId: 1001}
	sampleCanalConnector.SoTime = soTimeOut
	sampleCanalConnector.IdleTimeOut = idleTimeOut
	sampleCanalConnector.RrollbackOnConnect = true
	samplecanal = *sampleCanalConnector

	return sampleCanalConnector

}

func (s SimpleCanalConnector) Connect() {
	if samplecanal.Connected {
		return
	}

	if samplecanal.Running {
		return
	}

	doConnect()
	if samplecanal.Filter != "" {
		Subscribe(samplecanal.Filter)
	}

	if samplecanal.RrollbackOnConnect {

	}

	samplecanal.Connected = true

}

func doConnect() {
	address := samplecanal.Address + ":" + fmt.Sprintf("%d", samplecanal.Port)
	con, err := net.Dial("tcp", address)
	conn = con
	defer conn.Close()
	checkError(err)

	p := new(protocol.Packet)
	data := readNextPacket()
	err = proto.Unmarshal(data, p)
	checkError(err)
	if p != nil {
		if p.GetVersion() != 1 {
			panic("unsupported version at this client.")
		}

		if p.GetType() != protocol.PacketType_HANDSHAKE {
			panic("expect handshake but found other type.")
		}

		handshake := &protocol.Handshake{}
		err = proto.Unmarshal(p.GetBody(), handshake)
		checkError(err)
		pas := []byte(samplecanal.PassWord)
		ca := &protocol.ClientAuth{
			Username:               samplecanal.UserName,
			Password:               pas,
			NetReadTimeoutPresent:  &protocol.ClientAuth_NetReadTimeout{NetReadTimeout: samplecanal.IdleTimeOut},
			NetWriteTimeoutPresent: &protocol.ClientAuth_NetWriteTimeout{NetWriteTimeout: samplecanal.IdleTimeOut},
		}
		caByteArray, _ := proto.Marshal(ca)
		packet := &protocol.Packet{
			Type: protocol.PacketType_CLIENTAUTHENTICATION,
			Body: caByteArray,
		}

		packArray, _ := proto.Marshal(packet)

		WriteWithHeader(packArray)

		pp := readNextPacket()
		pk := &protocol.Packet{}

		err = proto.Unmarshal(pp, pk)
		checkError(err)

		if pk.Type != protocol.PacketType_ACK {
			panic("unexpected packet type when ack is expected")
		}

		ackBody := &protocol.Ack{}
		err = proto.Unmarshal(pk.GetBody(), ackBody)

		if ackBody.GetErrorCode() > 0 {

			panic(errors.New(fmt.Sprintf("something goes wrong when doing authentication:%s", ackBody.GetErrorMessage())))
		}

		samplecanal.Connected = true

	}

}

func readHeaderLength() int {
	buf := make([]byte, 4)
	conn.Read(buf)
	bytesBuffer := bytes.NewBuffer(buf)
	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}

func readNextPacket() []byte {
	readLen := readHeaderLength()
	receiveData := make([]byte, readLen)
	conn.Read(receiveData)
	return receiveData
}

func WriteWithHeader(body []byte) {
	lenth := len(body)
	bytes := getWriteHeaderBytes(lenth)
	conn.Write(bytes)
	conn.Write(body)
}

func getWriteHeaderBytes(lenth int) []byte {
	x := int32(lenth)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

func Subscribe(filter string) {

}

func Rollback() {

}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
