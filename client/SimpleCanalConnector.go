package client

import (
	protocol "canal-go/protocol"
	"fmt"
	"net"
	"os"
	"bytes"
	"encoding/binary"
)

type SimpleCanalConnector struct {
	Address        string
	Port           int
	UserName       string
	PassWord       string
	SoTime         int
	IdleTimeOut    int
	ClientIdentity protocol.ClientIdentity
	Connected      bool
	Running        bool
	Filter         string
}

var (
	samplecanal SimpleCanalConnector
	conn        net.Conn
)

func NewSimpleCanalConnector(address string, port int, username string, password string, destination string, soTimeOut int, idleTimeOut int) *SimpleCanalConnector {
	sampleCanalConnector := &SimpleCanalConnector{}
	sampleCanalConnector.Address = address
	sampleCanalConnector.Port = port
	sampleCanalConnector.UserName = username
	sampleCanalConnector.PassWord = password
	sampleCanalConnector.ClientIdentity = protocol.ClientIdentity{Destination: destination, ClientId: 1001}
	sampleCanalConnector.SoTime = soTimeOut
	sampleCanalConnector.IdleTimeOut = idleTimeOut
	samplecanal = *sampleCanalConnector
	return sampleCanalConnector

}

func Connect() {
	if samplecanal.Connected {
		return
	}

	if samplecanal.Running {
		return
	}

}

func DoConnect() {
	address := samplecanal.Address + ":" + string(samplecanal.Port)
	conn, err := net.Dial("tcp", address)
	defer conn.Close()
	checkError(err)
	// p := &protocol.Packet{}
	// err := proto.Unmarshal(,p)

}

func ReadNextPacket() {

}

func ReadHeaderLength() int {
	headerBytes := make([]byte,4)
	_, err :=conn.Read(headerBytes)
	checkError(err)
	var len int
    _ =  binary.Read(bytes.NewReader(headerBytes), binary.BigEndian, &len)
	return len
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
