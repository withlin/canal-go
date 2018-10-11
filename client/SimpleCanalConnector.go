package client

import (
	"errors"
	"github.com/golang/protobuf/proto"
	protocol "canal-go/protocol"
	"fmt"
	"net"
	"os"
	// "bytes"
	// "encoding/binary"
	"io/ioutil"
)

type SimpleCanalConnector struct {
	Address        string
	Port           int
	UserName       string
	PassWord       string
	SoTime         int32
	IdleTimeOut    int32
	ClientIdentity protocol.ClientIdentity
	Connected      bool
	Running        bool
	Filter         string
	RrollbackOnConnect bool
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

	DoConnect()
	if samplecanal.Filter != "" {
		Subscribe(samplecanal.Filter)
	}

	if samplecanal.RrollbackOnConnect {

	}

	samplecanal.Connected = true

}

func DoConnect() {
	address := samplecanal.Address + ":" +  fmt.Sprintf("%d", samplecanal.Port)
	con, err := net.Dial("tcp", address)
	conn = con
	defer conn.Close()
	checkError(err)
	// ReadHeaderLength() 
	// receiveData, err :=ioutil.ReadAll(conn)
	// checkError(err)
	buf := make([]byte, 1024)
	for {
		lenght, err := conn.Read(buf)
		checkError(err)
		p := &protocol.Packet{}
	    err = proto.Unmarshal(buf[0:lenght],p)
		checkError(err)
	}
	p := &protocol.Packet{}
	// err = proto.Unmarshal(headerBytes,p)
	checkError(err)
	if p != nil{
		if(p.GetVersion() !=1){
			panic("unsupported version at this client.")
		}

		if(p.GetType() != protocol.PacketType_HANDSHAKE){
			panic("expect handshake but found other type.")
		}
		
		handshake :=&protocol.Handshake{}
		err =proto.Unmarshal(p.GetBody(),handshake)
		checkError(err)
		pas,_:=proto.MarshalMessageSetJSON(samplecanal.PassWord)
		ca := protocol.ClientAuth{
			Username:samplecanal.UserName,
			Password:pas,
			NetReadTimeoutPresent:&protocol.ClientAuth_NetReadTimeout{NetReadTimeout:samplecanal.IdleTimeOut},
			NetWriteTimeoutPresent:&protocol.ClientAuth_NetWriteTimeout{NetWriteTimeout:samplecanal.IdleTimeOut},
		}
		cA,_ :=proto.MarshalMessageSetJSON(ca)
		packet := &protocol.Packet{
			Type : protocol.PacketType_CLIENTAUTHENTICATION,
			Body:cA,
		}

		packArray,_ :=proto.MarshalMessageSetJSON(packet)
		conn.Write(packArray)

		readData, err :=ioutil.ReadAll(conn)
	    checkError(err)
		pk := &protocol.Packet{}
		
		err = proto.Unmarshal(readData,pk)
		checkError(err)
		
		if pk.Type != protocol.PacketType_ACK {
			panic("unexpected packet type when ack is expected")
		}

		ackBody := &protocol.Ack{}
		err = proto.Unmarshal(pk.GetBody(),ackBody)
		
		if ackBody.GetErrorCode()>0{

			panic(errors.New(fmt.Sprintf("something goes wrong when doing authentication:%s",ackBody.GetErrorMessage())))
		}

		samplecanal.Connected = true
		

	}

}


func Subscribe(filter string){

}

func Rollback(){

}

// func ReadNextPacket() {
// 	var buf  bytes.Buffer
// 	headerlen := ReadHeaderLength()
// 	receiveData :=make([]byte,2*1024)
// 	while(headerlen >0) {
// 		len := conn.Read()
// 	}
// }

func ReadHeaderLength() int {
	headerBytes := make([]byte,1024)
	for{
		_, err :=conn.Read(headerBytes)
		checkError(err)
	}
	
}

func reverse(input []byte) []byte {
    if len(input) == 0 {
        return input
    }
    return append(reverse(input[1:]), input[0]) 
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
