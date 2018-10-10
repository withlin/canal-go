package client

import protocol "canal-go/protocol"

type CanalConnector interface {
	Connect()
	Disconnect()
	CheckValid()
	Subscribe()
	UnSubscribe()
	Get(arg ...interface{}) protocol.Message
	GetWithoutAck(arg ...interface{}) protocol.Message
	Ack(batchId int64)
	Rollback(arg ...interface{})
	StopRunning()
}
