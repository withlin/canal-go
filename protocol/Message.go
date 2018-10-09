package com_alibaba_otter_canal_protocol

type Message struct {
	Id         int64
	Entries    interface{}
	Raw        bool
	RawEntries interface{}
}

func NewMessage(id int64) *Message {
	message := &Message{Id: id, Entries: nil, Raw: false, RawEntries: nil}
	return message
}
