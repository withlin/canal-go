package com_alibaba_otter_canal_protocol

type ClientIdentity struct {
	Destination string
	ClientId    int
	Filter      string
}

func (c *ClientIdentity) ClientIdentity(destination string, clientId int) *ClientIdentity {
	c.ClientId = clientId
	c.Destination = destination
	return c
}
