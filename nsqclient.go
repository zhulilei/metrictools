package metrictools

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/bitly/nsq/nsq"
	"log"
	"net"
	"time"
)

// define write message struct
type WMessage struct {
	Topic string
	Body  interface{}
	Stat  chan error
}

// may be should add more info in writer
type Writer struct {
	MessageChan chan WMessage
	net.Conn
}

func NewWriter(addr string) *Writer {
	this := &Writer{
		MessageChan: make(chan WMessage),
	}
	var err error
	this.Conn, err = net.DialTimeout("tcp", addr, time.Second*5)
	if err != nil {
		log.Println("connect failed:", err)
		return nil
	}
	go this.WriteLoop()
	return this
}

func (this *Writer) Stop() {
	close(this.MessageChan)
	this.Conn.Close()
}

// body should be []byte or [][]byte
func (this *Writer) Write(topic string, body interface{}) error {
	msg := WMessage{
		Topic: topic,
		Body:  body,
		Stat:  make(chan error),
	}
	this.MessageChan <- msg
	return <-msg.Stat
}

// send message to nsqd node
func (this *Writer) WriteLoop() {
	this.Conn.Write(nsq.MagicV2)
	rwbuf := bufio.NewReadWriter(bufio.NewReader(this.Conn),
		bufio.NewWriter(this.Conn))
	var err error
	for {
		msg, ok := <-this.MessageChan
		if !ok {
			break
		}
		var cmd *nsq.Command
		switch msg.Body.(type) {
		case []byte:
			cmd = nsq.Publish(msg.Topic, msg.Body.([]byte))
		case [][]byte:
			cmd, err = nsq.MultiPublish(msg.Topic, msg.Body.([][]byte))
		default:
			err = errors.New("wrong data format")
		}
		if err != nil {
			msg.Stat <- err
			break
		}
		if err = cmd.Write(rwbuf); err != nil {
			log.Println("write buf error", err)
			msg.Stat <- err
			break
		}
		if err = rwbuf.Flush(); err != nil {
			log.Println("flush buf error", err)
			msg.Stat <- err
			break
		}
		resp, err := nsq.ReadResponse(rwbuf)
		if err != nil {
			log.Println("failed to read response", err)
			msg.Stat <- err
			break
		}
		_, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			log.Println("unpack failed", err)
			continue
		}
		if !bytes.Equal(data, []byte("OK")) &&
			!bytes.Equal(data, []byte("_heartbeat_")) {
			log.Println("response not ok", string(data))
			continue
		}
		msg.Stat <- err
	}
}
