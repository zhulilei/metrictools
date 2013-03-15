package metrictools

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"log"
	"net"
	"strconv"
	"sync"
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
	MessageChan         chan WMessage
	NSQList             map[string]int
	lookupdHTTPAddrs    []string
	LookupdPollInterval time.Duration
	TimeOut             time.Duration
	nsqchan             chan []string
	ExitChan            chan int
	sync.Mutex
}

func NewWriter() *Writer {
	return &Writer{
		MessageChan:         make(chan WMessage),
		NSQList:             make(map[string]int),
		LookupdPollInterval: time.Second * 30,
		TimeOut:             time.Second * 5,
		nsqchan:             make(chan []string),
		ExitChan:            make(chan int),
	}
}

func (this *Writer) Stop() {
	close(this.MessageChan)
	close(this.ExitChan)
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

// lookup allo nsqd node, send nsqd node via nsqd_ch
func (this *Writer) ConnectToLookupd(addr string) error {
	for _, x := range this.lookupdHTTPAddrs {
		if x == addr {
			return errors.New("lookupd address already exists")
		}
	}
	this.lookupdHTTPAddrs = append(this.lookupdHTTPAddrs, addr)

	go this.queryLookupd(addr)
	if len(this.lookupdHTTPAddrs) == 1 {
		go this.lookupdLoop()
	}
	return nil
}

func (this *Writer) lookupdLoop() {
	for {
		select {
		case <-this.ExitChan:
			break
		case nsqd_list := <-this.nsqchan:
			for _, nsq_instance := range nsqd_list {
				if _, ok := this.NSQList[nsq_instance]; ok {
					continue
				}
				this.Lock()
				this.NSQList[nsq_instance] = 1
				this.Unlock()
				go func() {
					this.writeLoop(nsq_instance)
					this.Lock()
					delete(this.NSQList, nsq_instance)
					this.Unlock()
				}()
			}
		}
	}
}

//lookup nsqd from lookupd server
func (this *Writer) queryLookupd(lookupaddr string) {
	ticker := time.Tick(this.LookupdPollInterval)
	for {
		var nsqd_list []string
		endpoint := fmt.Sprintf("http://%s/nodes", lookupaddr)
		log.Printf("LOOKUPD: querying %s", endpoint)
		data, err := nsq.ApiRequest(endpoint)
		if err != nil {
			log.Println("Fail to query ", endpoint)
			return
		} else {
			producers := data.Get("producers")
			producersArray, _ := producers.Array()
			for i, _ := range producersArray {
				producer := producers.GetIndex(i)
				address := producer.Get("address").MustString()
				tcpPort := producer.Get("tcp_port").MustInt()
				port := strconv.Itoa(tcpPort)
				nsqd_list = append(nsqd_list, address+":"+port)
			}
		}
		this.nsqchan <- nsqd_list
		<-ticker
	}
}

// send message to nsqd node
func (this *Writer) writeLoop(nsq_instance string) {
	con, err := net.DialTimeout("tcp", nsq_instance, this.TimeOut)
	if err != nil {
		log.Println("connect failed:", err)
		return
	}
	defer con.Close()
	con.Write(nsq.MagicV2)
	rwbuf := bufio.NewReadWriter(bufio.NewReader(con), bufio.NewWriter(con))
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
