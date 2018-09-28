package main

import (
	"bufio"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/metrics/statsd"
	"github.com/nsqio/go-nsq"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type message struct {
	topic      string
	body       [][]byte
	ResultChan chan error
}

type FileSetting struct {
	Name       string `json:"Name"`
	BatchSize  int    `json:"BatchSize"`
	StartAtEOF bool   `json:"StartAtEOF"`
}
type LogTask struct {
	Version      string        `json:"Version"`
	SinkType     string        `json:"SinkType"`
	SinkAddress  string        `json:"SinkAddress"`
	Topic        string        `json:"Topic"`
	FileSettings []FileSetting `json:"FileSettings"`
	statsd       *statsd.Statsd
	msgChan      chan *message
	exitChan     chan int
}

func (m *LogTask) Start() {
	m.exitChan = make(chan int)
	m.msgChan = make(chan *message)
	for _, file := range m.FileSettings {
		go m.ReadLog(file)
	}
	switch m.SinkType {
	case "nsq":
		cfg := nsq.NewConfig()
		hostname, err := os.Hostname()
		cfg.Set("user_agent", fmt.Sprintf("file_to_nsq/%s", hostname))
		cfg.Set("snappy", true)
		w, err := nsq.NewProducer(m.SinkAddress, cfg)
		if err != nil {
			fmt.Println("init nsq producer err", err)
		}
		go m.NSQWriteLoop(w)
	case "kafka":
		config := sarama.NewConfig()
		config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
		config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
		config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
		producer, err := sarama.NewAsyncProducer(strings.Split(m.SinkAddress, ","), nil)
		if err != nil {
			fmt.Println("init kafka producer err", err)
		}
		go m.KafkaWriteLoop(producer)
	default:
		fmt.Println("not support")
	}
}
func (m *LogTask) Stop() {
	close(m.exitChan)
}

func (m *LogTask) ReadLog(file FileSetting) {
	if m.SinkType == "kafka" {
		file.BatchSize = 0
	}

	fd, err := os.Open(file.Name)
	if err != nil {
		log.Println(err)
		return
	}
	defer fd.Close()
	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		return
	}
	if file.StartAtEOF {
		_, err = fd.Seek(0, io.SeekEnd)
		if err != nil {
			return
		}
		log.Println("reading from EOF")
	}
	log.Println("reading ", file.Name)
	reader := bufio.NewReader(fd)
	retryCount := 0
	var body [][]byte
	readcount := m.statsd.NewCounter(fmt.Sprintf("%s_readline", file.Name), 1.0)
	for {
		select {
		case <-m.exitChan:
			return
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				time.Sleep(time.Second)
				retryCount++
				line, err = reader.ReadString('\n')
			}
			if err == io.EOF {
				log.Println(file, "READ EOF")
				size0, err := fd.Seek(0, io.SeekCurrent)
				if err != nil {
					return
				}
				fd, err = os.Open(file.Name)
				if err != nil {
					log.Println("open failed", err)
					return
				}
				size1, err := fd.Seek(0, io.SeekEnd)
				if err != nil {
					log.Println(err)
				}
				if size1 < size0 {
					fd.Seek(0, io.SeekCurrent)
				} else {
					fd.Seek(size0, io.SeekStart)
				}
				reader = bufio.NewReader(fd)
				if (len(body) == 0) || (retryCount < 5) {
					continue
				} else {
					err = nil
				}
			}
			if err != nil {
				log.Println(err)
				return
			}
			if line != "" {
				body = append(body, []byte(line))
			}
			readcount.Add(1)
			if len(body) < file.BatchSize {
				continue
			}
			retryCount = 0
			msg := &message{
				topic:      m.Topic,
				body:       body,
				ResultChan: make(chan error),
			}
			m.msgChan <- msg
			for {
				err := <-msg.ResultChan
				if err == nil {
					break
				}
				time.Sleep(time.Second)
				m.msgChan <- msg
			}
			body = body[:0]
		}
	}
}

func (m *LogTask) NSQWriteLoop(producer *nsq.Producer) {
	defer producer.Stop()
	writecount := m.statsd.NewCounter("nsq_write", 1.0)
	for {
		select {
		case <-m.exitChan:
			return
		case msg := <-m.msgChan:
			var err error
			if len(msg.body) > 1 {
				err = producer.MultiPublish(msg.topic, msg.body)
			} else {
				err = producer.Publish(msg.topic, msg.body[0])
			}
			writecount.Add(1)
			msg.ResultChan <- err
		}
	}
}

func (m *LogTask) KafkaWriteLoop(producer sarama.AsyncProducer) {
	defer producer.Close()
	writecount := m.statsd.NewCounter("kafka_write", 1.0)

	for msg := range m.msgChan {
		select {
		case <-m.exitChan:
			return
		case producer.Input() <- &sarama.ProducerMessage{Topic: m.Topic, Key: nil, Value: sarama.StringEncoder(msg.body[0])}:
			msg.ResultChan <- nil
			writecount.Add(1)
		case err := <-producer.Errors():
			fmt.Println(err.Err)
		}
	}
}
