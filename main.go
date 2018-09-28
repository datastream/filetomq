package main

import (
	"encoding/json"
	"flag"
	"fmt"
	kitlog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics/statsd"
	"github.com/hashicorp/consul/api"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	conf_file = flag.String("conf", "config.json", "config file")
)

func main() {
	flag.Parse()
	m, err := ReadConfig(*conf_file)
	if err != nil {
		log.Fatal(err)
	}
	// statd metric
	logger := kitlog.NewJSONLogger(kitlog.NewSyncWriter(os.Stdout))
	m.statsd = statsd.New("filetomq.", logger)
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	go m.statsd.SendLoop(report.C, "udp", m.StatsdAddr)
	go m.Tasks()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	close(m.exitChan)
	for _, t := range m.taskList {
		t.Stop()
	}
}

type MainTask struct {
	// consul
	ConsulAddress string `json:"ConsulAddress"`
	DataCenter    string `json:"Datacenter"`
	ClusterName   string `json:"Cluster"`
	Token         string `json:"Token"`
	client        *api.Client
	// statsd
	StatsdAddr string `json:"StatsdAddr"`
	statsd     *statsd.Statsd
	// task control
	taskList map[string]*LogTask
	exitChan chan int
}

func (m *MainTask) Tasks() {
	m.exitChan = make(chan int)
	m.taskList = make(map[string]*LogTask)
	ticker := time.Tick(time.Second * 600)
	config := api.DefaultConfig()
	config.Address = m.ConsulAddress
	config.Datacenter = m.DataCenter
	config.Token = m.Token
	var err error
	m.client, err = api.NewClient(config)
	if err != nil {
		fmt.Println("reload consul setting failed", err)
	}
	err = m.CheckConfig()
	if err != nil {
		fmt.Println("reload consul setting failed", err)
	}
	for {
		select {
		case <-ticker:
			err = m.CheckConfig()
			if err != nil {
				fmt.Println("reload consul setting failed", err)
			}
		case <-m.exitChan:
			return
		}
	}

}

func (m *MainTask) CheckConfig() error {
	kv := m.client.KV()
	rst, _, err := kv.Get(m.ClusterName, nil)
	if err != nil {
		return err
	}
	var tasks map[string]LogTask
	err = json.Unmarshal(rst.Value, &tasks)
	if err != nil {
		return err
	}
	for k, _ := range m.taskList {
		if _, ok := tasks[k]; ok {
			if tasks[k].Version == m.taskList[k].Version {
				delete(tasks, k)
				continue
			}
		}
		m.taskList[k].Stop()
		delete(m.taskList, k)
	}
	for k, v := range tasks {
		v.statsd = m.statsd
		v.Start()
		m.taskList[k] = &v
	}
	return nil
}

func ReadConfig(file string) (*MainTask, error) {
	m := &MainTask{}
	config_file, err := os.Open(file)
	config, err := ioutil.ReadAll(config_file)
	if err != nil {
		return nil, err
	}
	defer config_file.Close()
	if err := json.Unmarshal(config, m); err != nil {
		return nil, err
	}
	if m.StatsdAddr == "" {
		m.StatsdAddr = "127.0.0.1:8125"
	}
	return m, nil
}
