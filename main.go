package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	//"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/router"
	"gopkg.in/yaml.v3"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

type Config struct {
	Etcd       etcdv3.Config `json:"etcd"`
	EtcdPrefix string        `json:"etcd-prefix"`
	Wamp       string        `json:"wamp"`
	Prometheus string        `json:"prometheus-push-uri"`
	Static     string        `json:"static"`
	Debug      bool          `json:"debug"`
}

func main() {
	config := Config{
		Etcd: etcdv3.Config{
			Endpoints: []string{"127.0.0.1:2379"},
		},
		Wamp:   "0.0.0.0:8080",
		Static: "/usr/share/viinex/web/browser/en",
		Debug:  false,
	}

	configPath := flag.String("config", "vnxclass.yaml", "Path to configuration file")
	flag.Parse()

	if *configPath != "" {
		confBytes, err := os.ReadFile(*configPath)
		if err != nil {
			log.Fatal("could not read config: ", err)
		}
		err = yaml.Unmarshal(confBytes, &config)
		if err != nil {
			log.Fatal("could not deserialize config: ", err)
		}
	}
	cli, err := etcdv3.New(config.Etcd)
	if err != nil {
		log.Fatal("Could not open etcd client", err)
	}

	//logger := log.New(os.Stderr, "[wamp] ", 0)

	var cfg router.Config
	cfg.Debug = config.Debug
	theRouter, err := router.NewRouter(&cfg, nil)
	if err != nil {
		log.Fatal("could not create wamp router: ", err)
	}

	srv := router.NewWebsocketServer(theRouter)
	if err != nil {
		log.Fatal("ListenAndServe failed on a wamp router: ", err)
	}

	imp := EtcdClient{cli: cli, prefix: config.EtcdPrefix}

	tenantProjectsMap, err := imp.GetTenantsAndProjects()
	if err != nil {
		log.Fatal("failed to build map of tenants and projects: ", err)
	}

	closer, err := imp.PopulateWampRealms(theRouter, tenantProjectsMap, config.Prometheus)
	if err != nil {
		log.Fatal("could not populate wamp realms: ", err)
	}
	defer closer.Close()

	http.HandleFunc("/ws", srv.ServeHTTP)
	fs := http.FileServer(http.Dir(config.Static))
	http.Handle("/", http.StripPrefix("/", fs))

	err = http.ListenAndServe(config.Wamp, nil)
	if err != nil {
		log.Fatal("could not serve http: ", err)
	}

	//defer closer.Close()

	quit := make(chan string)
	<-quit
}
