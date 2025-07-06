package main

import (
	"flag"
	"log"
	"os"

	//"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/router"
	"gopkg.in/yaml.v3"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

type Config struct {
	Etcd       etcdv3.Config `json:"etcd"`
	Wamp       string        `json:"wamp"`
	Prometheus string        `json:"prometheus"`
}

func main() {
	configPath := flag.String("config", "vnxclass.yaml", "Path to configuration file")
	confBytes, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatal("could not read config: %w", err)
	}
	config := Config{
		Etcd: etcdv3.Config{
			Endpoints: []string{"127.0.0.1:2379"},
		},
		Wamp: "0.0.0.0:8080",
	}
	err = yaml.Unmarshal(confBytes, &config)
	if err != nil {
		log.Fatal("could not deserialize config: %w", err)
	}
	cli, err := etcdv3.New(config.Etcd)
	if err != nil {
		log.Fatal("Could not open etcd client", err)
	}

	logger := log.New(os.Stderr, "wamp", 0)

	var cfg router.Config
	cfg.Debug = true
	theRouter, err := router.NewRouter(&cfg, logger)
	if err != nil {
		log.Fatal("could not create wamp router: %w", err)
	}

	srv := router.NewWebsocketServer(theRouter)
	closer, err := srv.ListenAndServe(config.Wamp)
	if err != nil {
		log.Fatal("ListenAndServe failed on a wamp router: %w", err)
	}

	imp := EtcdClient{cli: cli}

	tenantProjectsMap, err := imp.GetTenantsAndProjects()
	if err != nil {
		log.Fatal("failed to build map of tenants and projects: %w", err)
	}

	err = imp.PopulateWampRealms(theRouter, tenantProjectsMap)
	if err != nil {
		log.Fatal("could not populate wamp realms: %w", err)
	}

	defer closer.Close()

	quit := make(chan string)
	<-quit
}
