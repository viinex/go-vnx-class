package main

import (
	"log"
	"os"

	//"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/router"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	cli, err := etcdv3.New(etcdv3.Config{
		Endpoints: []string{"192.168.0.103:2379"},
		Username:  "vnxclass",
		Password:  "vnxclass",
	})
	if err != nil {
		log.Fatal("Could not open etcd client", err)
		return
	}

	//t := cli.Txn(context.Background())
	// vm := jsonnet.MakeVM()
	// vm.Importer(EtcdImporter{cli: cli})

	// jsonStr, err := vm.EvaluateFile("test.jsonnet")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// t.Commit()

	// fmt.Println(jsonStr)

	logger := log.New(os.Stderr, "wamp", 0)

	var cfg router.Config
	cfg.Debug = true
	theRouter, err := router.NewRouter(&cfg, logger)
	if err != nil {
		log.Fatal("could not create wamp router: %w", err)
		os.Exit(1)
	}

	srv := router.NewWebsocketServer(theRouter)
	closer, err := srv.ListenAndServe("0.0.0.0:8080")
	if err != nil {
		log.Fatal("ListenAndServe failed on a wamp router: %w", err)
		os.Exit(1)
	}

	imp := EtcdImporter{cli: cli}

	tenantProjectsMap, err := imp.GetTenantsAndProjects()
	if err != nil {
		log.Fatal("failed to build map of tenatnts and projects: %w", err)
		os.Exit(1)
	}

	err = imp.PopulateWampRealms(theRouter, tenantProjectsMap)
	if err != nil {
		log.Fatal("could not populate wamp realms: %w", err)
		os.Exit(1)
	}

	defer closer.Close()

	quit := make(chan string)
	<-quit

	/*
	   {
	     "person1": {
	         "name": "Alice",
	         "welcome": "Hello Alice!"
	     },
	     "person2": {
	         "name": "Bob",
	         "welcome": "Hello Bob!"
	     }
	   }
	*/
}
