package main

import (
	"fmt"

	"context"
	"log"
	"os"

	"github.com/google/go-jsonnet"

	//"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/router"
	"github.com/gammazero/nexus/v3/router/auth"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

func (imp EtcdImporter) Import(importedFrom, importedPath string) (contents jsonnet.Contents, foundAt string, err error) {
	k, err := imp.cli.KV.Get(context.Background(), "/templates/jsonnet/"+importedPath)
	if err != nil {
		log.Fatal("Could not read jsonnet", importedPath, err)
		return
	}
	fmt.Println("count: ", k.Count)

	return jsonnet.MakeContentsRaw(k.Kvs[0].Value), importedPath, nil
}

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

	t := cli.Txn(context.Background())
	vm := jsonnet.MakeVM()
	vm.Importer(EtcdImporter{cli: cli})

	jsonStr, err := vm.EvaluateFile("test.jsonnet")
	if err != nil {
		log.Fatal(err)
	}
	t.Commit()

	fmt.Println(jsonStr)

	logger := log.New(os.Stderr, "wamp", 0)

	var cfg router.Config
	cfg.Debug = true
	theRouter, err := router.NewRouter(&cfg, logger)
	if err != nil {
		log.Fatal("Could not create wamp router")
		os.Exit(1)
	}

	srv := router.NewWebsocketServer(theRouter)
	closer, err := srv.ListenAndServe("0.0.0.0:8080")
	if err != nil {
		log.Fatal("ListenAndServe failed on a wamp router: ", err)
		os.Exit(1)
	}

	rcfg := router.RealmConfig{
		URI:           "s26",
		AnonymousAuth: false,
	}

	var eks EtcdKeyStore
	eks.cli = cli
	eks.Tenant = "gzh"
	eks.Realm = "s26"
	rcfg.Authenticators = append(rcfg.Authenticators, auth.NewCryptoSignAuthenticator(eks, 0))
	rcfg.Authorizer = ViinexAuthorizer{permissions: defaultViinexPermissions()}
	theRouter.AddRealm(&rcfg)

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
