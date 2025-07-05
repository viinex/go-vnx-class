package main

import (
	"errors"
	"fmt"

	"context"
	"log"
	"os"

	"github.com/google/go-jsonnet"

	//"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/router"
	"github.com/gammazero/nexus/v3/router/auth"

	"github.com/gammazero/nexus/v3/wamp"

	etcdv3 "go.etcd.io/etcd/client/v3"

	hex "encoding/hex"

	yaml "gopkg.in/yaml.v3"
)

type EtcdImporter struct {
	cli *etcdv3.Client
}

func (imp EtcdImporter) Import(importedFrom, importedPath string) (contents jsonnet.Contents, foundAt string, err error) {
	k, err := imp.cli.KV.Get(context.Background(), "/templates/jsonnet/"+importedPath)
	if err != nil {
		log.Fatal("Could not read jsonnet", importedPath, err)
		return
	}
	fmt.Println("count: ", k.Count)

	return jsonnet.MakeContentsRaw(k.Kvs[0].Value), importedPath, nil
}

type EtcdKeyStore struct {
	EtcdImporter
	Tenant string
	Realm  string
}

// AuthRole implements auth.KeyStore.
func (ksi EtcdKeyStore) AuthRole(authid string) (string, error) {
	k, err := ksi.cli.KV.Get(context.Background(), "/config/"+ksi.Tenant+"/"+ksi.Realm+"/wamp/"+authid+"/role")
	if err != nil {
		return "", err
	}
	if len(k.Kvs) != 1 {
		return "", errors.New("key 'role' not found")
	}
	return string(k.Kvs[0].Value), nil
}

// PasswordInfo implements auth.KeyStore.
func (ksi EtcdKeyStore) PasswordInfo(authid string) (salt string, keylen int, iterations int) {
	return "", 0, 0
}

// Provider implements auth.KeyStore.
func (ksi EtcdKeyStore) Provider() string {
	return "EtcdKeyStore"
}

type WampKeyStoreData struct {
	Realm string              `yaml:"realm"`
	Roles map[string][]string `yaml:"roles"`
	Creds map[string]string   `yaml:"creds"`
}

func (ksi EtcdKeyStore) AuthKey1(authid, authmethod string) ([]byte, error) {
	if authmethod != "cryptosign" {
		return nil, fmt.Errorf("unsupported authmethod %s", authmethod)
	}
	k, err := ksi.cli.KV.Get(context.Background(), "/config/"+ksi.Tenant+"/"+ksi.Realm+"/wamp.yaml")
	if err != nil {
		return nil, fmt.Errorf("failed to get wamp.yaml from etcd: %w", err)
	}
	var wks WampKeyStoreData
	v := k.Kvs[0].Value
	err = yaml.Unmarshal(v, &wks)
	if err != nil {
		return nil, fmt.Errorf("failed to decode wamp.yaml: %w", err)
	}
	keyHex, ok := wks.Creds[authid]
	if !ok {
		return nil, fmt.Errorf("authid %s not found", authid)
	}
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("failed to base16 decode value: %w", err)
	}
	return key, nil
}
func (ksi EtcdKeyStore) AuthKey(authid, authmethod string) ([]byte, error) {
	if authmethod != "cryptosign" {
		return nil, fmt.Errorf("unsupported authmethod %s", authmethod)
	}
	k, err := ksi.cli.KV.Get(context.Background(), "/config/"+ksi.Tenant+"/"+ksi.Realm+"/wamp/"+authid+"/cryptosign")
	if err != nil {
		return nil, err
	}
	if len(k.Kvs) != 1 {
		return nil, errors.New("key 'cryptosign' not found")
	}
	keyHex := k.Kvs[0].Value
	key, err := hex.DecodeString(string(keyHex))
	if err != nil {
		return nil, fmt.Errorf("not a base16 string: %w", err)
	}
	if len(key) != 32 {
		return nil, errors.New("cryptosign public key length should be 32")
	}
	return key, nil
}

type WampPermissions map[string]map[wamp.MessageType][]wamp.URI

func defaultViinexPermissions() WampPermissions {
	res := WampPermissions{
		"viinex": {
			wamp.PUBLISH:   {"com.viinex.api"},
			wamp.SUBSCRIBE: {"com.viinex.api", "com.viinex.infra"},
			wamp.CALL:      {"com.viinex.api", "com.viinex.infra"},
			wamp.REGISTER:  {"com.viinex.api"},
		},
		"user": {
			wamp.SUBSCRIBE: {"com.viinex", "wamp.registration"},
			wamp.CALL:      {"com.viinex", "wamp.registration"},
		},
		"operator": {
			wamp.SUBSCRIBE: {"com.viinex", "com.viinex.infra", "wamp.registration"},
			wamp.CALL:      {"com.viinex", "com.viinex.infra", "wamp.registration"},
		},
	}
	return res
}

type ViinexAuthorizer struct {
	permissions WampPermissions
}

func (va ViinexAuthorizer) Authorize(sess *wamp.Session, msg wamp.Message) (bool, error) {
	msgtype := msg.MessageType()
	if msgtype != wamp.PUBLISH && msgtype != wamp.SUBSCRIBE && msgtype != wamp.CALL && msgtype != wamp.REGISTER {
		return true, nil
	}
	role, ok := sess.Details["authrole"]
	if !ok {
		return false, errors.New("undefined authrole")
	}
	if role == "root" {
		return true, nil
	}
	var uri wamp.URI
	switch msg := msg.(type) {
	case *wamp.Call:
		uri = msg.Procedure
	case *wamp.Register:
		uri = msg.Procedure
	case *wamp.Publish:
		uri = msg.Topic
	case *wamp.Subscribe:
		uri = msg.Topic
	default:
		return false, nil
	}
	permissions, ok := va.permissions[role.(string)]
	if !ok {
		return false, nil
	}
	prefixes, ok := permissions[msg.MessageType()]
	if !ok {
		return false, nil
	}
	for _, p := range prefixes {
		if uri.PrefixMatch(p) {
			return true, nil
		}
	}
	return true, nil
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
