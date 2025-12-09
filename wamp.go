package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	//"log"
	//"os"

	//"github.com/gammazero/nexus/v3/client"
	//"github.com/gammazero/nexus/v3/router"
	"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/router/auth"

	"github.com/gammazero/nexus/v3/router"
	"github.com/gammazero/nexus/v3/wamp"

	hex "encoding/hex"

	yaml "gopkg.in/yaml.v3"
)

type WampKeyStoreData struct {
	Realm string              `yaml:"realm"`
	Roles map[string][]string `yaml:"roles"`
	Creds map[string]string   `yaml:"creds"`
}

func (ksi EtcdKeyStore) AuthKey1(authid, authmethod string) ([]byte, error) {
	if authmethod != "cryptosign" {
		return nil, fmt.Errorf("unsupported authmethod %s", authmethod)
	}
	k, err := ksi.cli.KV.Get(context.Background(), ksi.GetRealmConfigKeyPath("wamp.yaml"))
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
	k, err := ksi.cli.KV.Get(context.Background(), ksi.GetRealmConfigKeyPath("wamp/"+authid+"/cryptosign"))
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
			wamp.SUBSCRIBE: {"com.viinex.api", "wamp.registration"},
			wamp.CALL:      {"com.viinex.api", "wamp.registration"},
		},
		"operator": {
			wamp.SUBSCRIBE: {"com.viinex.api", "com.viinex.infra", "wamp.registration"},
			wamp.CALL:      {"com.viinex.api", "com.viinex.infra", "wamp.registration"},
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
	return false, nil
}

// AuthRole implements auth.KeyStore.
func (ksi EtcdKeyStore) AuthRole(authid string) (string, error) {
	k, err := ksi.cli.KV.Get(context.Background(), ksi.GetRealmConfigKeyPath("wamp/"+authid+"/role"))
	if err != nil {
		return "", err
	}
	if len(k.Kvs) != 1 {
		return "", fmt.Errorf("key 'role' not found while looking up for authid %s in realm %s of tenant %s", authid, ksi.Realm, ksi.Tenant)
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

func (rms *RealmManagers) createRealmManager(theRouter router.Router, prometheusUrl string, eks EtcdKeyStore) error {
	rms.mutex.Lock()
	for _, rm := range rms.realmManagers {
		if rm.Tenant == eks.Tenant && rm.Realm == eks.Realm {
			rms.mutex.Unlock()
			return nil // not an error: realm already exists, we may receive update on recipe.yaml change here
		}
	}
	rms.mutex.Unlock()

	rcfg := router.RealmConfig{
		URI:           wamp.URI(eks.Realm),
		AnonymousAuth: false,
	}

	rcfg.Authenticators = append(rcfg.Authenticators, auth.NewCryptoSignAuthenticator(eks, 0))
	rcfg.Authorizer = ViinexAuthorizer{permissions: defaultViinexPermissions()}

	ccfg := client.Config{
		Realm: eks.Realm,
	}
	err := theRouter.AddRealm(&rcfg)
	if err != nil {
		return fmt.Errorf("could not add realm: %w", err)
	}
	// create a local client and register infra endpoints
	wampClient, err := client.ConnectLocal(theRouter, ccfg)
	if err != nil {
		theRouter.RemoveRealm(rcfg.URI)
		return fmt.Errorf("could not create a local client on realm %s: %w", ccfg.Realm, err)
	}
	// wampClient.Close() should be called at some point
	err = wampClient.Register("com.viinex.infra.get_cluster_config", eks.GetClusterConfigHandler, nil)
	if err != nil {
		wampClient.Close()
		theRouter.RemoveRealm(rcfg.URI)
		return fmt.Errorf("could not register com.viinex.infra.get_cluster_config: %w", err)
	}

	rm, err := rms.CreateRealmManager(eks, wampClient, prometheusUrl)
	if err != nil {
		wampClient.Close()
		theRouter.RemoveRealm(rcfg.URI)
		return fmt.Errorf("failed to create realm manager: %w", err)
	}

	rm.mutex.Lock()
	rm.realmCloser = func() { theRouter.RemoveRealm(rcfg.URI) }
	rm.mutex.Unlock()
	return nil
}

func (imp EtcdClient) PopulateWampRealms(theRouter router.Router, tenantProjectsMap map[string][]string, prometheusUrl string) (io.Closer, error) {
	rms := RealmManagers{}
	rms.cancelEtcdWatch = imp.WatchTenantAndProjectChanges(func(tenant string, project string) {
		var eks EtcdKeyStore
		eks.EtcdClient = imp
		eks.Tenant = tenant
		eks.Realm = project
		err := rms.createRealmManager(theRouter, prometheusUrl, eks)
		if err != nil {
			log.Printf("failed to create launch manager for realm %s of tenant %s: %s", eks.Realm, eks.Tenant, err)
			// and what? retry over time?
		}
	})
	for tenant, projects := range tenantProjectsMap {
		for _, project := range projects {
			var eks EtcdKeyStore
			eks.EtcdClient = imp
			eks.Tenant = tenant
			eks.Realm = project
			err := rms.createRealmManager(theRouter, prometheusUrl, eks)
			if err != nil {
				log.Printf("failed to create launch manager for realm %s of tenant %s: %s", eks.Realm, eks.Tenant, err)
				// and what? retry over time?
			}
		}
	}
	return &rms, nil
}

const ErrWampInfraHanlerFailed wamp.URI = "com.viinex.infra.failed"

func (eks EtcdKeyStore) GetClusterConfigHandler(ctx context.Context, inv *wamp.Invocation) client.InvokeResult {
	if len(inv.Arguments) != 1 {
		log.Print("there should be exactly 1 string argument to com.viinex.infra.get_cluster_config call")
		return client.InvokeResult{Err: wamp.ErrInvalidArgument}
	}
	clusterName, ok := inv.Arguments[0].(string)
	if !ok {
		log.Print("there should be exactly 1 argument to com.viinex.infra.get_cluster_config call")
		return client.InvokeResult{Err: wamp.ErrInvalidArgument}
	}

	resConfig, err := eks.GetClusterConfig(ctx, clusterName)

	if err != nil {
		log.Printf("could not GetClusterConfig: %s\n", err)
		return client.InvokeResult{Err: ErrWampInfraHanlerFailed, Args: wamp.List{err.Error()}}
	}

	res := client.InvokeResult{
		Args: wamp.List{resConfig},
	}
	return res
}
