package main

import (

	//"log"
	//"os"

	//"github.com/gammazero/nexus/v3/client"
	//"github.com/gammazero/nexus/v3/router"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gammazero/nexus/v3/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"

	"github.com/gammazero/nexus/v3/wamp"
)

type RealmManager struct {
	EtcdKeyStore
	mapping Mapping

	wampClient *client.Client
	quit       chan struct{}
	regEvents  chan *wamp.Event

	regMutex         sync.RWMutex
	instances        map[wamp.ID]InstanceInfo
	clusters         map[wamp.ID]ClusterInfo
	metricsProviders map[wamp.ID]wamp.URI
}

type InstanceInfo struct {
	name string
	uri  wamp.URI
}

type ClusterInfo struct {
	name string
	uri  wamp.URI
}

type RealmManagers struct {
	realmManagers []*RealmManager
}

func (rms RealmManagers) Close() error {
	for _, rm := range rms.realmManagers {
		rm.quit <- struct{}{}
	}
	return nil
}

func (rm *RealmManager) RunManageRealm() {
FOR:
	for {
	SEL:
		select {
		case <-rm.quit:
			log.Print("quitting realm manager for ", rm.Realm)
			break FOR
		case <-rm.wampClient.Done():
			log.Print("quitting realm manager for ", rm.Realm)
			break FOR
		case regEvent := <-rm.regEvents:
			topic, ok := regEvent.Details["topic"].(wamp.URI)
			if !ok {
				break SEL
			}
			if len(regEvent.Arguments) < 2 {
				break SEL
			}
			switch topic {
			case "wamp.registration.on_create":
				details, ok := regEvent.Arguments[1].(wamp.Dict)
				if !ok {
					break SEL
				}
				uri, ok := details["uri"].(wamp.URI)
				if !ok {
					break SEL
				}
				id, ok := details["id"].(wamp.ID)
				if !ok {
					break SEL
				}

				if instance, ok := rm.mapping.MatchInstance(uri); ok {
					rm.registerInstance(uri, id, instance)
				} else if cluster, ok := rm.mapping.MatchCluster(uri); ok {
					rm.registerCluster(uri, id, cluster)
				}
			case "wamp.registration.on_register":
				id, ok := regEvent.Arguments[1].(wamp.ID)
				if !ok {
					break SEL
				}
				rm.registrationAlive(id)
			case "wamp.registration.on_delete":
				id, ok := regEvent.Arguments[1].(wamp.ID)
				if !ok {
					break SEL
				}
				rm.deregister(id)
				// uri := reg
			}
		}
	}
}

func (rms RealmManagers) RealmManager(eks EtcdKeyStore, wampClient *client.Client) (*RealmManager, error) {
	rm := RealmManager{
		EtcdKeyStore:     eks,
		wampClient:       wampClient,
		mapping:          MappingNone{},
		quit:             make(chan struct{}),
		regEvents:        make(chan *wamp.Event, 100),
		instances:        make(map[wamp.ID]InstanceInfo),
		clusters:         make(map[wamp.ID]ClusterInfo),
		metricsProviders: make(map[wamp.ID]wamp.URI),
	}
	mappingResp, err := eks.cli.KV.Get(context.Background(), eks.GetRealmKeyPath("mapping.yaml"))
	if err != nil {
		return nil, fmt.Errorf("fail to contact etcd: %w", err)
	}
	if len(mappingResp.Kvs) != 0 {
		r := PolymorphicMapping{}
		err = yaml.Unmarshal(mappingResp.Kvs[0].Value, &r)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cluster to instance mapping: %w", err)
		}
		rm.mapping = r
	}
	rms.realmManagers = append(rms.realmManagers, &rm)
	go rm.RunManageRealm()
	return &rm, nil
}

func (rm *RealmManager) registerInstance(uri wamp.URI, id wamp.ID, instance string) error {
	rm.regMutex.Lock()
	defer rm.regMutex.Unlock()
	_, ok := rm.instances[id]
	if ok {
		return errors.New("registration already present for the instance")
	}
	rm.instances[id] = InstanceInfo{
		uri:  uri,
		name: instance,
	}
	log.Printf("RealmManager.registerInstance: added registration %d for instance %s at %s", id, instance, uri)
	return nil
}
func (rm *RealmManager) registerCluster(uri wamp.URI, id wamp.ID, cluster string) error {
	rm.regMutex.Lock()
	defer rm.regMutex.Unlock()
	_, ok := rm.clusters[id]
	if ok {
		return errors.New("registration already present for the cluster")
	}
	rm.clusters[id] = ClusterInfo{
		uri:  uri,
		name: cluster,
	}
	log.Printf("RealmManager.registerCluster: added registration %d for cluster %s at %s", id, cluster, uri)
	return nil
}

func (rm *RealmManager) deregister(id wamp.ID) {
	rm.regMutex.Lock()
	defer rm.regMutex.Unlock()
	instance, ok := rm.instances[id]
	if ok {
		delete(rm.instances, id)
		log.Printf("RealmManager.deregister: removed registration %d for instance %s at %s", id, instance.name, instance.uri)
		return
	}
	cluster, ok := rm.clusters[id]
	if ok {
		delete(rm.clusters, id)
		log.Printf("RealmManager.deregister: removed registration %d for cluster %s at %s", id, cluster.name, instance.uri)
		return
	}
}

func (rm *RealmManager) registrationAlive(id wamp.ID) {
	rm.regMutex.Lock()
	defer rm.regMutex.Unlock()
	instance, ok := rm.instances[id]
	if ok {
		log.Printf("RealmManager.registrationAlive: registration %d for instance %s at %s", id, instance.name, instance.uri)
		go rm.handleInstanceAlive(instance)
		return
	}
	cluster, ok := rm.clusters[id]
	if ok {
		log.Printf("RealmManager.registrationAlive: registration %d for cluster %s at %s", id, cluster.name, instance.uri)
		return
	}
}

func (rm *RealmManager) handleInstanceAlive(instance InstanceInfo) {
	opCtx, opCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer opCancel()

	rangeStart := rm.EtcdKeyStore.GetRealmKeyPath("clusters/")
	rangeEnd := clientv3.GetPrefixRangeEnd(rangeStart)
	resp, err := rm.cli.Get(opCtx, rangeStart, clientv3.WithRange(rangeEnd), clientv3.WithLimit(10000))
	if err != nil {
		log.Print("RealmManager.handleInstanceAlive: failed to get config keys")
	}
	var clusters []string
	for _, kv := range resp.Kvs {
		key, found := strings.CutPrefix(string(kv.Key), rangeStart)
		if !found {
			continue
		}
		key, found = strings.CutSuffix(key, ".yaml")
		if !found {
			continue
		}
		if rm.mapping.MatchClusterToInstance(instance.name, key) {
			clusters = append(clusters, key)
		}
	}
	log.Printf("Clusters in project %s suitable for instance %s: %v", rm.EtcdKeyStore.Realm, instance.name, clusters)
	/*
		LIST 0: get list of cluster configs from etcd (/config/T/P/*.yaml)
		LIST 1: get list of clusters assigned to this instance from etcd (/status/T/P/instance/instance.name/*) along with their hashes (keys)

		get viinex objects from instance (uri.svc, uri.svc.meta). Find the vnxclass db (KeyValueStore) and vnxclass script (Updateable)
		LIST 2: From the vnxclass db list the cluster configs previously pushed to the db (names)

		Iterate over LIST 1 \ LIST 0.
			Remove the record from db and ask the script to stop the cluster.

		Iterate over LIST 0.
		If cluster does not match this instance, check if it is in LIST 1. If it is -- remove it from db at viinex instance and from /status branch in etcd.
		If cluster matches this instance, check if it is in LIST 1.
		    If it isn't -- evaluate the config, evaluate its hash, push config to viinex db, write hash to /status/T/P/instance/I/C, and ask the script to restart the cluster
			If it is -- check if it is in LIST 2. If it is -- we're good. If it's not -- see previous step.


		Apart from that, if config changes, remove cluster's record from /status
		If mapping changes, remove the whole /status
	*/
}
