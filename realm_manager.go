package main

import (

	//"log"
	//"os"

	//"github.com/gammazero/nexus/v3/client"
	//"github.com/gammazero/nexus/v3/router"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"maps"
	"slices"
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
	instances        map[wamp.ID]*InstanceInfo
	clusters         map[wamp.ID]*ClusterInfo
	metricsProviders map[wamp.ID]wamp.URI
}

type InstanceInfo struct {
	name string
	uri  wamp.URI

	endpoints *VnxclassEndpoints
}

type ClusterInfo struct {
	name string
	uri  wamp.URI

	endpoints *[]SvcEntry
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
		instances:        make(map[wamp.ID]*InstanceInfo),
		clusters:         make(map[wamp.ID]*ClusterInfo),
		metricsProviders: make(map[wamp.ID]wamp.URI),
	}
	mappingResp, err := eks.cli.KV.Get(context.Background(), eks.GetRealmConfigKeyPath("mapping.yaml"))
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
	rm.instances[id] = &InstanceInfo{
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
	rm.clusters[id] = &ClusterInfo{
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

func (rm *RealmManager) getInstanceClustersProjected(instance string) ([]string, error) {
	opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer opCancel()
	rangeStart := rm.EtcdKeyStore.GetRealmConfigKeyPath("clusters/")
	rangeEnd := clientv3.GetPrefixRangeEnd(rangeStart)
	resp, err := rm.cli.Get(opCtx, rangeStart, clientv3.WithRange(rangeEnd), clientv3.WithLimit(10000), clientv3.WithKeysOnly())
	if err != nil {
		return nil, fmt.Errorf("failed to get config keys: %w", err)
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
		if rm.mapping.MatchClusterToInstance(instance, key) {
			clusters = append(clusters, key)
		}
	}
	return clusters, nil
}

func (rm *RealmManager) getInstanceClustersDeployed(instance string) (map[string]string, error) {
	opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer opCancel()
	rangeStart := rm.EtcdKeyStore.GetRealmStatusKeyPath("instance/" + instance + "/clusters/")
	rangeEnd := clientv3.GetPrefixRangeEnd(rangeStart)
	resp, err := rm.cli.Get(opCtx, rangeStart, clientv3.WithRange(rangeEnd), clientv3.WithLimit(10000))
	if err != nil {
		return nil, fmt.Errorf("failed to get config keys: %w", err)
	}
	clusters := make(map[string]string)
	for _, kv := range resp.Kvs {
		key, found := strings.CutPrefix(string(kv.Key), rangeStart)
		if !found {
			continue
		}
		clusters[key] = string(kv.Value)
	}
	return clusters, nil
}

func (rm *RealmManager) findInstanceDbClusters(instance *InstanceInfo) (map[string]string, error) {
	opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer opCancel()

	if instance.endpoints == nil {
		uriSvc := string(instance.uri) + ".svc"
		uriSvcMeta := string(instance.uri) + ".svc.meta"

		resSvc, err := rm.wampClient.Call(opCtx, uriSvc, nil, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		resSvcMeta, err := rm.wampClient.Call(opCtx, uriSvcMeta, nil, nil, nil, nil)
		if err != nil {
			return nil, err
		}

		svc, ok := ParseSvc(resSvc)
		if !ok {
			return nil, errors.New("could not recognize response to svc call")
		}
		svcMeta, ok := ParseSvcMeta(resSvcMeta)
		if !ok {
			return nil, errors.New("could not recognize response to svc.meta call")
		}
		log.Printf("svc and meta: %v, %v", svc, svcMeta)
		var ep = FilterVnxclass(instance.uri, svc, svcMeta)
		instance.endpoints = &ep
	}

	hashPrefix := GetDbPathClusterConfigHash(instance.name, "")
	resList, err := rm.wampClient.Call(opCtx, instance.endpoints.ConfigDatabase+".get2", nil, wamp.List{hashPrefix, wamp.Dict{"as_prefix": true}}, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get config hashes from instance's config db: %w", err)
	}
	configHashes, ok := ParseKvStoreGet2(resList, hashPrefix)
	if !ok {
		return nil, fmt.Errorf("failed to parse list response: %w", err)
	}
	result := make(map[string]string, 0)
	for k, v := range configHashes {
		s, ok := v.(string)
		if !ok {
			continue
		}
		result[k] = s
	}
	return result, nil
}

func (rm *RealmManager) handleInstanceAlive(instance *InstanceInfo) {

	clustersProjected, err := rm.getInstanceClustersProjected(instance.name)
	if err != nil {
		log.Printf("could not get projected clusters for %s: %s", instance.name, err)
		return
	}
	log.Printf("Clusters projected in realm %s for instance %s: %v", rm.EtcdKeyStore.Realm, instance.name, clustersProjected)

	clustersDeployed, err := rm.getInstanceClustersDeployed(instance.name)
	if err != nil {
		log.Printf("could not get deployed clusters for %s: %s", instance.name, err)
		return
	}
	log.Printf("Clusters previously deployed in realm %s for instance %s: %v", rm.EtcdKeyStore.Realm, instance.name, clustersDeployed)

	clustersFound, err := rm.findInstanceDbClusters(instance)
	if err != nil {
		log.Printf("could not find clusters for %s: %s", instance.name, err)
		return
	}
	log.Printf("Clusters found in realm %s at instance %s: %v", rm.EtcdKeyStore.Realm, instance.name, clustersFound)

	// first dispose clusters which are not meant to be up.
	// dispose means we remove them from viinex' db, stop them and remove from status branch in etcd
	clustersToDispose := make(map[string]bool)
	for k := range clustersDeployed {
		clustersToDispose[k] = true
	}
	for k := range clustersFound {
		clustersToDispose[k] = true
	}
	for _, cp := range clustersProjected {
		delete(clustersToDispose, cp)
	}
	var wg sync.WaitGroup
	wg.Add(len(clustersToDispose))
	log.Printf("going to dispose clusters %v previously known to instance %s", slices.Collect(maps.Keys(clustersToDispose)), instance.name)
	for cluster := range clustersToDispose {
		go func() {
			defer wg.Done()
			err := rm.disposeCluster(instance, cluster)
			if err != nil {
				log.Printf("failed to deploy cluster %s to instance %s: %s", cluster, instance.name, err)
			}
		}()
	}
	wg.Wait()
	log.Printf("disposed clusters %v previously known to instance %s", slices.Collect(maps.Keys(clustersToDispose)), instance.name)

	clustersToDeploy := make(map[string]bool)
	for _, cluster := range clustersProjected {
		hashDeployed, wasDeployed := clustersDeployed[cluster]
		hashFound, wasFound := clustersFound[cluster]
		if !wasDeployed || !wasFound || hashDeployed != hashFound {
			clustersToDeploy[cluster] = true
		}
	}

	wg.Add(len(clustersToDeploy))
	log.Printf("going to deploy clusters %v to instance %s", slices.Collect(maps.Keys(clustersToDeploy)), instance.name)
	for cluster := range clustersToDeploy {
		go func() {
			defer wg.Done()
			err := rm.deployCluster(instance, cluster)
			if err != nil {
				log.Printf("failed to deploy cluster %s to instance %s: %s", cluster, instance.name, err)
			}
		}()
	}
	wg.Wait()
	log.Printf("deployed clusters %v to instance %s", slices.Collect(maps.Keys(clustersToDeploy)), instance.name)
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

func (rm *RealmManager) disposeCluster(instance *InstanceInfo, cluster string) error {
	opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer opCancel()

	hashPath := GetDbPathClusterConfigHash(instance.name, cluster)
	configPath := GetDbPathClusterConfig(instance.name, cluster)

	res, err := rm.wampClient.Call(opCtx, instance.endpoints.ControllerScript+".update", nil,
		wamp.List{wamp.Dict{"method": "prepare", "cluster": cluster, "intent": "dispose"}}, nil, nil)
	if err != nil {
		return err
	}
	var prepared bool
	err = DecodeViaJSON(res, &prepared)
	if err != nil {
		return err
	}
	if !prepared {
		return fmt.Errorf("controller script at instance %s declined prepare call to dispose cluster %s", instance.name, cluster)
	}
	_, err = rm.wampClient.Call(opCtx, instance.endpoints.ConfigDatabase+".delete", nil, wamp.List{hashPath}, nil, nil)
	if err != nil {
		return err
	}
	_, err = rm.wampClient.Call(opCtx, instance.endpoints.ConfigDatabase+".delete", nil, wamp.List{configPath}, nil, nil)
	if err != nil {
		return err
	}
	_, err = rm.wampClient.Call(opCtx, instance.endpoints.ControllerScript+".update", nil,
		wamp.List{wamp.Dict{"method": "dispose", "cluster": cluster}}, nil, nil)
	if err != nil {
		return err
	}
	statusPath := rm.EtcdKeyStore.GetRealmStatusKeyPath("instance/" + instance.name + "/clusters/" + cluster)
	_, err = rm.EtcdKeyStore.cli.KV.Delete(opCtx, statusPath)
	if err != nil {
		return err
	}
	return nil
}
func (rm *RealmManager) deployCluster(instance *InstanceInfo, cluster string) error {
	opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer opCancel()

	res, err := rm.wampClient.Call(opCtx, instance.endpoints.ControllerScript+".update", nil,
		wamp.List{wamp.Dict{"method": "prepare", "cluster": cluster, "intent": "deploy"}}, nil, nil)
	if err != nil {
		return err
	}
	var prepared bool
	err = DecodeViaJSON(res, &prepared)
	if err != nil {
		return err
	}
	if !prepared {
		return fmt.Errorf("controller script at instance %s declined prepare call to deploy cluster %s", instance.name, cluster)
	}

	hashPath := GetDbPathClusterConfigHash(instance.name, cluster)
	configPath := GetDbPathClusterConfig(instance.name, cluster)

	config, err := rm.EtcdKeyStore.GetClusterConfig(opCtx, cluster)
	if err != nil {
		return err
	}
	h := sha256.New()
	h.Write([]byte(config))
	hash := hex.EncodeToString(h.Sum(nil))

	_, err = rm.wampClient.Call(opCtx, instance.endpoints.ConfigDatabase+".put", nil, wamp.List{configPath, config}, nil, nil)
	if err != nil {
		return err
	}
	_, err = rm.wampClient.Call(opCtx, instance.endpoints.ConfigDatabase+".put", nil, wamp.List{hashPath, hash}, nil, nil)
	if err != nil {
		return err
	}
	_, err = rm.wampClient.Call(opCtx, instance.endpoints.ControllerScript+".update", nil,
		wamp.List{wamp.Dict{"method": "deploy", "cluster": cluster}}, nil, nil)
	if err != nil {
		return err
	}
	statusPath := rm.EtcdKeyStore.GetRealmStatusKeyPath("instance/" + instance.name + "/clusters/" + cluster)
	_, err = rm.EtcdKeyStore.cli.KV.Put(opCtx, statusPath, hash)
	if err != nil {
		return err
	}

	return nil
}
