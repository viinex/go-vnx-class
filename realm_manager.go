package main

import (

	//"log"
	//"os"

	//"github.com/gammazero/nexus/v3/client"
	//"github.com/gammazero/nexus/v3/router"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
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
	mapping       Mapping
	prometheusUrl string

	wampClient *client.Client
	quit       chan struct{}
	regEvents  chan *wamp.Event
	watchChan  clientv3.WatchChan

	instances map[wamp.ID]*InstanceInfo
	clusters  map[wamp.ID]*ClusterInfo

	mutex sync.RWMutex
}

type InstanceInfo struct {
	name       string
	uri        wamp.URI
	deregister chan struct{}

	endpoints *VnxclassEndpoints
}

type ClusterInfo struct {
	name       string
	uri        wamp.URI
	deregister chan struct{}

	endpoints        []SvcEntry
	metricsExporters []string
}

type RealmManagers struct {
	realmManagers []*RealmManager
}

func (rms RealmManagers) Close() error {
	for _, rm := range rms.realmManagers {
		close(rm.quit)
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
		case watchResponse := <-rm.watchChan:
			for _, v := range watchResponse.Events {
				rm.handleEtcdConfigBranchChange(v)
			}
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
				mapping := rm.GetMapping()
				if instance, ok := mapping.MatchInstance(uri); ok {
					rm.registerInstance(uri, id, instance)
				} else if cluster, ok := mapping.MatchCluster(uri); ok {
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

func (rms RealmManagers) RealmManager(eks EtcdKeyStore, wampClient *client.Client, prometheusUrl string) (*RealmManager, error) {
	watchChan := eks.cli.Watcher.Watch(context.Background(), eks.GetRealmConfigKeyPath(""), clientv3.WithPrefix())
	mapping, err := eks.LoadMapping(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	rm := RealmManager{
		EtcdKeyStore:  eks,
		wampClient:    wampClient,
		mapping:       mapping,
		prometheusUrl: prometheusUrl,
		quit:          make(chan struct{}),
		regEvents:     make(chan *wamp.Event, 100),
		watchChan:     watchChan,
		instances:     make(map[wamp.ID]*InstanceInfo),
		clusters:      make(map[wamp.ID]*ClusterInfo),
	}
	rms.realmManagers = append(rms.realmManagers, &rm)
	go rm.RunManageRealm()
	return &rm, nil
}

func (rm *RealmManager) GetMapping() Mapping {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()
	return rm.mapping
}

func (eks *EtcdKeyStore) LoadMapping(ctx context.Context, mappingYaml *[]byte) (Mapping, error) {
	if mappingYaml == nil {
		mappingResp, err := eks.cli.KV.Get(ctx, eks.GetRealmConfigKeyPath("mapping.yaml"))
		if err != nil {
			return nil, fmt.Errorf("failed to contact etcd: %w", err)
		}
		if len(mappingResp.Kvs) != 0 {
			mappingYaml = &(mappingResp.Kvs[0].Value)
		}
	}
	if mappingYaml != nil {
		r := PolymorphicMapping{}
		err := yaml.Unmarshal(*mappingYaml, &r)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cluster to instance mapping: %w", err)
		}
		return r, nil
	}
	return MappingNone{}, nil
}

func (rm *RealmManager) registerInstance(uri wamp.URI, id wamp.ID, instance string) error {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()
	_, ok := rm.instances[id]
	if ok {
		return errors.New("registration already present for the instance")
	}
	rm.instances[id] = &InstanceInfo{
		uri:        uri,
		name:       instance,
		deregister: make(chan struct{}),
	}
	log.Printf("RealmManager.registerInstance: added registration %d for instance %s at %s\n", id, instance, uri)
	return nil
}
func (rm *RealmManager) registerCluster(uri wamp.URI, id wamp.ID, cluster string) error {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()
	_, ok := rm.clusters[id]
	if ok {
		return errors.New("registration already present for the cluster")
	}
	rm.clusters[id] = &ClusterInfo{
		uri:        uri,
		name:       cluster,
		deregister: make(chan struct{}),
	}
	log.Printf("RealmManager.registerCluster: added registration %d for cluster %s at %s\n", id, cluster, uri)
	return nil
}

func (rm *RealmManager) deregister(id wamp.ID) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()
	instance, ok := rm.instances[id]
	if ok {
		close(instance.deregister)
		delete(rm.instances, id)
		log.Printf("RealmManager.deregister: removed registration %d for instance %s at %s\n", id, instance.name, instance.uri)
		return
	}
	cluster, ok := rm.clusters[id]
	if ok {
		close(cluster.deregister)
		delete(rm.clusters, id)
		log.Printf("RealmManager.deregister: removed registration %d for cluster %s at %s\n", id, cluster.name, cluster.uri)
		return
	}
}

func (rm *RealmManager) registrationAlive(id wamp.ID) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()
	instance, ok := rm.instances[id]
	if ok {
		log.Printf("RealmManager.registrationAlive: registration %d for instance %s at %s\n", id, instance.name, instance.uri)
		go rm.handleInstanceAlive(instance)
		return
	}
	cluster, ok := rm.clusters[id]
	if ok {
		log.Printf("RealmManager.registrationAlive: registration %d for cluster %s at %s\n", id, cluster.name, cluster.uri)
		go rm.handleClusterAlive(cluster)
		return
	}
}

func (rm *RealmManager) getInstanceClustersProjected(instance string) ([]string, error) {
	opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer opCancel()
	clusters, err := rm.getClustersKnown(opCtx)
	if err != nil {
		return nil, err
	}
	return Filter(clusters, func(cluster string) bool { return rm.mapping.MatchClusterToInstance(instance, cluster) }), nil
}

func (rm *RealmManager) getClustersKnown(ctx context.Context) ([]string, error) {

	prefix := rm.EtcdKeyStore.GetRealmConfigKeyPath("clusters/")
	resp, err := rm.cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, fmt.Errorf("failed to get config keys: %w", err)
	}
	var clusters []string
	for _, kv := range resp.Kvs {
		key, found := strings.CutPrefix(string(kv.Key), prefix)
		if !found {
			continue
		}
		key, found = strings.CutSuffix(key, ".yaml")
		if !found {
			continue
		}
		clusters = append(clusters, key)
	}
	return clusters, nil
}

func (rm *RealmManager) getInstanceClustersDeployed(instance string) (map[string]string, error) {
	opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer opCancel()
	prefix := rm.EtcdKeyStore.GetRealmStatusMappingRecordPrefix(instance)
	resp, err := rm.cli.Get(opCtx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get config keys: %w", err)
	}
	clusters := make(map[string]string)
	for _, kv := range resp.Kvs {
		key, found := strings.CutPrefix(string(kv.Key), prefix)
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
		log.Printf("svc and meta: %v, %v\n", svc, svcMeta)
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
		log.Printf("could not get projected clusters for %s: %s\n", instance.name, err)
		return
	}
	log.Printf("Clusters projected in realm %s for instance %s: %v\n", rm.EtcdKeyStore.Realm, instance.name, clustersProjected)

	clustersDeployed, err := rm.getInstanceClustersDeployed(instance.name)
	if err != nil {
		log.Printf("could not get deployed clusters for %s: %s\n", instance.name, err)
		return
	}
	log.Printf("Clusters previously deployed in realm %s for instance %s: %v\n", rm.EtcdKeyStore.Realm, instance.name, clustersDeployed)

	clustersFound, err := rm.findInstanceDbClusters(instance)
	if err != nil {
		log.Printf("could not find clusters for %s: %s\n", instance.name, err)
		return
	}
	log.Printf("Clusters found in realm %s at instance %s: %v\n", rm.EtcdKeyStore.Realm, instance.name, clustersFound)

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
	if len(clustersToDispose) > 0 {
		var wg sync.WaitGroup
		wg.Add(len(clustersToDispose))
		log.Printf("going to dispose clusters %v previously known to instance %s\n", slices.Collect(maps.Keys(clustersToDispose)), instance.name)
		for cluster := range clustersToDispose {
			go func() {
				defer wg.Done()
				err := rm.disposeCluster(instance, cluster)
				if err != nil {
					log.Printf("failed to dispose cluster %s at instance %s: %s\n", cluster, instance.name, err)
				}
			}()
		}
		wg.Wait()
		log.Printf("disposed clusters %v previously known to instance %s\n", slices.Collect(maps.Keys(clustersToDispose)), instance.name)
	}

	clustersToDeploy := make(map[string]bool)
	for _, cluster := range clustersProjected {
		hashDeployed, wasDeployed := clustersDeployed[cluster]
		hashFound, wasFound := clustersFound[cluster]
		if !wasDeployed || !wasFound || hashDeployed != hashFound {
			clustersToDeploy[cluster] = true
		}
	}

	if len(clustersToDeploy) > 0 {
		var wg sync.WaitGroup
		wg.Add(len(clustersToDeploy))
		log.Printf("going to deploy clusters %v to instance %s", slices.Collect(maps.Keys(clustersToDeploy)), instance.name)
		for cluster := range clustersToDeploy {
			go func() {
				defer wg.Done()
				err := rm.deployCluster(instance, cluster)
				if err != nil {
					log.Printf("failed to deploy cluster %s to instance %s: %s\n", cluster, instance.name, err)
				}
			}()
		}
		wg.Wait()
		log.Printf("deployed clusters %v to instance %s\n", slices.Collect(maps.Keys(clustersToDeploy)), instance.name)
	}
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

func (rm *RealmManager) handleClusterAlive(cluster *ClusterInfo) {
	opCtx, opCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer opCancel()
	if cluster.endpoints == nil {
		uriSvc := string(cluster.uri) + ".svc"

		resSvc, err := rm.wampClient.Call(opCtx, uriSvc, nil, nil, nil, nil)
		if err != nil {
			cluster.endpoints = make([]SvcEntry, 0, 1)
			log.Printf("failed to call svc on cluster %s at instance %s of tenant %s: %s", cluster.uri, rm.Realm, rm.Tenant, err)
			return
		}

		svc, ok := ParseSvc(resSvc)
		if !ok {
			log.Printf("could not recognize response to svc call on cluster %s at instance %s of tenant %s", cluster.uri, rm.Realm, rm.Tenant)
			return
		}
		cluster.endpoints = svc
	}
	for _, ep := range cluster.endpoints {
		if ep.EndpointType == "MetricsExporter" {
			cluster.metricsExporters = append(cluster.metricsExporters, ep.ObjectName)
		}
	}
	if len(cluster.metricsExporters) > 0 && rm.prometheusUrl != "" {
		go rm.exportClusterMetrics(cluster)
	}
}

func (rm *RealmManager) exportClusterMetrics(cluster *ClusterInfo) {
	debugPrintOnce := true
	for {
		select {
		case <-cluster.deregister:
			return
		case <-rm.quit:
			return
		case <-rm.wampClient.Done():
			return
		case <-time.After(30 * time.Second):
			for _, exporter := range cluster.metricsExporters {
				rm.exportClusterMetricsStep(cluster, exporter, &debugPrintOnce)
			}
		}
	}
}
func (rm *RealmManager) exportClusterMetricsStep(cluster *ClusterInfo, exporter string, debugPrintOnce *bool) error {
	opCtx, opCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer opCancel()
	res, err := rm.wampClient.Call(opCtx, string(cluster.uri)+"."+exporter+".sample", nil, wamp.List{}, nil, nil)
	if err != nil {
		log.Printf("failed to sample metrics from %s in cluster %s in realm %s of tenant %s: %s", exporter, cluster.name, rm.Realm, rm.Tenant, err)
		return err
	}
	var b64gzmetrics string
	err = DecodeViaJSON(res, &b64gzmetrics)
	if err != nil {
		log.Printf("failed to parse results of sampling metrics from %s in cluster %s in realm %s of tenant %s: %s", exporter, cluster.name, rm.Realm, rm.Tenant, err)
		return err
	}
	decodedBytes, err := base64.StdEncoding.DecodeString(b64gzmetrics)
	if err != nil {
		log.Printf("error decoding base64 while exporting metrics from %s in cluster %s in realm %s of tenant %s: %v", exporter, cluster.name, rm.Realm, rm.Tenant, err)
		return err
	}
	gzipReader, err := gzip.NewReader(bytes.NewReader(decodedBytes))
	if err != nil {
		log.Fatalf("Error creating gzip reader: %v", err)
	}
	defer gzipReader.Close()

	if debugPrintOnce == nil || (debugPrintOnce != nil && *debugPrintOnce) {
		log.Printf("metrics exporter: first iteration, received %d compressed bytes from %s at cluster %s in realm %s of tenant %s",
			len(b64gzmetrics), exporter, cluster.name, rm.Realm, rm.Tenant)
		if debugPrintOnce != nil {
			*debugPrintOnce = false
		}
	}

	req, err := http.NewRequestWithContext(opCtx, "POST", rm.prometheusUrl, gzipReader)
	if err != nil {
		log.Fatalf("Error creating http request: %v", err)
	}

	req.Header.Set("Content-Type", "text/plain")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("http client yield an error while exporting metrics from %s in cluster %s in realm %s of tenant %s: %v",
			exporter, cluster.name, rm.Realm, rm.Tenant, err)
		return err
	}
	resp.Body.Close()
	return nil
}

func (rm *RealmManager) disposeCluster(instance *InstanceInfo, cluster string) error {
	opCtx, opCancel := context.WithTimeout(context.Background(), 120*time.Second)
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
	statusPath := rm.EtcdKeyStore.GetRealmStatusMappingRecordPrefix(instance.name) + cluster
	_, err = rm.EtcdKeyStore.cli.KV.Delete(opCtx, statusPath)
	if err != nil {
		return err
	}
	return nil
}
func (rm *RealmManager) deployCluster(instance *InstanceInfo, cluster string) error {
	opCtx, opCancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer opCancel()

	// read out and generate cluster config BEFORE calling the prepare on controller script at viinex instance
	// because this can be lengthy, and config change by controller script is a 2phase commit transaction and
	// has timeout of 30 seconds
	hashPath := GetDbPathClusterConfigHash(instance.name, cluster)
	configPath := GetDbPathClusterConfig(instance.name, cluster)

	config, err := rm.EtcdKeyStore.GetClusterConfig(opCtx, cluster)
	if err != nil {
		return err
	}
	h := sha256.New()
	h.Write([]byte(config))
	hash := hex.EncodeToString(h.Sum(nil))

	// initiate transaction ("prepare")
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
	// populate the remote database with config data and its hash
	_, err = rm.wampClient.Call(opCtx, instance.endpoints.ConfigDatabase+".put", nil, wamp.List{configPath, config}, nil, nil)
	if err != nil {
		return err
	}
	_, err = rm.wampClient.Call(opCtx, instance.endpoints.ConfigDatabase+".put", nil, wamp.List{hashPath, hash}, nil, nil)
	if err != nil {
		return err
	}
	// commit the remote transaction ("deploy")
	_, err = rm.wampClient.Call(opCtx, instance.endpoints.ControllerScript+".update", nil,
		wamp.List{wamp.Dict{"method": "deploy", "cluster": cluster}}, nil, nil)
	if err != nil {
		return err
	}

	// and record the confirmation in ETCD that config with said hash has been pushed and deployed
	statusPath := rm.EtcdKeyStore.GetRealmStatusMappingRecordPrefix(instance.name) + cluster
	_, err = rm.EtcdKeyStore.cli.KV.Put(opCtx, statusPath, hash)
	if err != nil {
		return err
	}

	return nil
}

func (rm *RealmManager) handleEtcdConfigBranchChange(event *clientv3.Event) {
	opCtx, opCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer opCancel()
	key := string(event.Kv.Key)
	prefixClusters := rm.EtcdKeyStore.GetRealmConfigKeyPath("clusters/")
	pathMappingYaml := rm.EtcdKeyStore.GetRealmConfigKeyPath("mapping.yaml")
	pathRecipeYaml := rm.EtcdKeyStore.GetRealmConfigKeyPath("recipe.yaml")
	if key == pathRecipeYaml {
		// when recipe gets changed we assume all configs should be re-generated and re-deployed
		// might optimize this later
		log.Printf("Detected possible change of config generation RECIPE in realm %s of tenant %s", rm.Realm, rm.Tenant)
		rm.handleClusterConfigChange(opCtx, nil)
	} else if key == pathMappingYaml {
		log.Printf("Detected possible change of cluster-to-instance MAPPING in realm %s of tenant %s", rm.Realm, rm.Tenant)
		var mapping Mapping
		if event.Type == clientv3.EventTypeDelete {
			mapping = MappingNone{}
		} else {
			var err error //??
			mapping, err = rm.EtcdKeyStore.LoadMapping(opCtx, &event.Kv.Value)
			if err != nil {
				log.Printf("warning: failed to load changed mapping: %s; previous mapping will be kept\n", err)
				return
			}
		}
		rm.mutex.Lock()
		rm.mapping = mapping
		rm.mutex.Unlock()
		// iterate over status records, check if mapping is still valid, and if it's not -- remove status record and undeploy the cluster
		rm.rearrangeMappings(opCtx)
	} else if after, found := strings.CutPrefix(key, prefixClusters); found {
		cluster, found := strings.CutSuffix(after, ".yaml")
		if found {
			log.Printf("Detected possible change of CONFIG for cluster %s in realm %s of tenant %s", cluster, rm.Realm, rm.Tenant)
			rm.handleClusterConfigChange(opCtx, &cluster)
		}
	}
}

func (rm *RealmManager) handleClusterConfigChange(ctx context.Context, maybeCluster *string) error {
	affected := make(map[string][]string)
	err := rm.iterateStatusMappingRecords(ctx, func(ctx context.Context, smr StatusMappingRecord) (err error) {
		if maybeCluster == nil || smr.Cluster == *maybeCluster {
			affected[smr.Instance] = append(affected[smr.Instance], smr.Cluster)
			_, err = rm.cli.Delete(ctx, rm.GetRealmStatusMappingRecordPrefix(smr.Instance)+smr.Cluster)
		}
		return err
	})
	if err != nil {
		log.Printf("failed to remove outdated status mapping records: %s\n", err)
		return nil
	}
	//var wg sync.WaitGroup
	rm.mutex.Lock()
	for _, instance := range rm.instances {
		clusters, found := affected[instance.name]
		if !found {
			continue
		}
		//wg.Add(len(clusters))
		for _, cluster := range clusters {
			doDeploy := rm.mapping.MatchClusterToInstance(instance.name, cluster)
			go func() {
				//defer wg.Done()
				if doDeploy {
					rm.deployCluster(instance, cluster)
				} else {
					rm.disposeCluster(instance, cluster)
				}
			}()
		}
	}
	rm.mutex.Unlock()
	//wg.Wait()
	return nil
}

func (rm *RealmManager) rearrangeMappings(ctx context.Context) {
	clustersKnown, err := rm.getClustersKnown(ctx)
	if err != nil {
		log.Fatalf("could not get list of known clusters: %s", err)
	}
	clustersToDeploy := make(map[string]bool)
	for _, c := range clustersKnown {
		clustersToDeploy[c] = true
	}
	deployedMismatching := make(map[string][]string)
	deployedMatching := make(map[string][]string)
	err = rm.iterateStatusMappingRecords(ctx, func(ctx context.Context, smr StatusMappingRecord) error {
		if rm.mapping.MatchClusterToInstance(smr.Instance, smr.Cluster) {
			deployedMatching[smr.Instance] = append(deployedMatching[smr.Instance], smr.Cluster)
		} else {
			deployedMismatching[smr.Instance] = append(deployedMismatching[smr.Instance], smr.Cluster)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("rearrangeMappings: could not iterate over status mapping records: %s", err)
	}

	var wg sync.WaitGroup
	rm.mutex.Lock()

	for _, instance := range rm.instances {
		toDispose, found := deployedMismatching[instance.name]
		if found {
			wg.Add(len(toDispose))
			for _, cluster := range toDispose {
				go func() {
					defer wg.Done()
					err := rm.disposeCluster(instance, cluster)
					if err != nil {
						log.Printf("rearrangeMappings: failed to dispose cluster %s in realm %s for tenant %s at instance %s: %s\n", cluster, rm.Realm, rm.Tenant, instance.name, err)
					}
				}()
			}
		}
		alreadyDeployed, found := deployedMatching[instance.name]
		if found {
			for _, cluster := range alreadyDeployed {
				delete(clustersToDeploy, cluster)
			}
		}
	}
	// deploy clusters left to be deployed
	for _, instance := range rm.instances {
		for cluster, shouldDeployYet := range clustersToDeploy {
			if shouldDeployYet && rm.mapping.MatchClusterToInstance(instance.name, cluster) {
				clustersToDeploy[cluster] = false
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := rm.deployCluster(instance, cluster)
					if err != nil {
						log.Printf("rearrangeMappings: failed to deploy cluster %s in realm %s for tenant %s at instance %s: %s\n", cluster, rm.Realm, rm.Tenant, instance.name, err)
					}
				}()
			}
		}
	}

	rm.mutex.Unlock()
	wg.Wait()
}

type StatusMappingRecordCb func(context.Context, StatusMappingRecord) error

type StatusMappingRecord struct {
	Instance string
	Cluster  string
	Hash     string
}

func (rm *RealmManager) iterateStatusMappingRecords(ctx context.Context, callback StatusMappingRecordCb) error {
	prefixStatus := rm.EtcdKeyStore.GetRealmStatusKeyPath("")
	rangeStart := prefixStatus
	rangeEnd := clientv3.GetPrefixRangeEnd(rangeStart)
	for {
		resp, err := rm.cli.Get(ctx, rangeStart, clientv3.WithRange(rangeEnd), clientv3.WithLimit(10000))
		if err != nil {
			log.Fatalf("failed to iterate over status records for tenant %s cluster %s: %s", rm.Tenant, rm.Tenant, err)
			return err
		}
		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			after, found := strings.CutPrefix(key, prefixStatus)
			// after now is supposed to match pattern "instance/INSTANCE/clusters/CLUSTER"
			s := strings.Split(after, "/")
			if !found || len(s) != 4 || s[0] != "instance" || s[2] != "clusters" {
				log.Printf("iterateStatusMappingRecords: ignoring unrecognized key %s\n", key)
				continue
			}
			smr := StatusMappingRecord{
				Instance: s[1],
				Cluster:  s[3],
				Hash:     string(kv.Value),
			}
			err := callback(ctx, smr)
			if err != nil {
				return err
			}
		}
		if resp.More {
			rangeStart = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
		} else {
			break
		}
	}
	return nil
}
