package main

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/gammazero/nexus/v3/wamp"
)

// .svc call on a cluster
type SvcEntry struct {
	EndpointType string
	ObjectName   string
}

func ParseSvc(res *wamp.Result) ([]SvcEntry, bool) {
	if len(res.Arguments) != 1 {
		return nil, false
	}
	list, ok := wamp.AsList(res.Arguments[0])
	if !ok {
		return nil, false
	}
	var entries []SvcEntry
	for _, v := range list {
		pair, ok := wamp.AsList(v)
		if !ok {
			return nil, false
		}
		if len(pair) != 2 {
			return nil, false
		}
		ept, ok := wamp.AsString(pair[0])
		if !ok {
			return nil, false
		}
		oid, ok := wamp.AsString(pair[1])
		if !ok {
			return nil, false
		}
		entries = append(entries, SvcEntry{EndpointType: ept, ObjectName: oid})
	}
	return entries, true
}

func ParseSvcMeta(res *wamp.Result) (wamp.Dict, bool) {
	if len(res.Arguments) != 1 {
		return nil, false
	}
	return wamp.AsDict(res.Arguments[0])
}

type VnxclassEndpoints struct {
	ConfigDatabase   string
	ControllerScript string
}

func FilterVnxclass(prefix wamp.URI, svc []SvcEntry, meta wamp.Dict) VnxclassEndpoints {
	metaVnxclass := make(map[string]bool)
	for k, v := range meta {
		vdict, ok := wamp.AsDict(v)
		if !ok {
			continue
		}
		mvnxclass, ok := vdict["vnxclass"]
		if !ok {
			continue
		}
		vnxclass, ok := mvnxclass.(bool)
		if !ok || !vnxclass {
			continue
		}
		metaVnxclass[k] = true
	}
	var res VnxclassEndpoints
	for _, e := range svc {
		vnxclass, found := metaVnxclass[e.ObjectName]
		if found && vnxclass {
			switch e.EndpointType {
			case "Updateable":
				res.ControllerScript = string(prefix) + "." + e.ObjectName + ".updateable"
			case "KeyValueStore":
				res.ConfigDatabase = string(prefix) + "." + e.ObjectName + ".key_value_store"
			}
		}
	}
	return res
}

func DecodeViaJSON(res *wamp.Result, result any) error {
	if len(res.Arguments) != 1 {
		return errors.New("there should be exactly one result in wamp response")
	}
	bytes, err := json.Marshal(res.Arguments[0])
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, result)
	return err
}

func ParseKvStoreList(res *wamp.Result, prefix string) ([]string, bool) {
	var r []string
	err := DecodeViaJSON(res, &r)
	if err != nil {
		return nil, false
	}
	for k := range r {
		r[k], _ = strings.CutPrefix(r[k], prefix)
	}
	return r, true
}

func ParseKvStoreGet2(res *wamp.Result, prefix string) (map[string]interface{}, bool) {
	var r [][]interface{}
	m := make(map[string]interface{})
	err := DecodeViaJSON(res, &r)
	if err != nil {
		return nil, false
	}
	for _, v := range r {
		if len(v) != 2 {
			return nil, false
		}
		n, ok := v[0].(string)
		if !ok {
			return nil, false
		}
		n, _ = strings.CutPrefix(n, prefix)
		m[n] = v[1]
	}
	return m, true
}

func GetDbPathClusterConfig(instance string, cluster string) string {
	return "/instance/" + instance + "/clusterconfig/" + cluster
}
func GetDbPathClusterConfigHash(instance string, cluster string) string {
	return "/instance/" + instance + "/cchash/" + cluster
}
