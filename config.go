package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/go-jsonnet"
	"gopkg.in/yaml.v3"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdJsonnetImporter struct {
	EtcdClient
	cache map[string]jsonnet.Contents
}

func (imp EtcdJsonnetImporter) Import(importedFrom, importedPath string) (contents jsonnet.Contents, foundAt string, err error) {
	cached, ok := imp.cache[importedPath]
	if ok {
		return cached, importedPath, nil
	}
	k, err := imp.cli.KV.Get(context.Background(), "/templates/jsonnet/"+importedPath)
	if err != nil {
		log.Printf("could not read jsonnet %s: %s", importedPath, err)
		return jsonnet.Contents{}, "", err
	}
	if k.Count == 0 {
		err = fmt.Errorf("etcd key %s not found", importedPath)
		return jsonnet.Contents{}, "", err
	}
	contents = jsonnet.MakeContentsRaw(k.Kvs[0].Value)
	imp.cache[importedPath] = contents

	return contents, importedPath, nil
}

func (imp EtcdClient) GetTenantsAndProjects() (res map[string][]string, err error) {
	return enumerateHierarchy(imp.cli, "/config")
}

// by gemini
func enumerateHierarchy(cli *clientv3.Client, prefix string) (map[string][]string, error) {
	// Overall context for the entire enumeration operation.
	opCtx, opCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer opCancel()

	// Ensure the prefix ends with a "/" to properly delineate the hierarchy.
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// --- Consistent Read via Revision ---
	// First, perform a single, small GET to find the current revision of the store.
	// We only need the header, so we can limit the response to 1 key.
	initialResp, err := cli.Get(opCtx, prefix, clientv3.WithLimit(1), clientv3.WithSerializable())
	if err != nil {
		return nil, fmt.Errorf("failed to get initial revision: %w", err)
	}
	// All subsequent reads will be performed at this specific revision to ensure
	// we are iterating over a consistent snapshot of the data.
	revision := initialResp.Header.Revision

	results := make(map[string][]string)
	// Helper map to track which third-level keys we've already added.
	seen := make(map[string]map[string]struct{})

	// --- Pagination Logic ---
	const pageSize = 1000
	lastKey := prefix
	rangeEnd := clientv3.GetPrefixRangeEnd(prefix)

	for {
		// Get a page of keys. We use WithRev(revision) to ensure every paged request
		// is part of the same consistent view (snapshot isolation).
		resp, err := cli.Get(opCtx, lastKey, clientv3.WithRange(rangeEnd), clientv3.WithLimit(pageSize), clientv3.WithRev(revision))
		if err != nil {
			return nil, fmt.Errorf("failed to get a page of keys from etcd at revision %d: %w", revision, err)
		}

		// Iterate over the key-value pairs in the current page.
		for _, kv := range resp.Kvs {
			keyStr := string(kv.Key)
			trimmedKey := strings.TrimPrefix(keyStr, prefix)
			parts := strings.SplitN(trimmedKey, "/", 3)

			if len(parts) < 2 {
				continue
			}

			secondLevel, thirdLevel := parts[0], parts[1]
			if secondLevel == "" || thirdLevel == "" {
				continue
			}

			if _, ok := seen[secondLevel]; !ok {
				seen[secondLevel] = make(map[string]struct{})
			}

			if _, ok := seen[secondLevel][thirdLevel]; !ok {
				results[secondLevel] = append(results[secondLevel], thirdLevel)
				seen[secondLevel][thirdLevel] = struct{}{}
			}
		}

		if !resp.More {
			break // All keys for the given revision have been fetched.
		}

		// Set the starting point for the next page.
		lastKey = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}

	return results, nil
}

type ConfigRecipe struct {
	Main       string            `yaml:"main"`
	OSName     string            `yaml:"os-name"`
	ExtStr     map[string]string `yaml:"ext-str"`
	ExtStrFile map[string]string `yaml:"ext-str-file"`
}

func (eks EtcdKeyStore) GetClusterConfig(ctx context.Context, clusterName string) (string, error) {
	vm := jsonnet.MakeVM()
	vm.Importer(EtcdJsonnetImporter{EtcdClient: eks.EtcdClient, cache: make(map[string]jsonnet.Contents)})

	conf, err := eks.cli.KV.Get(ctx, "/config/"+eks.Tenant+"/"+eks.Realm+"/"+clusterName+".yaml")
	if err != nil || conf.Count != 1 {
		log.Print("cannot load config document")
		return "", err
	}

	recipeKv, err := eks.cli.KV.Get(ctx, "/config/"+eks.Tenant+"/"+eks.Realm+"/recipe.yaml")
	if err != nil {
		log.Print("error while trying to load recipe.yaml")
		return "", err
	}
	recipe := ConfigRecipe{
		Main:   "main.jsonnet",
		OSName: "Linux",
	}
	if recipeKv.Count > 0 {
		err = yaml.Unmarshal(recipeKv.Kvs[0].Value, &recipe)
		if err != nil {
			log.Print("cannot yaml-decode recipe.yaml", err)
			return "", err
		}
		for k, v := range recipe.ExtStr {
			vm.ExtVar(k, v)
		}
		for k, f := range recipe.ExtStrFile {
			fkv, err := eks.cli.KV.Get(ctx, "/config/"+eks.Tenant+"/"+eks.Realm+"/"+f)
			if err != nil || fkv.Count != 1 {
				log.Printf("could not load value for ext-str-file %s: %s", f, err)
				return "", err
			}
			vm.ExtVar(k, string(fkv.Kvs[0].Value))
		}
	}

	vm.ExtVar("OSName", recipe.OSName)
	vm.ExtVar("confYaml", string(conf.Kvs[0].Value))
	vm.ExtVar("CID", clusterName)
	jsonStr, err := vm.EvaluateFile(recipe.Main)
	if err != nil {
		log.Print("error while building config", err)
		return "", err
	}
	return jsonStr, nil
}
