package main

import (
	etcdv3 "go.etcd.io/etcd/client/v3"
)

type EtcdImporter struct {
	cli *etcdv3.Client
}

type EtcdKeyStore struct {
	EtcdImporter
	Tenant string
	Realm  string
}
