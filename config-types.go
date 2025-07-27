package main

import (
	etcdv3 "go.etcd.io/etcd/client/v3"
)

type EtcdClient struct {
	prefix string
	cli    *etcdv3.Client
}

type EtcdKeyStore struct {
	EtcdClient
	Tenant string
	Realm  string
}
