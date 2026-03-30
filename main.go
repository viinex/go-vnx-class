package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	//"github.com/gammazero/nexus/v3/client"
	"github.com/gammazero/nexus/v3/router"
	"gopkg.in/yaml.v3"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

type Config struct {
	Etcd       etcdv3.Config `yaml:"etcd"`
	EtcdPrefix string        `yaml:"etcd-prefix"`
	Wamp       string        `yaml:"wamp"`
	Prometheus string        `yaml:"prometheus-push-uri"`
	Static     string        `yaml:"static"`
	Debug      bool          `yaml:"debug"`
}

func BuildConfig(configPath string) (*Config, error) {
	config := Config{
		Etcd: etcdv3.Config{
			Endpoints: []string{"127.0.0.1:2379"},
		},
		Wamp:   "0.0.0.0:8080",
		Static: "/usr/share/viinex/web/browser/en",
		Debug:  false,
	}
	if configPath != "" {
		confBytes, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
		}
		err = yaml.Unmarshal(confBytes, &config)
		if err != nil {
			return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
		}
	}

	envEtcdEndpoints := os.Getenv("ETCD_ENDPOINTS")
	if envEtcdEndpoints != "" {
		config.Etcd.Endpoints = strings.Split(envEtcdEndpoints, ",")
	}
	maybeSetFromEnv("ETCD_USERNAME", &config.Etcd.Username)
	maybeSetFromEnv("ETCD_PASSWORD", &config.Etcd.Password)
	maybeSetFromEnv("ETCD_PREFIX", &config.EtcdPrefix)
	maybeSetFromEnv("WAMP", &config.Wamp)
	maybeSetFromEnv("PROMETHEUS_PUSH_URI", &config.Prometheus)
	maybeSetFromEnv("STATIC", &config.Static)
	envDebug := os.Getenv("DEBUG")
	if envDebug != "" && envDebug != "0" {
		config.Debug = true
	}

	return &config, nil
}

func maybeSetFromEnv(env string, dest *string) {
	val := os.Getenv(env)
	if val != "" {
		*dest = val
	}
}

// Custom handler for serving static files and SPA fallback
type spaHandler struct {
	staticPath string
	indexPath  string
}

func (h spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Check if the requested file exists in the static directory
	fullPath := h.staticPath + r.URL.Path
	if _, err := os.Stat(fullPath); err == nil {
		// File exists, serve it using http.FileServer
		http.FileServer(http.Dir(h.staticPath)).ServeHTTP(w, r)
		return
	}

	// If the file doesn't exist and the path does not start with "/v1/",
	// serve index.html
	if !strings.HasPrefix(r.URL.Path, "/v1/") {
		http.ServeFile(w, r, h.indexPath)
		// Add headers to prevent caching
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		return
	}

	// If the file doesn't exist and the path starts with "/v1/", return 404
	http.NotFound(w, r)
}

func main() {
	configPath := flag.String("config", "", "Path to configuration file")
	flag.Parse()

	config, err := BuildConfig(*configPath)
	if err != nil {

	}

	log.SetFlags(0)
	log.Print("==== vnx-class new run")
	cli, err := etcdv3.New(config.Etcd)
	if err != nil {
		log.Fatal("Could not open etcd client", err)
	}

	log.Print("connected to etcd")

	//logger := log.New(os.Stderr, "[wamp] ", 0)

	var cfg router.Config
	cfg.Debug = config.Debug
	theRouter, err := router.NewRouter(&cfg, log.Default())
	if err != nil {
		log.Fatal("could not create wamp router: ", err)
	}

	srv := router.NewWebsocketServer(theRouter)

	err = srv.AllowOrigins([]string{"*"})
	if err != nil {
		log.Fatal("failed to set allowed origins on the wamp router: ", err)
	}

	imp := EtcdClient{cli: cli, prefix: config.EtcdPrefix}

	tenantProjectsMap, err := imp.GetTenantsAndProjects()
	if err != nil {
		log.Fatal("failed to build map of tenants and projects: ", err)
	}

	closer, err := imp.PopulateWampRealms(theRouter, tenantProjectsMap, config.Prometheus)
	if err != nil {
		log.Fatal("could not populate wamp realms: ", err)
	}
	defer closer.Close()

	// Register WAMP websocket handler
	http.HandleFunc("/ws", srv.ServeHTTP) // This handler takes precedence over the root handler for /ws

	// Create and register the SPA handler
	spa := spaHandler{staticPath: config.Static, indexPath: config.Static + "/index.html"}
	http.Handle("/", spa)

	err = http.ListenAndServe(config.Wamp, nil)
	if err != nil {
		log.Fatal("could not serve http: ", err)
	}

	quit := make(chan string)
	<-quit
}
