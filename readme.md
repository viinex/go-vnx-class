What is vnx-class
=================
This is a service aiming to help for viinex configuration and deployment at scale of more than one instance. It combines the Jsonnet configuration language engine, ETCD client, and a WAMP router.

Jsonnet is used for generating viinex configs, according to the needs of a specific customer in a specific project.

Etcd is used as a single storage for configuration-related information, as well as for holding the information on current distribution of viinex clusters to viinex instances.

WAMP is used to communicate with viinex instances and clusters. A WAMP realm (an isolated namespace) is being instantiated by `vnx-class` for every "project". Viinex instances should register themselves within the realm. Once this happens, `vnx-class` can manage the registered viinex instance, in particular -- push clusters' configuration onto that instance, and instruct it to run the clusters. Clusters, as they go live, may too register themselves in the same WAMP realm.

`vnx-class` means viinex cloud assistant.

Vocabulary
-----------
A few words about the terminology:
- viinex *instance* is a running `viinex` process. Typically (for the use together with `vnx-class`) it's configured so that it may handle requests to create and run dynamic viinex clusters.
- viinex *cluster* is a group of viinex applicative objects, which are created and being run together at the same time, linked together before they're started, so each object knows which other objects it works with. More information on viinex clusters is available in viinex documentation https://www.viinex.com/ViinexGuide.pdf
- *Tenant* is a customer, that is a person or an organization, who uses viinex applications
- *Project* is an isolated group of viinex instances and clusters.
- *Realm* is an isolated namespace maintained by WAMP router. Objects can be transparently addressed and address each other within the same realm. Within vnx-class, there is one realm created of every project, and the name of that realm matches the name of the project.
- The term "file" is often used throughout this document where it actually refers to a value within the ETCD database. This is always clear from the context: if the subject operating on a "file" is `vnx-class` service, -- then the file is a record in ETCD, unless it's `vnx-class`' own configuration file.


ETCD structure
==============

Top-level branches start under a common prefix which can be configured with config value `etcd-prefix`, and is empty by default.

There are three top-level branches: `templates`, `config` and `status`.

Branch `templates` should have a sub-branch `jsonnet`. Keys stored under `templates/jsonnet` should have a suffix of `jsonnet` and should be jsonnet files defining how viinex configuration is generated. The files referenced by jsonnets with `import` directive are being sought under the same location, that is under `templates/jsonnet/` prefix in etcd. The `templates` branch is shared across all tenants and projects.

Branch `config` contains two sub-levels named after Tenants and their Projects. (Tenant is a customer/organisation code name, while Project is a code name of the project). A project is considered an isolated namespace, where viinex instances and objects can potentially address each other, and users connecting to that namespace can address viinex objects. (This does not mean there is no access control enforced by the objects -- but access control is enforced by viinex objects, whereas Projects form the shape of infrastructure and are defined by `vnx-class`). Note that while Tenants are present in etcd hierarchy, -- at the time of this writing they don't form separate namespaces for projects. This means that no two projects may have the same name, even if they belong to different tenants.

Branch `status` is automatically maintained by `vnx-class` and should not be modified by any actor except vnx-class instances.

Under the prefix of `config/TENANT/PROJECT/` there should be a sub-prefix `clusters`, containing yaml files, each defining a configuration of an explicit cluster. There also should be auxiliary files named `recipe.yaml` and `mapping.yaml`, which define how the Jsonnet templates are applied to generate the viinex configuration from the yaml cluster configurations stored in etcd (that's `recipe.yaml`), and also how the clusters should be distributed (assigned) to viinex instances (that's `mapping.yaml`). Also there's another subprefix `wamp/` residing under `cconfig/TENANT/PROJECT/` branch; that subprefix holds the information to authenticate viinex instances and users connecting to WAMP realm associated with the project. In particular, for every WAMP user there should be two keys, `config/TENANT/PROJECT/wamp/USERNAME/role`, holding the role name of that user, and `config/TENANT/PROJECT/wamp/USERNAME/cryptosign`, holding the Cryptosign public key of the user.

In addition to that, `config/TENANT/PROJECT/` may store additional subkeys with other arbitrary names, holding any data which may be needed to generate configuration. These keys can be referenced as external files from within Jsonnet code.

To sum up, the `config` branch in etcd should look as follows:
```
config
  `- TENANT1
      `- PROJECT1
          |- clusters
          |    |- SITE1PART1.yaml
          |    |- SITE1PART2.yaml
          |    |- ...
          |    `- SITEnPARTk.yaml
          |- wamp
          |    |- USERNAME1
          |    |    |- role
          |    |    `- cryptosign
          |    |- USERNAME2
          |    |    |- role
          |    |    `- cryptosign
          |    |- ...
          |    `- USERNAMEj
          |         |- role
          |         `- cryptosign
          |- recipe.yaml
          |- mapping.yaml
          |- ...
          |- arbitrary-data.csv
          `- ...
```

Let's take a closer look at how these files may look like.

Recipe
------
In general vnxclass does not impose any requirements on the structure of configuration format of clusters. Strictly speaking to does not even have to be yaml, although changing this requires a slight change in vnxclass source code. The only file related to generation of viinex config which has a pre-defined struct for vnx-class is `recipe.yaml`. Namely, it may have the following format:

```
ext-str-file:
  csv1: data.csv
  csv2: data2.csv

ext-str:
  somePrivateEndpoint: http://10.0.1.258:78080/hook/call

os-name: Linux

main: mainMyPrivate.jsonnet
```

Most important line here is `main: mainMyPrivate.jsonnet` which defines the entry point Jsonnet script for generating the configuration.

When `vnx-class` needs to or is asked to produce the configuration for a specific cluster in a specific project, it does the following:
- reads the `/config/TENANT/PROJECT/recipe.yaml` for that project. (`recipe.yaml` key must exist in order for the realm to be recognized by `vnx-class` service. Only for the sub-branches of `/config` which contain `recipe.yaml` the realm is created and maintained by `vnx-class`).
- reads the yaml definition of the cluster configuration under `/config/TENANT/PROJECT/cluster/CLUSTERNAME.yaml`.
- for every entry mentioned in `ext-str-file` section of `recipe.yaml` file, -- respective file (key) is read from ETCD. The contents of the file is made available for Jsonnet engine as `ext-str-file` with the key name, as it's specified in the `ext-str-file` section of `recipe.yaml`. For example, if `recipe.yaml` contains the record
```
ext-str-file:
  csv1: data.csv
```
and the `/config/TENANT/PROJECT/data.csv` key in ETCD holds the data
```
1,foo,bar
2,bra,baz
```
then the above data will be made available for Jsonnet and can be referenced from within Jsonnet code as `std.extVar("csv1")`.
- All the entries mentioned under `ext-str` section of `recipe.yaml` are made available to Jsonnet engine, so that if `recipe.yaml` contains the record
```
ext-str:
  somePrivateEndpoint: http://10.0.1.258:78080/hook/call
```
then Jsonnet code may reference `std.extVar("somePrivateEndpoint")`.
- In addition to that, `vnx-class` injects three more `ext-var`s into Jsonnet VM before running it: `CID`, containing the name of the cluster, `confYaml`, containing the YAML configuration file for the cluster, and `OSName`, containing the value of `os-name` property from the recipe (or the value of `Linux` if `os-name` is not specified).
- Finally, the Jsonnet entry point defined by the recipe (or `main.jsonnet`, if none is given) is loaded from the file with according name which should be stored under `/templates/jsonnet`. That file, in its turn, may reference other Jsonnet code via Jsonnet `import` directives. It may also reference external variables mentioned above, via Jsonnet function `std.extVar`. In particular, it would typically `std.parseYaml(std.extVar('confYaml'))` to parse the YAML cluster's configuration file, and process it further according to the logic specified in Jsonnet code and combining that with other given parameters.

Note that Jsonnet language is designed to be free from side effects, which is why the output of a Jsonnet program is fully defined by its text and the provided `extVar`s.

Sometimes it can be handy to test what's being generated and debug Jsonnet code. While this is totally possible to do offilne, without the use of `vnx-class`, -- the latter still offers the WAMP endpoint to retrieve the configuration of a cluster. Note that configuration may contain sensitive information, which is why the actual WAMP credentials are required to access that endpoint. The actual WAMP call may be performed with the help from `wick` utility available at https://github.com/viinex/wick and would look as follows:
```
export WICK_URL=wss://cloud.viinex.com/ws
export WICK_AUTHMETHOD=cryptosign
export WICK_AUTHID=viinex
export WICK_PRIVATE_KEY=f3d2dd1aa07865df14152b20e732c7fc179a5705e33827a1ac087f730099ad81
export WICK_REALM=myProject

wick call com.viinex.infra.get_cluster_config CLUSTERNAME | jq '.[0]' -r
```
This `com.viinex.infra.get_cluster_config` is a WAMP endpoint published by the `vnx-class` itself within every realm it creates for every Project found for every Tenant within the underlying etcd database.

The step of generation a viinex config from the cluster's YAML config file and Jsonnet code can also be performed "offline", without any help from `vnx-class` service -- see the following section for that. Note that Jsonnet code provided three requires Jsonnet version 0.21; same version is used as the dependency to build `vnx-class`. 


Jsonnet code and sample clusters configuration
----------------------------------------------
With Jsonnet files held under etcd and executed as viinex configuration for a specific cluster is requested, the format of yaml clusters' config can be made as customizable as needed. Literally, it may be as terse as one would need, leaving most of the decisions to the Jsonnet code, or even combinig that with arbitrary external data: as an example, one may choose to put the list of cameras into CSV files under ETCD, and within cluster's YAML configuration only select which CSV file to use to populate respective viinex config. All the application-specific logic knowing which viinex objects need to be created and which links need to be established is then left out to Jsonnet code, and it effectively defines the scope and nature of the application being built.

A set of Jsonnet files which can be used as an example, can be found at https://github.com/viinex/config-templates. The content of this repository can be uploaded into etcd (there's also a Makefile which defines the target `etcdupload` for doing that; be aware that it overwrites existing keys with the same names).

That repository defines a reasonable `main.jsonnet` to produce a viinex configuration from YAML files which may look like similar to the `sample-home.yaml` file provided within the same repository. Cluster's YAML config can be as simple as
```
onvif:
  - addr: 192.168.0.128
    id: fyard
    rec: motion
  - addr: 192.168.0.125
    id: attic
  - addr: 192.168.0.111
    id: porch
    rec: permanent

creds:
  - ['admin', '12345'] # common secret to access all cameras
```
This, together with the Jsonnet code in that repository, produces viinex config with three cameras, media archive, webrtc and rtsp server, and metrics.

The `config-templates` repository contains the Jsonnet files which can be used as library files to produce the configuration for viinex obects of other types (like replication sink and source, video analytics modules, etc), and to combine these objects and link them as necessary to get the reasonable viinex configuration. This logic can be used as a starting point to define viinex setups which best suite your project needs. The repository also defines a framework for authoring "applications", where a group of viinex objects in the cluster serve for a specific goal defined by the application. Currently two publicly provided applications are NVR (in `app-nvr.jsonnet`) and a license plate recognition box (`app-alpr-box.jsonnet`). The selection of a specific application is made with the `type` property of the `app` section of cluster's YAML config, as shown here https://github.com/viinex/config-templates/blob/d8981dba768bce570c306b39c76dc841555f65cd/sample-home.yaml#L51. Adding more applications is another way of customization of this framework. If you would like to share your customizations, pull requests into https://github.com/viinex/config-templates are welcomed.

Also worth noting that the approach shown in that repository is relatively radical (at least non-conservative), as it re-defines the format of viinex configuration, in attempt to simplify it and take a number of decisions, while leaving a room for flexibility. There can be more conservative approach, where the Jsonnet code is used to simply replicate existing viinex configuration, with just a minor things being parameterized, and the parameters could be taken from cluster's YAML config. This approach is useful where the existing and tested viinex configuration just needs to be replicated across a number of devices. This existing viinex configuration can be literally used as the base of Jsonnet program (as Jsonnet language is a superset over JSON), with modifications only necessary to reflect how parameters coming from cluster's YAML file are taken into account. Then, one would need to author respective `myViinexAppTemplate.jsonnet` and specify it as the `main` entry point in the `recipe.yaml`.

WAMP credentials
----------------
The keys and values under the `/config/TENANT/PROJECT/wamp/` prefix in etcd define a user name, a role corresponding to that user, and public cryptosign key of the user. Here the term "user" means any actor which connects to the WAMP realm. This can be an actual user, or it can be viinex instance itself. 

It is highly recommended that every viinex instance is given its own credentials to access WAMP router. A pair of cryptosign keys can be generated with `wick` utility, with command `wick keygen`. Respective private cryptosign key should be stored on the host where viinex runs; a good place to store it would be file `/etc/viinex.conf.d/vars.j`. It may look as follows:
```
{
    "INSTANCE_ID": "i_347ef457",
    "REALM": "myProject",
    "URL": "wss://cloud.viinex.com/ws",
    "AUTHID": "viinex_347ef457",
    "PRIVATE_KEY": "27dbc91cc759194e00fc32d1c387fb9b4c646423bf4124b17c967377310da3a8"
}
```
The path to such file can be used as an argument to the parameter `--variables` when running a viinex instance. The file itself can be protected with permissions 600. The actual variables listed in this file are used by the pre-defined viinex configuration `vnx-class-instance.json` which is installed into `/usr/share/viinex/js/modules` and can be used without modifications as the configuration for a viinex instance to run under supervision of `vnx-class`.

For the variables as defined above, the following entries should be present in etcd:
```
config
  `- TENANT1
      `- myProject
          |- wamp
          |    |- viinex_347ef457
          |    |    |- role        -> viinex
          |    |    `- cryptosign  -> 0250a68c8bb15dc44602425978651c675d994c9d108d5d1bda850fb96dc74fff
```
Here, the part of key names, `viinex_347ef457`, matches the `AUTHID` variable set on the host where viinex runs. This is the "login" which is used to authenticate an actor which logs in to WAMP realm, in this case -- a viinex instance on a particular host. It is recommended that every host has its own login, to simplify credentials revovation when the host is decomissioned.

The content of the key `role` has the value of `viinex`. Currently the following roles are supported by `vnx-class`:
- viinex
- user
- operator

The role `viinex` should be used for accounts associated with viinex instances. Role `user` is meant to be associated with accounts associated with the users of viinex functionality. The role `operator` is for users who access both viinex and vnx-class functionality.

Note that while this model is pretty much coarse-grained, this does not affect viinex' security model. The latter is still available, but it needs to be configured within viinex itself; it operates with viinex users, roles, objects and endpoint types. This is a different layer of access control. The three roles described here define which WAMP endpoints may be addressed by every specific actor, and in what way. It may be treated as WAMP transport level security, while the access control lists used by viinex `authnz` object is the "application level" security. The latter may be omitted for viinex instances and clusters supervised by `vnx-class` (thus allowing one to configure within viinex an HTTP or RTSP server which does not require authentication), however the WAMP transport level security is mandatory.

The content of the key `cryptosign` contains the Crypotosign public key matching the private key set up at the host where viinex runs.

WAMP credentials can be freely changed in the etcd database during `vnx-class` runtime; they go in effect immediately for new connection attempts. However these changes do not have effect on existing WAMP connections.


Cluster to instance mapping
---------------------------
One of the important functions of `vnx-class` is to define how the viinex clusters should be distributed across viinex instances. Depending on the application requirements, it may be necessary that clusters are automatically started on the instances which get commissioned. There can also be various policies with regards to which viinex instances are used to run which clusters. For example, if there is a number of servers dedicated to run some video processing, and there is a goal to run the processing over several hundreds of video cameras, however media streams from these cameras can be uniformly accessible from any of the servers, -- it may be irrelevan how exactly the cameras are distributed across servers. One of the ways of managing this use case can be to group all the cameras into (relatively large) number of clusters, and allow vnx-class to automatically distribute resulting clusters onto viinex instances, where one instance runs on every available server. In contrast, if there is a non-uniform access to certain resources, like media streams, or media storage, -- it can be necessary to make the association of a viinex cluster to viinex instance "sticky", to make sure that the configuration which pulls data from specific cameras runs on the host which actually has access to these cameras, or if some cameras need to be stored on a specific media archive -- according viinex cluster needs to be run on the instance which has that disk storage available.

For this reason the notion of cluster to instance mapping, or briefly just mapping, is introduced in vnx-class.

The mapping is defined by the `mapping.yaml` file under the Project branch within etcd database. The `mapping.yaml` file should have the following format:
```
type: static # possible options: none, static, iso

# for the case of "static" mapping:
cluster-to-instance:
  CLUSTER_k: INSTANCE_j

# for the case of "iso" mapping:
prefix-instance: i_
prefix-cluster: c_
```
Mapping of type `none` means that `vnx-class` does not handle cluster to instance mapping, and does not attempt to govern cluster's lifecycle.

Mapping of type `static` means that for every cluster there's explicit instruction which viinex instance should be used to run that cluster. When viinex instance gets online and registers within `vnx-class`, the latter defines the list of clusters associated with that instance, if necessary generates their configuration and pushes that configuration to the instance, and instructs the instance to start the clusters.

Mapping of type `iso` means that clusters are not explicitly associated with instances, however there is an isomorphism between them, -- that is, one instance runs exactly one cluster, and one cluster runs on exactly one instance. The isomorphism is based on the names of clusters and instances; their names are allowed to have entity type-specific prefixes but aside from these prefixes they should be equal to each other. In the above example, clusters may have names of `c_1`, `c_2`, and so on, where corresponding instances should have names of `i_1`, `i_2`, and so on.

This set of mapping strategies is obviously not comprehensive, in particular there's currently no mapping which does a dynamic allocation of clusters. New mapping policies may be added in the future.


Clusters lifecycle management
=============================
When `vnx-class` supervises viinex instances and clusters, it is expected to watch for instances getting registererd and de-registered at the WAMP router, to manage the clusters' configuration and their lifecycle within viinex instances.

This is achieved by means of cooperation between `vnx-class` and viinex objects running in the "main" (statically configured) cluster of a viinex instance.

In particular, every viinex install has a copy of configuration file named `vnx-class-instance.json` which is installed into `/usr/share/viinex/js/modules`. This file can be either copied into `/etc/viinex.conf.d/`, or directly specified as the configuration source for viinex. The file refences a number of variables, which is why the variables file also has to be specified at viinex startup (as a command line parameter).

The `vnx-class-instance.json` configuration file defines three objects: a WAMP client, which maintains connection to the WAMP router within `vnx-class`, an SQLite database which is used to store the clusters' configuration, and the script which makes sure that clusters get started upon viinex startup, and re-starts the cluster when its configuration gets changed in the database. Both the database and the script are registered within WAMP client, so they can be called by `vnx-class`, and this is what actually happens when the latter detects that viinex instance gets registered, or when the cluster's configuration changes in etcd database. 

Even though `vnx-class` is necessary to populate the instance-wise configuration database at viinex, -- the controller script is capable of starting previously configured clusters when `vnx-class` is unavailable.

