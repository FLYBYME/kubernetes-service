const { MoleculerRetryableError, MoleculerClientError } = require("moleculer").Errors;

const k8s = require('@kubernetes/client-node');
const stream = require('stream');
const request = require('request');

const fs = require('fs').promises;

const Datastore = require('../lib/nedb/index');

/**
 * attachments of addons service
 */
module.exports = {
    /**
     * Service name
     */
    name: "kubernetes",

    /**
     * Version number
     */
    version: 1,

    mixins: [],

    /**
     * Service dependencies
     */
    dependencies: [],
    /**
     * Service settings
     */
    settings: {
        rest: "/v1/kubernetes/",


        apis: [
            'CoreV1Api',
            'NetworkingV1Api',
            'AppsV1Api',
            'NodeV1beta1Api',
            'BatchV1Api',
            'AuthenticationV1Api',
            'CertificatesV1Api',
            'DiscoveryV1beta1Api',
            'EventsV1beta1Api',
            'PolicyV1beta1Api',
            'StorageV1beta1Api',
            'CustomObjectsApi'
        ],
        core: [
            'pods',
            'endpoints',
            'services',
            'nodes',
            'namespaces',
            'persistentvolumes',
            'persistentvolumeclaims',
            'configmaps',
            'secrets',
            //'componentstatuses',
            'limitranges',
            'resourcequotas',
            'secrets',
            'serviceaccounts',
            //'customresourcedefinitions'
        ],
        apps: [
            'replicasets',
            'daemonsets',
            'statefulsets',
            'deployments'
        ],
        batch: [
            'jobs',
            'cronjobs'
        ],
        networking: [
            'networkpolicies'
        ],
        node: [
            'nodes',
            'runtimeclasses'
        ],
        authentication: [
            'tokenreviews',
            'selfsubjectaccessreviews',
            'subjectaccessreviews',
            'localsubjectaccessreviews',
            'selfsubjectrulesreviews'
        ],
        certificates: [
            'certificatesigningrequests'
        ],
        discovery: [
            'endpointslices'
        ],
        events: [
            'events'
        ],
        policy: [
            'podsecuritypolicies'
        ],
        storage: [
            'storageclasses',
            'volumeattachments'
        ],
        custom: [
            'apis',
            'apiextensions',
            'apiservices',
            'controllerrevisions',
            'customresourcedefinitions',
            'events',
            'flowschemas',
            'priorityclasses',
            'poddisruptionbudgets',
            'podsecuritypolicies',
            'podtemplates',
            'rangeallocation',
            'runtimeclasses',
            'storageclasses',
            'volumeattachments'
        ],

        config: {
            configFolder: "./config",
        }
    },

    /**
     * hooks
     */
    hooks: {

    },

    /**
     * Actions
     */

    actions: {
        /**
         * load kubernetes configuration
         * 
         * @actions
         * @param {Object} params - query parameters
         * 
         * @returns {Promise} object
         */
        loadKubeConfig: {
            rest: {
                method: "POST",
                path: "/loadKubeConfig"
            },
            params: {
                name: { type: "string" },
                kubeconfig: { type: "string" }
            },
            async handler(ctx) {
                // save to file
                await fs.writeFile(`${this.settings.config.configFolder}/${ctx.params.name}.kubeconfig`, ctx.params.kubeconfig);

                return this.loadKubeConfig(ctx.params.name, ctx.params.kubeconfig);
            }
        },

        exec: {
            rest: 'POST /exec',
            params: {
                cluster: { type: "string", default: 'default', optional: true },
                namespace: { type: "string", optional: false },
                name: { type: "string", optional: false },
                container: { type: "string", optional: true },
                command: {
                    type: "array",
                    items: {
                        type: "string",
                        convert: true
                    },
                    optional: false
                },
            },
            async handler(ctx) {
                const params = Object.assign({}, ctx.params);

                const config = this.configs.get(params.cluster);
                if (!config) {
                    throw new MoleculerClientError('Cluster not found', 404, 'CLUSTER_NOT_FOUND', {
                        cluster: params.cluster,
                    });
                }

                // check if we have a container
                if (!params.container) {
                    // get pod
                    const pod = await this.actions.readNamespacedPod({
                        cluster: params.cluster,
                        namespace: params.namespace,
                        name: params.name
                    });
                    params.container = pod.spec.containers[0].name;
                }

                const exec = new k8s.Exec(config.kc);

                const readStream = new stream.PassThrough();
                const writeStream = new stream.PassThrough();
                const errorStream = new stream.PassThrough();


                const readChunks = [];
                const errorChunks = [];
                writeStream.on('data', (c) => {
                    readChunks.push(c.toString());
                });
                errorStream.on('data', (c) => {
                    errorChunks.push(c.toString());
                });
                console.log(params.namespace, params.name, params.container, params.command)
                return exec.exec(params.namespace, params.name, params.container, params.command, writeStream, errorStream, null, false)
                    .then(() => {
                        return new Promise((resolve, reject) => {
                            writeStream.once('end', () => {
                                resolve({
                                    stdout: readChunks.join(''),
                                    stderr: errorChunks.join(''),
                                });
                            });
                        });
                    }).catch((err) => {
                        throw new MoleculerRetryableError(err.message, 500, 'EXEC_ERROR', {
                            cluster: params.cluster,
                            namespace: params.namespace,
                            name: params.name,
                            container: params.container,
                            command: params.command,
                        })
                    })
            }
        },

        /**
         * find one object
         * 
         * @actions
         * @param {Object} params - query parameters
         * 
         * @returns {Promise} object
         */
        findOne: {
            params: {

            },
            async handler(ctx) {
                const params = Object.assign({}, ctx.params);
                const fields = {}
                const sort = {}

                if (Array.isArray(params.fields)) {
                    for (let index = 0; index < params.fields.length; index++) {
                        const element = params.fields[index];
                        fields[element] = 1
                    }
                    delete params.fields
                } else if (params.fields) {
                    fields[params.fields] = 1
                    delete params.fields
                }
                if (Array.isArray(params.sort)) {
                    for (let index = 0; index < params.sort.length; index++) {
                        const element = params.sort[index];
                        sort[element] = 1
                    }
                    delete params.sort
                } else if (params.sort) {
                    sort[params.sort] = 1
                    delete params.sort
                }

                return new Promise((resolve, reject) => {
                    this.db.findOne({ ...this.flattenObject(params) }, fields).sort(sort).exec(function (err, docs) {
                        if (err) {
                            reject(err)
                        } else {
                            resolve(docs)
                        }
                    });
                })
            }
        },

        /**
         * find one object
         * 
         * @actions
         * @param {Object} params - query parameters
         * 
         * @returns {Promise} object
         */
        find: {
            params: {},
            async handler(ctx) {
                const params = Object.assign({}, ctx.params);
                const fields = {}
                const sort = {}

                if (Array.isArray(params.fields)) {
                    for (let index = 0; index < params.fields.length; index++) {
                        const element = params.fields[index];
                        fields[element] = 1
                    }
                    delete params.fields
                } else if (params.fields) {
                    fields[params.fields] = 1
                    delete params.fields
                }
                if (Array.isArray(params.sort)) {
                    for (let index = 0; index < params.sort.length; index++) {
                        const element = params.sort[index];
                        sort[element] = 1
                    }
                    delete params.sort
                } else if (params.sort) {
                    sort[params.sort] = 1
                    delete params.sort
                }
                return new Promise((resolve, reject) => {
                    this.db.find({ ...this.flattenObject(params) }, fields).sort(sort).exec(function (err, docs) {
                        if (err) {
                            reject(err)
                        } else {
                            resolve(docs)
                        }
                    });
                })
            }
        },
    },

    /**
     * Methods
     */
    methods: {

        /**
         * load kubernetes configuration
         * 
         * @param {String} name - name of the config
         * @param {String} kubeconfig - path to kubeconfig file
         * 
         * @returns {Object} k8s config
         */
        async loadKubeConfig(name, kubeconfig) {
            const config = { name, api: {} };

            config.kc = new k8s.KubeConfig();
            config.kc.loadFromString(kubeconfig);

            config.metrics = new k8s.Metrics(config.kc);
            config.watch = new k8s.Watch(config.kc);
            config.logger = new k8s.Log(config.kc);

            // load all apis
            for (let api of this.settings.apis) {
                config.api[api] = this.loadApi(api, config.kc);
            }

            if (this.configs.has(name)) {
                // stop watching resources
                await this.stopWatchingCluster(name);

                // remove config
                this.configs.delete(name);
            }

            this.configs.set(name, config);

            const list = [...this.settings.core, ...this.settings.apps, ...this.settings.batch];

            for (let index = 0; index < list.length; index++) {
                this.watchAPI(config, list[index], ['ADDED', 'MODIFIED', 'DELETED'])
            }

            return config;
        },

        /**
         * watch kubernetes api
         * 
         * @param {Object} config - k8s config
         * @param {String} api - name of the api
         * @param {Array} events - events to watch
         * 
         * @returns {Promise}
         */
        async watchAPI(config, api, events = ['ADDED', 'MODIFIED', 'DELETED']) {
            if (this.closed) return;

            const cluster = config.name;

            let path = `/api/v1/${api}`;


            if (this.settings.apps.includes(api)) {
                path = `/apis/apps/v1/${api}`;
            } else if (this.settings.batch.includes(api)) {
                path = `/apis/batch/v1/${api}`;
            } else if (this.settings.networking.includes(api)) {
                path = `/apis/networking.k8s.io/v1/${api}`;
            } else if (this.settings.node.includes(api)) {
                path = `/api/v1/${api}`;
            } else if (this.settings.authentication.includes(api)) {
                path = `/apis/authentication.k8s.io/v1/${api}`;
            } else if (this.settings.certificates.includes(api)) {
                path = `/apis/certificates.k8s.io/v1/${api}`;
            } else if (this.settings.discovery.includes(api)) {
                path = `/apis/discovery.k8s.io/v1/${api}`;
            } else if (this.settings.events.includes(api)) {
                path = `/apis/events.k8s.io/v1/${api}`;
            } else if (this.settings.policy.includes(api)) {
                path = `/apis/policy/v1beta1/${api}`;
            } else if (this.settings.storage.includes(api)) {
                path = `/apis/storage.k8s.io/v1/${api}`;
            } else if (this.settings.custom.includes(api)) {
                path = `/apis/${api}`;
            }

            this.logger.info(`loading kube api ${path}`);


            this.kubeEvents[`${cluster}-${api}`] = await config.watch.watch(path, {}, (phase, resource) => {

                const event = {
                    ...resource,
                    _id: resource.metadata.uid,
                    cluster: config.name,
                    phase: phase.toLocaleLowerCase()
                }

                const kind = event.kind.toLocaleLowerCase()

                delete event.metadata.managedFields
                if (event.phase == 'deleted') {
                    this.db.remove({ _id: event._id }, {}, (err, numRemoved) => {
                        if (err) {
                            console.log(event, err)
                        }
                        this.broker.emit(`kubernetes.${kind}s.deleted`, event)
                    });
                } else {
                    this.db.findOne({ _id: event._id }, (err, docs) => {
                        if (err) {
                            console.log(event, err)
                        } else {
                            let isNew = !docs
                            this.db.update({ _id: event._id }, event, {
                                upsert: true
                            }, (err, numAffected, affectedDocuments, upsert) => {
                                if (err) {
                                    console.log(event, err)
                                } else {
                                    this.broker.emit(`kubernetes.${kind}s.${isNew ? 'added' : 'modified'}`, event)
                                }
                            });
                        }
                    });
                }
            }, (err) => {
                if (err) {
                    this.logger.error(`Error watching ${cluster} ${api} resources`, err);
                }
                delete this.kubeEvents[`${cluster}-${api}`];
                setTimeout(() => {
                    this.watchAPI(config, api, events)
                }, err ? 15000 : 100)
            })
        },

        /**
         * load kubernetes api
         * 
         * @param {String} api - name of the api
         * @param {Object} kc - kubeconfig
         * 
         * @returns {Object} k8s api
         */
        loadApi(api, kc) {
            const apiClass = k8s[api];
            // make client
            const client = kc.makeApiClient(apiClass);
            return client;
        },

        /**
         * watch resources
         * 
         * @param {Object} config 
         */
        watchResources(config) {
            // watch all resources
            for (let api of this.settings.apis) {
                this.watchResource(api, config.api[api], config.watch, config);
            }
        },

        /**
         * watch a resource
         * 
         * @param {String} api - name of the api
         * @param {Object} client - k8s api client
         * @param {Object} watch - k8s watch client
         * @param {Object} config - k8s config
         */
        async watchResource(api, client, watch, config) {
            if (this.closed || config.closed) return;

            const cluster = config.name;

            let path = `/api/v1/${api}`;

            if (this.settings.apps.includes(api)) {
                path = `/apis/apps/v1/${api}`;
            } else if (this.settings.batch.includes(api)) {
                path = `/apis/batch/v1/${api}`;
            } else if (this.settings.networking.includes(api)) {
                path = `/apis/networking.k8s.io/v1/${api}`;
            }

            this.logger.info(`Watching ${cluster} ${api} resources at ${path}`);

            this.kubeEvents[`${cluster}-${api}`] = await watch.watch(path, {}, (phase, resource) => {

                const event = Object.assign({}, {
                    ...resource,
                    cluster: cluster,
                    phase: phase.toLocaleLowerCase(),
                    _id: resource.metadata.uid,// id is tracked by uid
                });

                const kind = event.kind.toLocaleLowerCase();

                delete event.metadata.managedFields;
                delete event.metadata.selfLink;
                console.log(kind)

                if (event.phase == 'deleted') {
                    this.db.remove({ _id: event._id }, {}, (err, numRemoved) => {
                        if (err) {
                            this.logger.error(`Error removing document ${event._id}`, err);
                        }
                        this.broker.emit(`kubernetes.${kind}s.deleted`, event);
                    });
                } else {
                    this.db.findOne({ _id: event._id }, (err, docs) => {
                        if (err) {
                            this.logger.error(`Error removing document ${event._id}`, err);
                        } else {
                            let isNew = !docs;

                            this.db.update({ _id: event._id }, event, {
                                upsert: true
                            }, (err, numAffected, affectedDocuments, upsert) => {
                                if (err) {
                                    this.logger.error(`Error removing document ${event._id}`, err);
                                } else {
                                    this.broker.emit(`kubernetes.${kind}s.${isNew ? 'added' : 'modified'}`, event);
                                }
                            });
                        }
                    });
                }

            }, (err) => {
                if (err) {
                    //console.log(err)
                    if (err.code == 'ECONNRESET' || err.code == 'ECONNREFUSED') {
                        this.logger.error(`Error watching ${cluster} ${api} resources`, err);
                    }
                }
                delete this.kubeEvents[`${cluster}-${api}`];
                setTimeout(() => {
                    this.watchResource(api, client, watch, config);
                }, err ? 15000 : 100)
            });
        },

        /**
         * load kubernetes configuration from clusters
         * 
         * @param {Context} ctx - moleculer context
         * 
         * @returns {Promise} 
         */
        async loadKubeConfigs(ctx) {
            const configFiles = await fs.readdir(`${this.settings.config.configFolder}`);

            for (let configFile of configFiles) {
                const clusterName = configFile.replace('.kubeconfig', '');
                const kubeconfig = await fs.readFile(`${this.settings.config.configFolder}/${configFile}`);
                await this.loadKubeConfig(clusterName, kubeconfig);
            }
        },

        /**
         * stop watching resources
         * 
         * @param {Context} ctx - moleculer context
         * 
         * @returns {Promise}
         */
        async stopWatching(ctx) {
            this.closed = true;
            for (let key in this.kubeEvents) {
                this.kubeEvents[key].abort();
                delete this.kubeEvents[key];
            }
        },

        /**
         * stop watching cluster
         * 
         * @param {String} cluster - name of the cluster
         * 
         * @returns {Promise}
         */
        async stopWatchingCluster(cluster) {
            const config = this.configs.get(cluster);
            if (!config) return;

            config.closed = true;

            for (let key in this.kubeEvents) {
                if (key.startsWith(cluster)) {
                    this.kubeEvents[key].abort();
                    delete this.kubeEvents[key];
                }
            }
            // remove config
            this.configs.delete(cluster);
        },

        /**
         * Get all methods of a class
         * 
         * @param {Object} className 
         * 
         * @returns {Array} methods
         */
        getClassMethods(className) {
            if (!(className instanceof Object)) {
                this.logger.error(`Invalid class name provided: ${className}`);
                return [];
            }
            let ret = new Set();

            const blockList = [
                'constructor',
                'setDefaultAuthentication',
                'setApiKey',
                'addInterceptor',
                '__defineGetter__',
                '__defineSetter__',
                'hasOwnProperty',
                '__lookupGetter__',
                '__lookupSetter__',
                'isPrototypeOf',
                'propertyIsEnumerable',
                'toString',
                'valueOf',
                'toLocaleString'
            ]

            function methods(obj) {
                if (obj) {
                    let ps = Object.getOwnPropertyNames(obj);

                    ps.forEach(p => {
                        if (blockList.includes(p)) {
                            return;
                        }
                        if (obj[p] instanceof Function) {
                            ret.add(p);
                        } else {
                            //can add properties if needed
                        }
                    });

                    methods(Object.getPrototypeOf(obj));
                }
            }
            methods(className.prototype);

            return Array.from(ret);
        },

        /**
         * Parse the arguments of a function
         * 
         * @param {Function} func
         * 
         * @returns {Array} arguments
         */
        parseArgs(func) {
            return (func + '')
                .replace(/[/][/].*$/mg, '') // strip single-line comments
                .replace(/\s+/g, '') // strip white space
                .replace(/[/][*][^/*]*[*][/]/g, '') // strip multi-line comments  
                .split('){', 1)[0].replace(/^[^(]*[(]/, '') // extract the parameters  
                .replace(/=[^,]+/g, '') // strip any ES6 defaults  
                .split(',').filter(Boolean); // split & filter [""]
        },

        /**
         * flatten an object
         * 
         * @param {Object} ob
         * 
         * @returns {Object} flattened object
         */
        flattenObject(ob) {
            var toReturn = {};

            for (var i in ob) {
                if (!ob.hasOwnProperty(i)) continue;

                if ((typeof ob[i]) == 'object' && ob[i] !== null) {
                    var flatObject = this.flattenObject(ob[i]);
                    for (var x in flatObject) {
                        if (!flatObject.hasOwnProperty(x)) continue;

                        toReturn[i + '.' + x] = flatObject[x];
                    }
                } else {
                    toReturn[i] = ob[i];
                }
            }
            return toReturn;
        },

        /**
         * find one document
         * 
         * @param {Object} query
         * 
         * @returns {Promise} document
         */
        async findOne(query) {
            return new Promise((resolve, reject) => {
                this.db.findOne(query, (err, doc) => err ? reject(err) : resolve(doc))
            })
        },

        /**
         * find all documents
         * 
         * @param {Object} query
         */
        async find(query) {
            return new Promise((resolve, reject) => {
                this.db.find(query, (err, docs) => err ? reject(err) : resolve(docs))
            })
        }
    },
    /**
     * Service created lifecycle event handler
     */
    created() {
        this.configs = new Map();
        this.kubeEvents = {};

        this.db = new Datastore();
    },

    /**
     * Service started lifecycle event handler
     */
    async started() {
        setTimeout(() => {
            this.loadKubeConfigs(this.broker);
        }, 10000);
    },

    /**
     * Service stopped lifecycle event handler
     */
    async stopped() {
        await this.stopWatching(this.broker);
    }
};


function generateAPI(name) {
    const api = k8s[name]
    const list = module.exports.methods.getClassMethods(api);

    for (let index = 0; index < list.length; index++) {
        const key = list[index];
        const args = module.exports.methods.parseArgs(api.prototype[key].toString());
        const keySplit = key.split(/(?=[A-Z])/)
        const namespaced = keySplit[1].includes('Namespace')
        const type = keySplit[0]
        let rest = '';
        const params = {
            cluster: { type: "string", default: 'default', optional: true },
        }

        if (type == 'connect' || !!module.exports.actions[`${key}`]) {
            continue;
        }
        switch (type) {
            case 'read':
            case 'list':
            case 'get':
                rest += 'GET '
                break;
            case 'patch':
                rest += 'PATCH '
                break;
            case 'replace':
                rest += 'POST '
                break;
            case 'create':
                rest += 'POST '
                break;
            case 'delete':
                rest += 'DELETE '
                break;
            default:
                break;
        }

        if (namespaced) {
            rest += '/cluster/:namespace'
        }


        const method = keySplit.slice(namespaced ? 2 : 1).join('')
        rest += `/${method.toLocaleLowerCase()}`

        if (args[0] == 'name') {
            rest += '/:name'
        }

        const known = {
            name: { type: "string", optional: false },
            namespace: { type: "string", default: 'default', optional: true },
            pretty: { type: "boolean", default: true, optional: true },
            //dryRun: { type: "string", default: 'All', optional: true },
            body: { type: "object", optional: false },
            group: { type: "string", optional: false },
            version: { type: "string", optional: false },
            plural: { type: "string", optional: false },
        }


        for (let index = 0; index < args.length; index++) {
            const element = args[index];
            if (known[element]) {
                params[element] = known[element]
            } else {
                break;
            }
        }


        module.exports.actions[`${key}`] = {
            rest,
            params,
            async handler(ctx) {
                const params = Object.assign({}, ctx.params);
                const properties = []
                for (let index = 0; index < args.length; index++) {
                    const element = args[index];
                    if (known[element]) {
                        properties.push(params[element])
                    } else {
                        if (element == 'limit') {
                            properties.push(1000)
                        } else {
                            properties.push(undefined)
                        }
                    }
                }

                const config = this.configs.get(params.cluster);
                if (!config) {
                    throw new MoleculerClientError(
                        `Cluster ${params.cluster} not found`,
                        404, "CLUSTER_NOT_FOUND", { cluster: params.cluster }
                    );
                }

                if (type == 'patch') {
                    const options = { headers: { 'Content-type': 'application/merge-patch+json' } };
                    properties.pop()
                    properties.push(options)
                }

                return config.api[name][`${key}`](...properties)
                    .then((res) => {
                        return res.body
                    }).catch((res) => {
                        if (!res.body) {
                            throw res
                        }
                        throw new MoleculerClientError(
                            res.body.message,
                            res.body.code,
                            res.body.reason
                        );
                    });
            }
        }
    }
}

for (let index = 0; index < module.exports.settings.apis.length; index++) {
    const api = module.exports.settings.apis[index];
    generateAPI(api);
}