[![Moleculer](https://badgen.net/badge/Powered%20by/Moleculer/0e83cd)](https://moleculer.services)

# Kubernetes Service

This service provides integration with Kubernetes clusters using the Moleculer framework. It allows you to manage and interact with Kubernetes resources programmatically.

## Features

- Load Kubernetes configurations
- Execute commands in Kubernetes pods
- Watch Kubernetes resources
- Manage Kubernetes resources through Moleculer actions

## Configuration

The service reads Kubernetes configurations from the `config` folder. Each configuration file should be named as `<cluster-name>.kubeconfig`.

## Actions

### loadKubeConfig

Load a Kubernetes configuration.

- **Params:**
  - `name` (string): The name of the configuration.
  - `kubeconfig` (string): The content of the kubeconfig file.

### exec

Execute a command in a Kubernetes pod.

- **Params:**
  - `cluster` (string, optional): The name of the cluster. Default is 'default'.
  - `namespace` (string): The namespace of the pod.
  - `name` (string): The name of the pod.
  - `container` (string, optional): The name of the container. If not provided, the first container in the pod will be used.
  - `command` (array of strings): The command to execute.

### findOne

Find one document in the database.

- **Params:** Query parameters to filter the documents.

### find

Find documents in the database.

- **Params:** Query parameters to filter the documents.

## Methods

### loadKubeConfig

Load a Kubernetes configuration from a kubeconfig string.

- **Params:**
  - `name` (string): The name of the configuration.
  - `kubeconfig` (string): The content of the kubeconfig file.

### watchAPI

Watch a Kubernetes API for changes.

- **Params:**
  - `config` (object): The Kubernetes configuration.
  - `api` (string): The name of the API to watch.
  - `events` (array of strings): The events to watch (default: ['ADDED', 'MODIFIED', 'DELETED']).

### loadApi

Load a Kubernetes API client.

- **Params:**
  - `api` (string): The name of the API.
  - `kc` (object): The Kubernetes configuration.

### watchResources

Watch all resources for a given configuration.

- **Params:**
  - `config` (object): The Kubernetes configuration.

### watchResource

Watch a specific resource for changes.

- **Params:**
  - `api` (string): The name of the API.
  - `client` (object): The Kubernetes API client.
  - `watch` (object): The Kubernetes watch client.
  - `config` (object): The Kubernetes configuration.

### loadKubeConfigs

Load all Kubernetes configurations from the config folder.

### stopWatching

Stop watching all resources.

### stopWatchingCluster

Stop watching resources for a specific cluster.

- **Params:**
  - `cluster` (string): The name of the cluster.

### getClassMethods

Get all methods of a class.

- **Params:**
  - `className` (object): The class to get methods from.

### parseArgs

Parse the arguments of a function.

- **Params:**
  - `func` (function): The function to parse.

### flattenObject

Flatten an object.

- **Params:**
  - `ob` (object): The object to flatten.

### findOne

Find one document in the database.

- **Params:**
  - `query` (object): The query to filter the documents.

### find

Find documents in the database.

- **Params:**
  - `query` (object): The query to filter the documents.

## Lifecycle Events

### created

Service created lifecycle event handler.

### started

Service started lifecycle event handler.

### stopped

Service stopped lifecycle event handler.

## Usage

To use this service, include it in your Moleculer project and configure it according to your needs. You can then call the actions provided by the service to interact with your Kubernetes clusters.

```javascript
const { ServiceBroker } = require("moleculer");
const KubernetesService = require("./services/kubernetes.service");

const broker = new ServiceBroker();

broker.createService(KubernetesService);

broker.start().then(() => {
    // Call actions
    broker.call("kubernetes.loadKubeConfig", { name: "default", kubeconfig: "..." });
});
