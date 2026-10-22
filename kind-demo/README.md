# kind demo cluster

Optional **local Kubernetes playground** for trying KubeLite without touching a real cluster.

Requires [kind](https://kind.sigs.k8s.io/) (v0.33+) and Docker.

## Create cluster and demo workloads

```bash
kind create cluster --config kind-demo/kind-config.yaml --kubeconfig ~/.kube/kubelite-dev.yaml
kubectl --kubeconfig ~/.kube/kubelite-dev.yaml apply -f kind-demo/manifests/
kubelite --kubeconfig ~/.kube/kubelite-dev.yaml
```

## What gets deployed

Manifests under `manifests/` create namespace `kubelite-demo` with sample pods and workloads in many states (running, crash-looping, pending, bad image, Jobs, CronJobs, StatefulSet, DaemonSet, chatty log generators). No real secrets or external services — safe for local use only.

## Tear down

```bash
kind delete cluster --name kubelite-dev
```
