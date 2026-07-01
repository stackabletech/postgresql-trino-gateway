# PostgreSQL-Trino Gateway Helm Chart

Deploys the [PostgreSQL-Trino Gateway](https://github.com/stackabletech/postgresql-trino-gateway) on Kubernetes.

## Quick start

```bash
# Minimal install — gateway forwards to Trino at trino.example.com:8443
helm install gw ./deploy/helm/postgresql-trino-gateway \
    --set trino.host=trino.example.com \
    --set trino.port=8443

# Verify
kubectl get svc -l app.kubernetes.io/name=postgresql-trino-gateway

# Connect via port-forward
kubectl port-forward svc/gw-postgresql-trino-gateway 5432:5432
psql -h 127.0.0.1 -p 5432 -U trino -d memory
```

## Auth and TLS postures

The gateway has two security planes — a listener side (PG clients →
gateway) and a Trino side (gateway → Trino). The chart exposes the same
matrix the binary's `--auth` / TLS flags do.

| Listener auth | Listener TLS | Posture | When to use |
|---|---|---|---|
| off | n/a | `auth.allowInsecureListener=true` (default) | Trusted network, NetworkPolicy gates access, or Trino itself authenticates |
| on  | required | `auth.enabled=true` plus `tls.secretClass` or `tls.existingSecret` | Production; cleartext passwords are forwarded to Trino as Basic auth over TLS |
| on  | not set | refused at startup | Chart-level pre-check fails |

Listener TLS materials can come from one of:

- `tls.secretClass` — a Stackable SecretClass; the secret-operator
  provisions a per-pod cert at `/stackable/listener-tls/{tls.crt,tls.key}`.
- `tls.existingSecret` — a pre-existing `kubernetes.io/tls` Secret
  (with `tls.crt` and `tls.key` keys), mounted at the same path.

Set at most one. Setting both is rejected at template time.

### Trino-side TLS

`trino.ssl=true` connects to Trino over HTTPS using the system trust
store. For private CAs, either set `trino.tlsNoVerify=true` (skips
verification entirely) or rebuild the image with the CA injected into
`/etc/pki/ca-trust/source/anchors/`. First-class custom-CA support is
not configured here.

`trino.allowPlaintextAuth=true` is required when `auth.enabled=true`
and `trino.ssl=false` (cleartext password forwarded to Trino over plain
HTTP). Use only with a loopback or otherwise-trusted Trino.

## Connectivity

The Service is `ClusterIP` on TCP/5432 by default. PG protocol is not
HTTP, so a standard ingress controller does not apply. Three ways to
expose the gateway outside the cluster:

1. `kubectl port-forward` — for development or ad-hoc Power BI Desktop.
2. `service.type=LoadBalancer` — gives the Service an external L4 IP.
3. An L4-aware ingress (e.g. nginx with `tcp-services`) or a
   `Gateway` resource using a TCPRoute.

## Scaling

`replicaCount` defaults to `1` and should not be raised without solving
sticky routing first. The cancel registry and other per-connection
state are in-memory per pod; a PG `CancelRequest` must land on the same
replica as the original connection to work, and the plain `ClusterIP`
Service this chart creates does not guarantee that (no
`sessionAffinity`, round-robin by default). Running multiple replicas
today means query cancellation silently no-ops for connections routed
to the wrong pod.

## Probes

Both readiness and liveness use TCP socket probes against the listening
port. The gateway has no HTTP health endpoint — it speaks only PG wire
protocol.

## Graceful shutdown

`terminationGracePeriodSeconds` controls how long Kubernetes waits
after SIGTERM before SIGKILL. `shutdownDrainTimeoutSecs` controls how
long the gateway itself waits for in-flight queries to complete before
aborting them. The chart defaults are 30 and 25 respectively, leaving a
5-second safety margin under the kubelet's SIGKILL.

## Values reference

See `values.yaml` for the full list. Notable groups:

- `image.*` — registry, repo, tag, pull policy, pull secrets.
- `service.*` — type, port, target port, annotations.
- `resources.*` — CPU/memory requests and limits.
- `trino.*` — backend host, port, catalog, schema, user, SSL/auth flags.
- `auth.*` — listener-side authentication enable, insecure-listener opt-in.
- `tls.*` — listener TLS source (secretClass or existingSecret).
- `livenessProbe`, `readinessProbe`, `startupProbe` — TCP socket probes.
- `podSecurityContext`, `securityContext` — non-root, read-only root FS,
  drop ALL capabilities.
- `maxConnections` — concurrent connection cap.
- `logLevel` — `RUST_LOG` value (e.g. `postgresql_trino_gateway=debug`).

## Uninstall

```bash
helm uninstall gw
```

The chart creates only namespaced resources (Deployment, Service,
ServiceAccount, optional Secret pull-through). No CRDs, no
cluster-scoped resources.
