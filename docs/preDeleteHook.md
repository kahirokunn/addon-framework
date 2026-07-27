# Overview

Addon-framework supports `Job` and `Pod` manifests as pre-delete hooks for an add-on.
These hooks are Kubernetes resources delivered through a `ManifestWork`; they are not Helm hooks.

# How it works

1. Add the `addon.open-cluster-management.io/addon-pre-delete` annotation to a `Job` or `Pod` manifest.
2. Addon-framework excludes the hook from the normal deployment `ManifestWork`.
3. When the `ManagedClusterAddOn` is deleting, addon-framework applies the hook in a `ManifestWork` named `addon-<addon name>-pre-delete`.
4. A hook is complete only after every `Job` reports `Complete=True` and every `Pod` reaches the `Succeeded` phase.
5. Addon-framework reports the result through the `HookManifestCompleted` condition.
6. After all hooks complete, addon-framework deletes every `ManifestWork` it owns before releasing its cleanup finalizers.

The controller derives the required `ManifestWork` set from the current `ManagedClusterAddOn`,
its finalizers, and the existing works on every reconciliation. The status condition reports
progress; it is not used as the source of truth for cleanup.

# Example

See the example [helloworld_helm](../examples/helloworld_helm)
