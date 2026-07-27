# Overview

Addon-framework supports `Jobs` and `Pods` as pre-delete manifests for an add-on. This is an
addon-framework lifecycle feature, not an implementation of Helm's `helm.sh/hook` contract or its
other hook phases.

# How it works

1. Add the annotation `addon.open-cluster-management.io/addon-pre-delete` to the `Jobs` or `Pods` manifests.
2. The `Jobs` or `Pods` will not be applied until the managedClusterAddon is deleted.
3. The `Jobs` or `Pods` will be applied on the managed cluster by applying the manifestWork named `addon-<addon name>-pre-delete` when the managedClusterAddon is deleting.
4. The hook is complete only after every `Job` is `Complete` or every `Pod` is in the `Succeeded` phase.
5. Addon-framework then deletes all ManifestWorks it owns for the add-on.

Addon-framework reports the observed result with the `PreDeleteHookCompleted` condition. Conditions
are not used as reconciliation input: the controller derives the required Work set from the
`ManagedClusterAddOn`, its finalizers, and the actual ManifestWorks on every reconciliation.
`HookManifestCompleted` is written with the same value for compatibility and is deprecated.

# Example

See the example [helloworld_helm](../examples/helloworld_helm)
