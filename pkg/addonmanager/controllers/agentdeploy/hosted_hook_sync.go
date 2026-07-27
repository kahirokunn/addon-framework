package agentdeploy

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"

	addonapiv1beta1 "open-cluster-management.io/api/addon/v1beta1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workapiv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/addon-framework/pkg/addonmanager/constants"
	"open-cluster-management.io/addon-framework/pkg/agent"
	"open-cluster-management.io/sdk-go/pkg/basecontroller/factory"
)

type hostedHookSyncer struct {
	buildWorks buildDeployHookFunc

	applyWork func(ctx context.Context, appliedType string,
		work *workapiv1.ManifestWork, addon *addonapiv1beta1.ManagedClusterAddOn) (*workapiv1.ManifestWork, error)

	deleteWork func(ctx context.Context, workNamespace, workName string) error

	getWorkByAddon func(addonName, addonNamespace string) ([]*workapiv1.ManifestWork, error)

	listWorkByAddon listWorksByAddonFunc

	getCluster func(clusterName string) (*clusterv1.ManagedCluster, error)

	agentAddon agent.AgentAddon
}

func (s *hostedHookSyncer) sync(ctx context.Context,
	syncCtx factory.SyncContext,
	cluster *clusterv1.ManagedCluster,
	addon *addonapiv1beta1.ManagedClusterAddOn) (*addonapiv1beta1.ManagedClusterAddOn, error) {

	installMode, hostingClusterName := s.agentAddon.GetAgentAddonOptions().InstallMode(addon, cluster)
	// hosted mode is not enabled or the installMode is changed from hosted to default,
	// cleanup the resources on the hosting cluster
	if installMode != constants.InstallModeHosted {
		return s.releaseHostingWork(ctx, addon)
	}

	// Get Hosting Cluster, check whether the hosting cluster is a managed cluster of the hub
	// TODO: check whether the hosting cluster of the addon is the same hosting cluster of the klusterlet
	hostingCluster, err := s.getCluster(hostingClusterName)
	if errors.IsNotFound(err) {
		return s.releaseHostingWork(ctx, addon)
	}
	if err != nil {
		return addon, err
	}

	if !hostingCluster.DeletionTimestamp.IsZero() {
		return s.releaseHostingWork(ctx, addon)
	}
	hookWork, err := s.buildWorks(ctx, hostingClusterName, cluster, addon)
	if err != nil {
		return addon, err
	}

	if hookWork == nil {
		deleting := !addon.DeletionTimestamp.IsZero()
		if err = s.cleanupHookWork(ctx, addon, deleting && addonHasHostingFinalizer(addon)); err != nil {
			return addon, err
		}
		// the deploy works are still owned by the hosted syncer if the hook finalizer is set.
		if deleting && !addonHasFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer) {
			addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
		}
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
		return addon, nil
	}

	if addon.DeletionTimestamp.IsZero() {
		manifestFinalizerAdded := addonAddFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
		hookFinalizerAdded := addonAddFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
		if manifestFinalizerAdded || hookFinalizerAdded {
			return addon, nil
		}

		// the hook work is only applied when the addon is deleting, delete the works left by a
		// previous addon.
		return addon, s.cleanupHookWork(ctx, addon, false)
	}

	if !addonHasFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer) {
		return s.releaseHostingWork(ctx, addon)
	}

	currentWorks, err := s.getWorkByAddon(addon.Name, addon.Namespace)
	if err != nil {
		return addon, err
	}
	_, staleWorks := partitionWorksForTarget(addon, hostingClusterName, currentWorks)
	for _, staleWork := range staleWorks {
		if err = s.deleteWork(ctx, staleWork.Namespace, staleWork.Name); err != nil {
			return addon, err
		}
	}

	hookWork, err = s.applyWork(ctx, addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied, hookWork, addon)
	if err != nil {
		return addon, err
	}

	if hookWorkIsCompleted(hookWork) {
		// the hook work is deleted by the next sync, the hosting manifest finalizer is still protecting it.
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
		return addon, nil
	}

	setHookManifestNotCompletedCondition(addon, hookWork.Name)
	return addon, nil
}

// releaseHostingWork cleans up the hook manifestWork and releases the hosting finalizers, it is called
// when the addon is not expected to have any resource on a hosting cluster.
func (s *hostedHookSyncer) releaseHostingWork(ctx context.Context,
	addon *addonapiv1beta1.ManagedClusterAddOn) (*addonapiv1beta1.ManagedClusterAddOn, error) {
	if err := s.cleanupHookWork(ctx, addon, addonHasHostingFinalizer(addon)); err != nil {
		return addon, err
	}
	addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
	addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
	return addon, nil
}

// cleanupHookWork will delete the hosting pre-delete hook manifestWorks of the addon, confirmOnHub is
// set when a finalizer is about to be released.
func (s *hostedHookSyncer) cleanupHookWork(ctx context.Context,
	addon *addonapiv1beta1.ManagedClusterAddOn, confirmOnHub bool) error {
	return cleanupAddonWorks(ctx, s.getWorkByAddon, s.listWorkByAddon, s.deleteWork, addon, confirmOnHub)
}
