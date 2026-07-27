package agentdeploy

import (
	"context"

	addonapiv1beta1 "open-cluster-management.io/api/addon/v1beta1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workapiv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/addon-framework/pkg/agent"
	"open-cluster-management.io/sdk-go/pkg/basecontroller/factory"
)

type defaultHookSyncer struct {
	buildWorks buildDeployHookFunc
	applyWork  func(ctx context.Context, appliedType string,
		work *workapiv1.ManifestWork, addon *addonapiv1beta1.ManagedClusterAddOn) (*workapiv1.ManifestWork, error)
	agentAddon agent.AgentAddon
}

func (s *defaultHookSyncer) sync(ctx context.Context,
	syncCtx factory.SyncContext,
	cluster *clusterv1.ManagedCluster,
	addon *addonapiv1beta1.ManagedClusterAddOn) (*addonapiv1beta1.ManagedClusterAddOn, error) {
	deployWorkNamespace := addon.Namespace

	hookWork, err := s.buildWorks(ctx, deployWorkNamespace, cluster, addon)
	if err != nil {
		return addon, err
	}

	if hookWork == nil {
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonPreDeleteHookFinalizer)
		return addon, nil
	}

	if addon.DeletionTimestamp.IsZero() {
		addonAddFinalizer(addon, addonapiv1beta1.AddonPreDeleteHookFinalizer)
		return addon, nil
	}

	// the hook has already been completed if the finalizer is gone, do not apply the hook work again.
	if !addonHasFinalizer(addon, addonapiv1beta1.AddonPreDeleteHookFinalizer) {
		return addon, nil
	}

	// will deploy the pre-delete hook manifestWork when the addon is deleting
	hookWork, err = s.applyWork(ctx, addonapiv1beta1.ManagedClusterAddOnManifestApplied, hookWork, addon)
	if err != nil {
		return addon, err
	}

	// TODO: will surface more message here
	if hookWorkIsCompleted(hookWork) {
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonPreDeleteHookFinalizer)
		return addon, nil
	}

	setHookManifestNotCompletedCondition(addon, hookWork.Name)
	return addon, nil
}
