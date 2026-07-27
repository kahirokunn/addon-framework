package agentdeploy

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	// A missing finalizer on a deleting addon means hook execution has already advanced or was never protected.
	// Never recreate a hook in this phase.
	if !addonHasFinalizer(addon, addonapiv1beta1.AddonPreDeleteHookFinalizer) {
		return addon, nil
	}

	// will deploy the pre-delete hook manifestWork when the addon is deleting
	hookWork, err = s.applyWork(ctx, addonapiv1beta1.ManagedClusterAddOnManifestApplied, hookWork, addon)
	if err != nil {
		return addon, err
	}

	if hookWorkIsCompleted(hookWork) {
		setPreDeleteHookCondition(
			addon,
			metav1.ConditionTrue,
			addonapiv1beta1.PreDeleteHookReasonCompleted,
			fmt.Sprintf("pre-delete hook ManifestWork %s/%s is completed", hookWork.Namespace, hookWork.Name),
		)

		addonRemoveFinalizer(addon, addonapiv1beta1.AddonPreDeleteHookFinalizer)
		return addon, nil
	}

	setPreDeleteHookCondition(
		addon,
		metav1.ConditionFalse,
		addonapiv1beta1.PreDeleteHookReasonPending,
		fmt.Sprintf("pre-delete hook ManifestWork %s/%s is not completed", hookWork.Namespace, hookWork.Name),
	)

	return addon, nil
}
