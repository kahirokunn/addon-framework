package agentdeploy

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

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

	options := s.agentAddon.GetAgentAddonOptions()
	if !options.HostedModeEnabled || options.HostedModeInfoFunc == nil {
		if err := s.cleanupHookWork(ctx, addon); err != nil {
			return addon, err
		}
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
		return addon, nil
	}
	installMode, hostingClusterName := options.HostedModeInfoFunc(addon, cluster)
	if installMode != constants.InstallModeHosted {
		if err := s.cleanupHookWork(ctx, addon); err != nil {
			return addon, err
		}
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
		return addon, nil
	}

	// Get Hosting Cluster, check whether the hosting cluster is a managed cluster of the hub
	// TODO: check whether the hosting cluster of the addon is the same hosting cluster of the klusterlet
	hostingCluster, err := s.getCluster(hostingClusterName)
	if errors.IsNotFound(err) {
		if err = s.cleanupHookWork(ctx, addon); err != nil {
			return addon, err
		}
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
		return addon, nil
	}
	if err != nil {
		return addon, err
	}

	if !hostingCluster.DeletionTimestamp.IsZero() {
		if err = s.cleanupHookWork(ctx, addon); err != nil {
			return addon, err
		}
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
		return addon, nil
	}
	hookWork, err := s.buildWorks(ctx, hostingClusterName, cluster, addon)
	if err != nil {
		return addon, err
	}

	if hookWork == nil {
		hadHookFinalizer := addonHasFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
		if addon.DeletionTimestamp.IsZero() {
			if err = s.cleanupUnexpectedHookWork(ctx, addon); err != nil {
				return addon, err
			}
		} else {
			if err = s.cleanupHookWork(ctx, addon); err != nil {
				return addon, err
			}
		}
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
		if !addon.DeletionTimestamp.IsZero() && !hadHookFinalizer {
			addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
		}
		return addon, nil
	}

	if addon.DeletionTimestamp.IsZero() {
		finalizerChanged := addonAddFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
		finalizerChanged = addonAddFinalizer(
			addon,
			addonapiv1beta1.AddonHostingPreDeleteHookFinalizer,
		) || finalizerChanged
		if finalizerChanged {
			return addon, nil
		}

		// A hook Work is only desired while the addon is deleting. Remove leftovers from
		// a previous ManagedClusterAddOn instance after its cleanup finalizers are durable.
		if err = s.cleanupUnexpectedHookWork(ctx, addon); err != nil {
			return addon, err
		}
		return addon, nil
	}

	if !addonHasFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer) {
		if err = s.cleanupHookWork(ctx, addon); err != nil {
			return addon, err
		}
		if !addon.DeletionTimestamp.IsZero() {
			addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
		}
		return addon, nil
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
		setPreDeleteHookCondition(
			addon,
			metav1.ConditionTrue,
			addonapiv1beta1.PreDeleteHookReasonCompleted,
			fmt.Sprintf("pre-delete hook ManifestWork %s/%s is completed", hookWork.Namespace, hookWork.Name),
		)
		// The completed Work remains as durable evidence until this finalizer removal is persisted.
		// The hosting cleanup finalizer protects the subsequent Work deletion phase.
		addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
	} else {
		setPreDeleteHookCondition(
			addon,
			metav1.ConditionFalse,
			addonapiv1beta1.PreDeleteHookReasonPending,
			fmt.Sprintf("pre-delete hook ManifestWork %s/%s is not completed", hookWork.Namespace, hookWork.Name),
		)
	}

	return addon, nil
}

// cleanupHookWork will delete the hosting pre-delete hook manifestWork and remove the finalizer,
// if the hostingClusterName is empty, will try to find out the hosting cluster by manifestWork labels and do the cleanup
func (s *hostedHookSyncer) cleanupHookWork(ctx context.Context,
	addon *addonapiv1beta1.ManagedClusterAddOn) (err error) {
	currentWorks, err := s.getWorkByAddon(addon.Name, addon.Namespace)
	if err != nil {
		return err
	}
	currentWorks = ownedWorks(currentWorks)
	if len(currentWorks) == 0 &&
		!addonHasFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer) &&
		!addonHasFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer) {
		return nil
	}

	return s.deleteHookWorks(ctx, addon, currentWorks)
}

func (s *hostedHookSyncer) cleanupUnexpectedHookWork(
	ctx context.Context,
	addon *addonapiv1beta1.ManagedClusterAddOn,
) error {
	currentWorks, err := s.getWorkByAddon(addon.Name, addon.Namespace)
	if err != nil {
		return err
	}
	currentWorks = ownedWorks(currentWorks)
	if len(currentWorks) == 0 {
		return nil
	}

	return s.deleteHookWorks(ctx, addon, currentWorks)
}

func (s *hostedHookSyncer) deleteHookWorks(
	ctx context.Context,
	addon *addonapiv1beta1.ManagedClusterAddOn,
	currentWorks []*workapiv1.ManifestWork,
) error {
	var errs []error
	for _, work := range currentWorks {
		err := s.deleteWork(ctx, work.Namespace, work.Name)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if aggregate := utilerrors.NewAggregate(errs); aggregate != nil {
		return aggregate
	}

	liveWorks, err := s.listWorkByAddon(ctx, addon.Name, addon.Namespace)
	if err != nil {
		return err
	}
	return verifyWorkDeletionStarted(liveWorks)
}
