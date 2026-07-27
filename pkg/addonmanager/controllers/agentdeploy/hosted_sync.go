package agentdeploy

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	addonapiv1beta1 "open-cluster-management.io/api/addon/v1beta1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workapiv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/addon-framework/pkg/addonmanager/constants"
	"open-cluster-management.io/addon-framework/pkg/agent"
	"open-cluster-management.io/sdk-go/pkg/basecontroller/factory"
)

type hostedSyncer struct {
	buildWorks buildDeployWorkFunc

	applyWork func(ctx context.Context, appliedType string,
		work *workapiv1.ManifestWork, addon *addonapiv1beta1.ManagedClusterAddOn) (*workapiv1.ManifestWork, error)

	deleteWork func(ctx context.Context, workNamespace, workName string) error

	getWorkByAddon func(addonName, addonNamespace string) ([]*workapiv1.ManifestWork, error)

	listWorkByAddon listWorksByAddonFunc

	getCluster func(clusterName string) (*clusterv1.ManagedCluster, error)

	agentAddon agent.AgentAddon
}

func (s *hostedSyncer) sync(ctx context.Context,
	syncCtx factory.SyncContext,
	cluster *clusterv1.ManagedCluster,
	addon *addonapiv1beta1.ManagedClusterAddOn) (*addonapiv1beta1.ManagedClusterAddOn, error) {
	installMode, hostingClusterName := s.agentAddon.GetAgentAddonOptions().InstallMode(addon, cluster)
	if installMode != constants.InstallModeHosted {
		// hosted mode is not enabled or the installMode is changed from hosted to default,
		// cleanup the hosting resources
		return addon, s.cleanupDeployWork(ctx, addon)
	}

	// Get Hosting Cluster, check whether the hosting cluster is a managed cluster of the hub
	// TODO: check whether the hosting cluster of the addon is the same hosting cluster of the klusterlet
	hostingCluster, err := s.getCluster(hostingClusterName)
	if errors.IsNotFound(err) {
		if err = s.cleanupDeployWork(ctx, addon); err != nil {
			return addon, err
		}

		meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
			Type:    addonapiv1beta1.ManagedClusterAddOnHostingClusterValidity,
			Status:  metav1.ConditionFalse,
			Reason:  addonapiv1beta1.HostingClusterValidityReasonInvalid,
			Message: fmt.Sprintf("hosting cluster %s is not a managed cluster of the hub", hostingClusterName),
		})

		return addon, nil
	}
	if err != nil {
		return addon, err
	}
	meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
		Type:    addonapiv1beta1.ManagedClusterAddOnHostingClusterValidity,
		Status:  metav1.ConditionTrue,
		Reason:  addonapiv1beta1.HostingClusterValidityReasonValid,
		Message: fmt.Sprintf("hosting cluster %s is a managed cluster of the hub", hostingClusterName),
	})

	if !hostingCluster.DeletionTimestamp.IsZero() {
		return addon, s.cleanupDeployWork(ctx, addon)
	}

	// Don't skip syncing if the addon is deleting and there is a predelete hook, since the deployment manifests may
	// need to be updated during the uninstall.
	if !addonHasFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer) {
		if !addon.DeletionTimestamp.IsZero() {
			return addon, s.cleanupDeployWork(ctx, addon)
		}

		// waiting for the addon to be deleted when cluster is deleting.
		// TODO: consider to delete addon in this scenario.
		if !cluster.DeletionTimestamp.IsZero() {
			return addon, nil
		}
	}

	if addonAddFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer) {
		return addon, nil
	}

	currentWorks, err := s.getWorkByAddon(addon.Name, addon.Namespace)
	if err != nil {
		return addon, err
	}
	currentWorks, staleWorks := partitionWorksForTarget(addon, hostingClusterName, currentWorks)

	deployWorks, deleteWorks, err := s.buildWorks(ctx, hostingClusterName, cluster, currentWorks, addon)
	if err != nil {
		return addon, err
	}
	deleteWorks = append(staleWorks, deleteWorks...)

	// the works of the previous install mode are deleted before the new ones are applied, so that a
	// work is never applied in the new namespace while the old one still exists.
	for _, deleteWork := range deleteWorks {
		if err = s.deleteWork(ctx, deleteWork.Namespace, deleteWork.Name); err != nil {
			return addon, err
		}
	}

	var errs []error
	for _, deployWork := range deployWorks {
		_, err = s.applyWork(ctx, addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied, deployWork, addon)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return addon, utilerrors.NewAggregate(errs)
}

// cleanupDeployWork deletes the deploy manifestWorks of the addon on the hosting cluster, the hub is
// checked as well when the finalizer protecting them is still set.
func (s *hostedSyncer) cleanupDeployWork(ctx context.Context,
	addon *addonapiv1beta1.ManagedClusterAddOn) error {
	return cleanupAddonWorks(ctx, s.getWorkByAddon, s.listWorkByAddon, s.deleteWork, addon,
		addonHasFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer))
}
