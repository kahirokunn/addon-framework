package agentdeploy

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	errorsutil "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	addonapiv1beta1 "open-cluster-management.io/api/addon/v1beta1"
	addonclient "open-cluster-management.io/api/client/addon/clientset/versioned"
	addoninformerv1beta1 "open-cluster-management.io/api/client/addon/informers/externalversions/addon/v1beta1"
	addonlisterv1beta1 "open-cluster-management.io/api/client/addon/listers/addon/v1beta1"
	clusterclient "open-cluster-management.io/api/client/cluster/clientset/versioned"
	clusterinformers "open-cluster-management.io/api/client/cluster/informers/externalversions/cluster/v1"
	clusterlister "open-cluster-management.io/api/client/cluster/listers/cluster/v1"
	workv1client "open-cluster-management.io/api/client/work/clientset/versioned"
	workinformers "open-cluster-management.io/api/client/work/informers/externalversions/work/v1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workapiv1 "open-cluster-management.io/api/work/v1"
	workapplier "open-cluster-management.io/sdk-go/pkg/apis/work/v1/applier"
	workbuilder "open-cluster-management.io/sdk-go/pkg/apis/work/v1/builder"
	"open-cluster-management.io/sdk-go/pkg/patcher"

	"open-cluster-management.io/addon-framework/pkg/addonmanager/constants"
	"open-cluster-management.io/addon-framework/pkg/agent"
	"open-cluster-management.io/addon-framework/pkg/index"
	"open-cluster-management.io/addon-framework/pkg/utils"
	"open-cluster-management.io/sdk-go/pkg/basecontroller/factory"
)

const (
	controllerName                       = "addon-deploy-controller"
	managedClusterNotFoundReason         = "ManagedClusterNotFound"
	managedClusterNotFoundMessagePattern = "source managed cluster %s is not found"
)

// addonDeployController deploy addon agent resources on the managed cluster.
type addonDeployController struct {
	workClient                 workv1client.Interface
	workApplier                *workapplier.WorkApplier
	workBuilder                *workbuilder.WorkBuilder
	addonClient                addonclient.Interface
	clusterClient              clusterclient.Interface
	managedClusterLister       clusterlister.ManagedClusterLister
	managedClusterAddonLister  addonlisterv1beta1.ManagedClusterAddOnLister
	managedClusterAddonIndexer cache.Indexer
	workIndexer                cache.Indexer
	agentAddons                map[string]agent.AgentAddon
	queue                      workqueue.TypedRateLimitingInterface[string]
	mcaFilterFunc              utils.ManagedClusterAddOnFilterFunc
}

func NewAddonDeployController(
	workClient workv1client.Interface,
	addonClient addonclient.Interface,
	clusterClient clusterclient.Interface,
	clusterInformers clusterinformers.ManagedClusterInformer,
	addonInformers addoninformerv1beta1.ManagedClusterAddOnInformer,
	workInformers workinformers.ManifestWorkInformer,
	agentAddons map[string]agent.AgentAddon,
	mcaFilterFunc utils.ManagedClusterAddOnFilterFunc,
) factory.Controller {
	syncCtx := factory.NewSyncContext(controllerName)

	c := &addonDeployController{
		queue:       syncCtx.Queue(),
		workClient:  workClient,
		workApplier: workapplier.NewWorkApplierWithTypedClient(workClient, workInformers.Lister()),
		// the default manifest limit in a work is 500k
		// TODO: make the limit configurable
		workBuilder:                workbuilder.NewWorkBuilder().WithManifestsLimit(500 * 1024),
		addonClient:                addonClient,
		clusterClient:              clusterClient,
		managedClusterLister:       clusterInformers.Lister(),
		managedClusterAddonLister:  addonInformers.Lister(),
		managedClusterAddonIndexer: addonInformers.Informer().GetIndexer(),
		workIndexer:                workInformers.Informer().GetIndexer(),
		agentAddons:                agentAddons,
		mcaFilterFunc:              mcaFilterFunc,
	}

	c.setClusterInformerHandler(clusterInformers)

	f := factory.New().WithSyncContext(syncCtx).
		WithFilteredEventsInformersQueueKeysFunc(
			func(obj runtime.Object) []string {
				key, _ := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
				return []string{key}
			},
			func(_ interface{}) bool {
				// Deletion events must also enqueue addons whose active manager no longer registers
				// an AgentAddon, so explicitly owned cross-namespace Works can be reaped.
				return true
			},
			addonInformers.Informer()).
		WithFilteredEventsInformersQueueKeysFunc(
			func(obj runtime.Object) []string {
				accessor, _ := meta.Accessor(obj)
				// in hosted mode, need get the addon namespace from the AddonNamespaceLabel, because
				// the namespaces of manifestWork and addon may be different.
				// in default mode, the addon and manifestWork are in the cluster namespace.
				if addonNamespace, ok := accessor.GetLabels()[addonapiv1beta1.AddonNamespaceLabelKey]; ok {
					return []string{fmt.Sprintf("%s/%s", addonNamespace, accessor.GetLabels()[addonapiv1beta1.AddonLabelKey])}
				}
				return []string{fmt.Sprintf("%s/%s", accessor.GetNamespace(), accessor.GetLabels()[addonapiv1beta1.AddonLabelKey])}
			},
			func(obj interface{}) bool {
				accessor, _ := meta.Accessor(obj)
				if accessor.GetLabels() == nil {
					return false
				}

				// only watch the addon deploy/hook manifestWorks here.
				addonName, ok := accessor.GetLabels()[addonapiv1beta1.AddonLabelKey]
				if !ok {
					return false
				}

				managedByFramework :=
					accessor.GetLabels()[constants.AddonFrameworkManagedByLabelKey] ==
						constants.AddonFrameworkManagedByLabelValue
				if _, ok := c.agentAddons[addonName]; !ok && !managedByFramework {
					return false
				}

				if strings.HasPrefix(accessor.GetName(), constants.DeployWorkNamePrefix(addonName)) ||
					strings.HasPrefix(accessor.GetName(), constants.PreDeleteHookWorkName(addonName)) {
					return true
				}
				return false
			},
			workInformers.Informer(),
		).
		WithBareInformers(clusterInformers.Informer()).
		WithSync(c.sync)

	return f.ToController(controllerName)
}

func (c addonDeployController) setClusterInformerHandler(clusterInformers clusterinformers.ManagedClusterInformer) {
	var filters []func(old, new *clusterv1.ManagedCluster) bool
	for _, addon := range c.agentAddons {
		if addon.GetAgentAddonOptions().AgentDeployTriggerClusterFilter != nil {
			filters = append(filters, addon.GetAgentAddonOptions().AgentDeployTriggerClusterFilter)
		}
	}

	_, err := clusterInformers.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: c.enqueueAddOnsByCluster(),
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldCluster, ook := oldObj.(*clusterv1.ManagedCluster)
				newCluster, nok := newObj.(*clusterv1.ManagedCluster)
				if !ook || !nok {
					return
				}

				if oldCluster.DeletionTimestamp.IsZero() != newCluster.DeletionTimestamp.IsZero() {
					c.enqueueAddOnsByCluster()(newObj)
					return
				}

				// enqueue the addon if one of cluster filters is matched.
				for _, filter := range filters {
					if filter(oldCluster, newCluster) {
						c.enqueueAddOnsByCluster()(newObj)
						break
					}
				}
			},
			DeleteFunc: c.enqueueAddOnsByCluster(),
		},
	)
	if err != nil {
		utilruntime.HandleError(err)
	}
}

func (c *addonDeployController) enqueueAddOnsByCluster() func(obj interface{}) {
	return func(obj interface{}) {
		clusterName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to get managed cluster key: %w", err))
			return
		}
		addons, err := c.managedClusterAddonIndexer.ByIndex(index.ManagedClusterAddonByNamespace, clusterName)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to get addons by cluster %s , err: %v", clusterName, err))
			return
		}
		enqueued := map[string]struct{}{}
		var addonNames []string
		enqueue := func(addon interface{}) {
			if addon == nil {
				return
			}
			key, _ := cache.MetaNamespaceKeyFunc(addon)
			if key == "" {
				return
			}
			if _, ok := enqueued[key]; ok {
				return
			}
			c.queue.Add(key)
			enqueued[key] = struct{}{}
			addonNames = append(addonNames, key)
		}
		for _, addon := range addons {
			enqueue(addon)
		}

		// A hosting cluster event must also enqueue source addons in other namespaces.
		// Add/delete events are rare, so enqueue every hosted-capable addon and let its
		// normal reconciliation evaluate custom HostedModeInfoFunc implementations.
		for _, obj := range c.managedClusterAddonIndexer.List() {
			addon, ok := obj.(*addonapiv1beta1.ManagedClusterAddOn)
			if !ok {
				continue
			}
			agentAddon, ok := c.agentAddons[addon.Name]
			if !ok {
				continue
			}
			options := agentAddon.GetAgentAddonOptions()
			if !options.HostedModeEnabled || options.HostedModeInfoFunc == nil {
				continue
			}
			enqueue(addon)
		}
		klog.V(5).Infof("Enqueue addons by cluster %s, addons: %v", clusterName, addonNames)
	}
}

type addonDeploySyncer interface {
	sync(ctx context.Context, syncCtx factory.SyncContext,
		cluster *clusterv1.ManagedCluster,
		addon *addonapiv1beta1.ManagedClusterAddOn) (*addonapiv1beta1.ManagedClusterAddOn, error)
}

func (c *addonDeployController) getWorksByAddonFn(index string) func(addonName, addonNamespace string) ([]*workapiv1.ManifestWork, error) {
	return func(addonName, addonNamespace string) ([]*workapiv1.ManifestWork, error) {
		items, err := c.workIndexer.ByIndex(index, fmt.Sprintf("%s/%s", addonNamespace, addonName))
		if err != nil {
			return nil, err
		}
		ret := make([]*workapiv1.ManifestWork, 0, len(items))
		for _, item := range items {
			ret = append(ret, item.(*workapiv1.ManifestWork))
		}

		return ret, nil
	}
}

type listWorksByAddonFunc func(
	ctx context.Context, addonName, addonNamespace string,
) ([]*workapiv1.ManifestWork, error)

func (c *addonDeployController) listHostedWorksByAddonFunc(hook bool) listWorksByAddonFunc {
	return func(ctx context.Context, addonName, addonNamespace string) ([]*workapiv1.ManifestWork, error) {
		selector := labels.SelectorFromSet(labels.Set{
			addonapiv1beta1.AddonLabelKey: addonName,
		}).String()
		workList, err := c.workClient.WorkV1().ManifestWorks(metav1.NamespaceAll).
			List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return nil, err
		}

		works := make([]*workapiv1.ManifestWork, 0, len(workList.Items))
		for i := range workList.Items {
			work := &workList.Items[i]
			sourceNamespace := work.Labels[addonapiv1beta1.AddonNamespaceLabelKey]
			if sourceNamespace == "" {
				sourceNamespace = work.Namespace
			}
			if sourceNamespace != addonNamespace || sourceNamespace == work.Namespace {
				continue
			}
			isHook := strings.HasPrefix(work.Name, constants.PreDeleteHookWorkName(addonName))
			isDeploy := strings.HasPrefix(work.Name, constants.DeployWorkNamePrefix(addonName))
			if hook && isHook || !hook && isDeploy {
				works = append(works, work)
			}
		}
		return works, nil
	}
}

func (c *addonDeployController) getManagedCluster(
	ctx context.Context,
	clusterName string,
) (*clusterv1.ManagedCluster, error) {
	cluster, err := c.managedClusterLister.Get(clusterName)
	if !errors.IsNotFound(err) {
		return cluster, err
	}

	// A cache miss is not sufficient evidence that a hosting target disappeared.
	return c.clusterClient.ClusterV1().ManagedClusters().Get(ctx, clusterName, metav1.GetOptions{})
}

func (c *addonDeployController) listWorksByAddonIdentity(
	ctx context.Context,
	addonName, addonNamespace string,
) ([]*workapiv1.ManifestWork, error) {
	selector := labels.SelectorFromSet(labels.Set{
		addonapiv1beta1.AddonLabelKey: addonName,
	}).String()
	workList, err := c.workClient.WorkV1().ManifestWorks(metav1.NamespaceAll).
		List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}

	works := make([]*workapiv1.ManifestWork, 0, len(workList.Items))
	for i := range workList.Items {
		work := &workList.Items[i]
		sourceNamespace := work.Labels[addonapiv1beta1.AddonNamespaceLabelKey]
		if sourceNamespace == "" {
			sourceNamespace = work.Namespace
		}
		if sourceNamespace != addonNamespace {
			continue
		}
		if strings.HasPrefix(work.Name, constants.DeployWorkNamePrefix(addonName)) ||
			strings.HasPrefix(work.Name, constants.PreDeleteHookWorkName(addonName)) {
			works = append(works, work)
		}
	}
	return works, nil
}

func (c *addonDeployController) cleanupWorksByAddonIdentity(
	ctx context.Context,
	addonName, addonNamespace string,
) error {
	currentWorks, err := c.listWorksByAddonIdentity(ctx, addonName, addonNamespace)
	if err != nil {
		return err
	}
	var errs []error
	for _, work := range ownedWorks(currentWorks) {
		if err := c.workApplier.Delete(ctx, work.Namespace, work.Name); err != nil {
			errs = append(errs, err)
		}
	}
	if aggregate := errorsutil.NewAggregate(errs); aggregate != nil {
		return aggregate
	}

	liveWorks, err := c.listWorksByAddonIdentity(ctx, addonName, addonNamespace)
	if err != nil {
		return err
	}
	return verifyWorkDeletionStarted(liveWorks)
}

func (c *addonDeployController) syncMissingManagedCluster(
	ctx context.Context,
	addon *addonapiv1beta1.ManagedClusterAddOn,
) error {
	oldAddon := addon
	addon = addon.DeepCopy()
	meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
		Type:               addonapiv1beta1.ManagedClusterAddOnConditionAvailable,
		Status:             metav1.ConditionUnknown,
		ObservedGeneration: addon.Generation,
		Reason:             managedClusterNotFoundReason,
		Message:            fmt.Sprintf(managedClusterNotFoundMessagePattern, addon.Namespace),
	})

	// Record the blocked state before starting cleanup.
	if !equality.Semantic.DeepEqual(addon.Status, oldAddon.Status) {
		return c.updateAddon(ctx, addon, oldAddon)
	}

	if !addon.DeletionTimestamp.IsZero() &&
		(addonHasFinalizer(addon, addonapiv1beta1.AddonPreDeleteHookFinalizer) ||
			addonHasFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)) {
		// Without the source cluster, a protected hook cannot be rendered safely.
		return nil
	}

	if err := c.cleanupWorksByAddonIdentity(ctx, addon.Name, addon.Namespace); err != nil {
		return err
	}

	addonRemoveFinalizer(addon, addonapiv1beta1.AddonPreDeleteHookFinalizer)
	addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer)
	addonRemoveFinalizer(addon, addonapiv1beta1.AddonHostingManifestFinalizer)
	return c.updateAddon(ctx, addon, oldAddon)
}

func (c *addonDeployController) sync(ctx context.Context, syncCtx factory.SyncContext, key string) error {
	klog.V(4).Infof("%s sync addon key %s", controllerName, key)
	clusterName, addonName, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		// ignore addon whose key is not in format: namespace/name
		return nil
	}

	addon, err := c.managedClusterAddonLister.ManagedClusterAddOns(clusterName).Get(addonName)
	if errors.IsNotFound(err) {
		// Confirm cache misses against the API server before deleting cross-namespace children.
		_, liveErr := c.addonClient.AddonV1beta1().ManagedClusterAddOns(clusterName).
			Get(ctx, addonName, metav1.GetOptions{})
		switch {
		case liveErr == nil:
			return nil
		case !errors.IsNotFound(liveErr):
			return liveErr
		default:
			return c.cleanupWorksByAddonIdentity(ctx, addonName, clusterName)
		}
	}
	if err != nil {
		return err
	}

	agentAddon, ok := c.agentAddons[addonName]
	if !ok {
		return nil
	}

	if c.mcaFilterFunc != nil && !c.mcaFilterFunc(addon) {
		return nil
	}

	// to deploy agents if there is RegistrationApplied condition.
	if meta.FindStatusCondition(addon.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnRegistrationApplied) == nil {
		return nil
	}

	cluster, err := c.managedClusterLister.Get(clusterName)
	if errors.IsNotFound(err) {
		// Confirm the informer miss before treating the source cluster as unavailable.
		_, liveErr := c.clusterClient.ClusterV1().ManagedClusters().
			Get(ctx, clusterName, metav1.GetOptions{})
		switch {
		case liveErr == nil:
			return nil
		case !errors.IsNotFound(liveErr):
			return liveErr
		default:
			return c.syncMissingManagedCluster(ctx, addon)
		}
	}
	if err != nil {
		return err
	}

	if condition := meta.FindStatusCondition(
		addon.Status.Conditions,
		addonapiv1beta1.ManagedClusterAddOnConditionAvailable,
	); condition != nil && condition.Reason == managedClusterNotFoundReason {
		oldAddon := addon
		addon = addon.DeepCopy()
		meta.RemoveStatusCondition(
			&addon.Status.Conditions,
			addonapiv1beta1.ManagedClusterAddOnConditionAvailable,
		)
		return c.updateAddon(ctx, addon, oldAddon)
	}

	options := agentAddon.GetAgentAddonOptions()
	defaultDeploySyncer := &defaultSyncer{
		buildWorks: c.buildDeployManifestWorksFunc(
			newAddonWorksBuilder(options.HostedModeEnabled, c.workBuilder),
			addonapiv1beta1.ManagedClusterAddOnManifestApplied,
		),
		applyWork:      c.applyWork,
		getWorkByAddon: c.getWorksByAddonFn(index.ManifestWorkByAddon),
		deleteWork:     c.workApplier.Delete,
		agentAddon:     agentAddon,
	}
	hostedDeploySyncer := &hostedSyncer{
		buildWorks: c.buildDeployManifestWorksFunc(
			newHostingAddonWorksBuilder(options.HostedModeEnabled, c.workBuilder),
			addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied,
		),
		applyWork:  c.applyWork,
		deleteWork: c.workApplier.Delete,
		getCluster: func(clusterName string) (*clusterv1.ManagedCluster, error) {
			return c.getManagedCluster(ctx, clusterName)
		},
		getWorkByAddon:  c.getWorksByAddonFn(index.ManifestWorkByHostedAddon),
		listWorkByAddon: c.listHostedWorksByAddonFunc(false),
		agentAddon:      agentAddon,
	}

	deploySyncers := []addonDeploySyncer{hostedDeploySyncer, defaultDeploySyncer}
	if options.HostedModeEnabled && options.HostedModeInfoFunc != nil {
		installMode, _ := options.HostedModeInfoFunc(addon, cluster)
		if installMode == constants.InstallModeHosted {
			deploySyncers = []addonDeploySyncer{defaultDeploySyncer, hostedDeploySyncer}
		}
	}

	syncers := append(deploySyncers,
		&defaultHookSyncer{
			buildWorks: c.buildHookManifestWorkFunc(
				newAddonWorksBuilder(options.HostedModeEnabled, c.workBuilder),
				addonapiv1beta1.ManagedClusterAddOnManifestApplied,
			),
			applyWork:  c.applyWork,
			agentAddon: agentAddon},
		&hostedHookSyncer{
			buildWorks: c.buildHookManifestWorkFunc(
				newHostingAddonWorksBuilder(options.HostedModeEnabled, c.workBuilder),
				addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied,
			),
			applyWork:  c.applyWork,
			deleteWork: c.workApplier.Delete,
			getCluster: func(clusterName string) (*clusterv1.ManagedCluster, error) {
				return c.getManagedCluster(ctx, clusterName)
			},
			getWorkByAddon:  c.getWorksByAddonFn(index.ManifestWorkHookByHostedAddon),
			listWorkByAddon: c.listHostedWorksByAddonFunc(true),
			agentAddon:      agentAddon},
		&healthCheckSyncer{
			getWorkByAddon:       c.getWorksByAddonFn(index.ManifestWorkByAddon),
			getWorkByHostedAddon: c.getWorksByAddonFn(index.ManifestWorkByHostedAddon),
			agentAddon:           agentAddon,
		},
	)

	oldAddon := addon
	addon = addon.DeepCopy()
	var errs []error
	for _, s := range syncers {
		var err error
		addon, err = s.sync(ctx, syncCtx, cluster, addon)
		if err != nil {
			errs = append(errs, err)
			break
		}
		// Persist every finalizer transition as its own reconciliation checkpoint. In particular,
		// no cross-namespace Work may be created in the same pass that first adds its cleanup finalizer.
		if !equality.Semantic.DeepEqual(addon.GetFinalizers(), oldAddon.GetFinalizers()) {
			break
		}
	}
	normalizePreDeleteHookCondition(addon)

	if err = c.updateAddon(ctx, addon, oldAddon); err != nil {
		return fmt.Errorf("failed to update addon %s/%s: %w", addon.Namespace, addon.Name, err)
	}
	return errorsutil.NewAggregate(errs)
}

// updateAddon persists one API boundary at a time. A finalizer must be added before
// creating cross-namespace resources. Conversely, status produced by a completed
// cleanup phase must be persisted before removing its finalizer because a regular
// Update does not persist the status subresource.
func (c *addonDeployController) updateAddon(ctx context.Context, new, old *addonapiv1beta1.ManagedClusterAddOn) error {
	finalizersChanged := !equality.Semantic.DeepEqual(new.GetFinalizers(), old.GetFinalizers())
	statusChanged := !equality.Semantic.DeepEqual(new.Status, old.Status)

	if finalizersChanged && (finalizerAdded(new.GetFinalizers(), old.GetFinalizers()) || !statusChanged) {
		_, err := c.addonClient.AddonV1beta1().ManagedClusterAddOns(new.Namespace).Update(ctx, new, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update addon finalizers: %w", err)
		}
		return nil
	}

	addonPatcher := patcher.NewPatcher[
		*addonapiv1beta1.ManagedClusterAddOn,
		addonapiv1beta1.ManagedClusterAddOnSpec,
		addonapiv1beta1.ManagedClusterAddOnStatus](c.addonClient.AddonV1beta1().ManagedClusterAddOns(new.Namespace))

	_, err := addonPatcher.PatchStatus(ctx, new, new.Status, old.Status)
	if err != nil {
		return fmt.Errorf("failed to update addon status: %w", err)
	}
	return nil
}

func finalizerAdded(new, old []string) bool {
	for _, newFinalizer := range new {
		found := false
		for _, oldFinalizer := range old {
			if newFinalizer == oldFinalizer {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}
	return false
}

func (c *addonDeployController) applyWork(ctx context.Context, appliedType string,
	work *workapiv1.ManifestWork, addon *addonapiv1beta1.ManagedClusterAddOn) (*workapiv1.ManifestWork, error) {

	work, err := c.workApplier.Apply(ctx, work)
	if err != nil {
		meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
			Type:    appliedType,
			Status:  metav1.ConditionFalse,
			Reason:  addonapiv1beta1.AddonManifestAppliedReasonWorkApplyFailed,
			Message: fmt.Sprintf("failed to apply manifestWork: %v", err),
		})
		return work, err
	}

	// Update addon status based on work's status
	WorkAppliedCond := meta.FindStatusCondition(work.Status.Conditions, workapiv1.WorkApplied)
	switch {
	case WorkAppliedCond == nil:
		// ManifestWork has been created but work-agent hasn't reported status yet
		// Clear any stale False condition to avoid showing outdated errors
		existingCond := meta.FindStatusCondition(addon.Status.Conditions, appliedType)
		if existingCond != nil && existingCond.Status == metav1.ConditionFalse {
			// Remove the outdated False condition, wait for work-agent to report
			meta.RemoveStatusCondition(&addon.Status.Conditions, appliedType)
		}
		// If it was already True or doesn't exist, leave it unchanged
		return work, nil
	case WorkAppliedCond.Status == metav1.ConditionTrue:
		meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
			Type:    appliedType,
			Status:  metav1.ConditionTrue,
			Reason:  addonapiv1beta1.AddonManifestAppliedReasonManifestsApplied,
			Message: "manifests of addon are applied successfully",
		})
	default:
		meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
			Type:    appliedType,
			Status:  metav1.ConditionFalse,
			Reason:  addonapiv1beta1.AddonManifestAppliedReasonManifestsApplyFailed,
			Message: "failed to apply the manifests of addon",
		})
	}

	return work, nil
}

type buildDeployWorkFunc func(
	ctx context.Context,
	workNamespace string,
	cluster *clusterv1.ManagedCluster, existingWorks []*workapiv1.ManifestWork,
	addon *addonapiv1beta1.ManagedClusterAddOn) (appliedWorks, deleteWorks []*workapiv1.ManifestWork, err error)

func (c *addonDeployController) buildDeployManifestWorksFunc(addonWorkBuilder *addonWorksBuilder, appliedType string) buildDeployWorkFunc {
	return func(
		ctx context.Context,
		workNamespace string,
		cluster *clusterv1.ManagedCluster, existingWorks []*workapiv1.ManifestWork,
		addon *addonapiv1beta1.ManagedClusterAddOn) (appliedWorks, deleteWorks []*workapiv1.ManifestWork, err error) {
		agentAddon := c.agentAddons[addon.Name]
		if agentAddon == nil {
			return nil, nil, fmt.Errorf("failed to get agentAddon")
		}

		if agentAddon.GetAgentAddonOptions().ConfigCheckEnabled &&
			!meta.IsStatusConditionTrue(addon.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnConditionConfigured) {
			klog.InfoS("Addon configured condition is not set in status", "addonName", addon.Name)
			return nil, nil, nil
		}

		objects, err := agentAddon.Manifests(ctx, cluster, addon)
		if err != nil {
			meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
				Type:    appliedType,
				Status:  metav1.ConditionFalse,
				Reason:  addonapiv1beta1.AddonManifestAppliedReasonWorkApplyFailed,
				Message: fmt.Sprintf("failed to get manifest from agent interface: %v", err),
			})
			return nil, nil, err
		}

		// this is to retrieve the intended mode of the addon.
		var mode string
		if agentAddon.GetAgentAddonOptions().HostedModeInfoFunc == nil {
			mode = constants.InstallModeDefault
		} else {
			mode, _ = agentAddon.GetAgentAddonOptions().HostedModeInfoFunc(addon, cluster)
		}

		manifestOptions, err := getManifestConfigOption(ctx, agentAddon, cluster, addon)
		if err != nil {
			return nil, nil, fmt.Errorf("get manifest config option error: %v", err)
		}

		existingWorksCopy := []workapiv1.ManifestWork{}
		for _, work := range existingWorks {
			existingWorksCopy = append(existingWorksCopy, *work)
		}
		appliedWorks, deleteWorks, err = addonWorkBuilder.BuildDeployWorks(
			mode, workNamespace, addon, existingWorksCopy, objects, manifestOptions)
		if err != nil {
			meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
				Type:    appliedType,
				Status:  metav1.ConditionFalse,
				Reason:  addonapiv1beta1.AddonManifestAppliedReasonWorkApplyFailed,
				Message: fmt.Sprintf("failed to build manifestwork: %v", err),
			})
			return nil, nil, err
		}
		if len(appliedWorks) == 0 {
			meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
				Type:    appliedType,
				Status:  metav1.ConditionTrue,
				Reason:  addonapiv1beta1.AddonManifestAppliedReasonManifestsApplied,
				Message: "no manifest need to apply",
			})
		}
		return appliedWorks, deleteWorks, nil
	}
}

type buildDeployHookFunc func(
	ctx context.Context,
	workNamespace string,
	cluster *clusterv1.ManagedCluster,
	addon *addonapiv1beta1.ManagedClusterAddOn) (*workapiv1.ManifestWork, error)

func (c *addonDeployController) buildHookManifestWorkFunc(addonWorkBuilder *addonWorksBuilder, appliedType string) buildDeployHookFunc {
	return func(
		ctx context.Context,
		workNamespace string,
		cluster *clusterv1.ManagedCluster,
		addon *addonapiv1beta1.ManagedClusterAddOn) (*workapiv1.ManifestWork, error) {
		agentAddon := c.agentAddons[addon.Name]
		if agentAddon == nil {
			return nil, fmt.Errorf("failed to get agentAddon")
		}

		if agentAddon.GetAgentAddonOptions().ConfigCheckEnabled &&
			!meta.IsStatusConditionTrue(addon.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnConditionConfigured) {
			klog.InfoS("Addon configured condition is not set in status", "addonName", addon.Name)
			return nil, nil
		}

		objects, err := agentAddon.Manifests(ctx, cluster, addon)
		if err != nil {
			meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
				Type:    appliedType,
				Status:  metav1.ConditionFalse,
				Reason:  addonapiv1beta1.AddonManifestAppliedReasonWorkApplyFailed,
				Message: fmt.Sprintf("failed to get manifest from agent interface: %v", err),
			})
			return nil, err
		}
		if len(objects) == 0 {
			return nil, nil
		}

		// this is to retrieve the intended mode of the addon.
		var mode string
		if agentAddon.GetAgentAddonOptions().HostedModeInfoFunc == nil {
			mode = constants.InstallModeDefault
		} else {
			mode, _ = agentAddon.GetAgentAddonOptions().HostedModeInfoFunc(addon, cluster)
		}
		hookWork, err := addonWorkBuilder.BuildHookWork(mode, workNamespace, addon, objects)
		if err != nil {
			meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
				Type:    appliedType,
				Status:  metav1.ConditionFalse,
				Reason:  addonapiv1beta1.AddonManifestAppliedReasonWorkApplyFailed,
				Message: fmt.Sprintf("failed to build manifestwork: %v", err),
			})
			return nil, err
		}
		return hookWork, nil
	}
}
