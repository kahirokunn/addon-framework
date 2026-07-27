package agentdeploy

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
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
	controllerName               = "addon-deploy-controller"
	managedClusterNotFoundReason = "ManagedClusterNotFound"
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
	hostedModeAddons           sets.Set[string]
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
		hostedModeAddons:           sets.New[string](),
		mcaFilterFunc:              mcaFilterFunc,
	}
	for addonName, agentAddon := range agentAddons {
		if agentAddon.GetAgentAddonOptions().HostedModeEnabled {
			c.hostedModeAddons.Insert(addonName)
		}
	}

	c.setClusterInformerHandler(clusterInformers)

	// the addons and works which are not registered in this manager are watched as well, since the
	// works in a hosting cluster ns have no ownerRef to be cleaned up by.
	f := factory.New().WithSyncContext(syncCtx).
		WithInformersQueueKeysFunc(
			func(obj runtime.Object) []string {
				key, _ := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
				return []string{key}
			},
			addonInformers.Informer()).
		WithFilteredEventsInformersQueueKeysFunc(
			func(obj runtime.Object) []string {
				// a deleted object is not filtered, so the work is checked here as well.
				work, ok := obj.(*workapiv1.ManifestWork)
				if !ok {
					return nil
				}
				ref, ok := index.ExtractAddonFromWork(work)
				if !ok {
					return nil
				}
				return []string{ref.Key()}
			},
			func(obj interface{}) bool {
				work, ok := obj.(*workapiv1.ManifestWork)
				if !ok {
					return false
				}

				// only watch the addon deploy/hook manifestWorks here.
				ref, ok := index.ExtractAddonFromWork(work)
				return ok && ref.IsAddonWork()
			},
			workInformers.Informer(),
		).
		WithBareInformers(clusterInformers.Informer()).
		WithSync(c.sync)

	return f.ToController(controllerName)
}

func (c *addonDeployController) setClusterInformerHandler(clusterInformers clusterinformers.ManagedClusterInformer) {
	var filters []func(old, new *clusterv1.ManagedCluster) bool
	for _, addon := range c.agentAddons {
		if addon.GetAgentAddonOptions().AgentDeployTriggerClusterFilter != nil {
			filters = append(filters, addon.GetAgentAddonOptions().AgentDeployTriggerClusterFilter)
		}
	}

	enqueue := c.enqueueAddOnsByCluster()
	_, err := clusterInformers.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: enqueue,
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldCluster, ook := oldObj.(*clusterv1.ManagedCluster)
				newCluster, nok := newObj.(*clusterv1.ManagedCluster)
				if !ook || !nok {
					return
				}

				if oldCluster.DeletionTimestamp.IsZero() != newCluster.DeletionTimestamp.IsZero() {
					enqueue(newObj)
					return
				}

				// enqueue the addon if one of cluster filters is matched.
				for _, filter := range filters {
					if filter(oldCluster, newCluster) {
						enqueue(newObj)
						break
					}
				}
			},
			DeleteFunc: enqueue,
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
		// the cluster may also be the hosting cluster of the addons in other cluster namespaces.
		var addons []interface{}
		if c.hostedModeAddons.Len() != 0 {
			addons = c.managedClusterAddonIndexer.List()
		} else {
			addons, err = c.managedClusterAddonIndexer.ByIndex(index.ManagedClusterAddonByNamespace, clusterName)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("failed to get addons by cluster %s , err: %v", clusterName, err))
				return
			}
		}

		var addonNames []string
		for _, obj := range addons {
			addon, ok := obj.(*addonapiv1beta1.ManagedClusterAddOn)
			if !ok {
				continue
			}
			if addon.Namespace != clusterName && !c.hostedModeAddons.Has(addon.Name) {
				continue
			}
			key, _ := cache.MetaNamespaceKeyFunc(addon)
			c.queue.Add(key)
			addonNames = append(addonNames, key)
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

// listWorksByAddonFn returns a func listing the works matched by the given func from the hub, the
// works of the addon are searched in all the namespaces since a work may be in a hosting cluster ns.
func (c *addonDeployController) listWorksByAddonFn(match func(index.AddonWorkRef) bool) listWorksByAddonFunc {
	return func(ctx context.Context, addonName, addonNamespace string) ([]*workapiv1.ManifestWork, error) {
		selector := labels.SelectorFromSet(labels.Set{
			addonapiv1beta1.AddonLabelKey: addonName,
		}).String()
		workList, err := c.workClient.WorkV1().ManifestWorks(metav1.NamespaceAll).
			List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return nil, err
		}

		addonKey := index.AddonWorkRef{Name: addonName, Namespace: addonNamespace}.Key()
		works := make([]*workapiv1.ManifestWork, 0, len(workList.Items))
		for i := range workList.Items {
			work := &workList.Items[i]
			ref, ok := index.ExtractAddonFromWork(work)
			if !ok || ref.Key() != addonKey || !match(ref) {
				continue
			}
			works = append(works, work)
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

	// the hub is checked on a cache miss, so that a stale informer is not treated as a deleted cluster.
	return c.clusterClient.ClusterV1().ManagedClusters().Get(ctx, clusterName, metav1.GetOptions{})
}

// cleanupWorksByAddonIdentity deletes all the manifestWorks applied for the addon identity, it is
// called when no resource is expected to be left for the addon on any cluster.
func (c *addonDeployController) cleanupWorksByAddonIdentity(
	ctx context.Context,
	addonName, addonNamespace string,
) error {
	listWorks := c.listWorksByAddonFn(index.AddonWorkRef.IsAddonWork)
	currentWorks, err := listWorks(ctx, addonName, addonNamespace)
	if err != nil {
		return err
	}
	if len(currentWorks) == 0 {
		return nil
	}

	return deleteWorksAndVerify(ctx, c.workApplier.Delete, listWorks, addonName, addonNamespace, currentWorks)
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
		Message:            fmt.Sprintf("source managed cluster %s is not found", addon.Namespace),
	})

	if !equality.Semantic.DeepEqual(addon.Status, oldAddon.Status) {
		return c.updateAddon(ctx, addon, oldAddon)
	}

	// the hook manifests cannot be rendered without the managed cluster, keep waiting for the cluster.
	if !addon.DeletionTimestamp.IsZero() && addonHasPreDeleteHookFinalizer(addon) {
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
		// confirm the cache miss on the hub before the works are deleted.
		_, liveErr := c.addonClient.AddonV1beta1().ManagedClusterAddOns(clusterName).
			Get(ctx, addonName, metav1.GetOptions{})
		if errors.IsNotFound(liveErr) {
			return c.cleanupWorksByAddonIdentity(ctx, addonName, clusterName)
		}
		return liveErr
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
		// confirm the cache miss on the hub before the cluster is treated as deleted, and keep waiting
		// for the informer otherwise, since the other caches may be stale as well.
		_, liveErr := c.clusterClient.ClusterV1().ManagedClusters().Get(ctx, clusterName, metav1.GetOptions{})
		if errors.IsNotFound(liveErr) {
			return c.syncMissingManagedCluster(ctx, addon)
		}
		return liveErr
	}
	if err != nil {
		return err
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
	getCluster := func(clusterName string) (*clusterv1.ManagedCluster, error) {
		return c.getManagedCluster(ctx, clusterName)
	}
	hostedDeploySyncer := &hostedSyncer{
		buildWorks: c.buildDeployManifestWorksFunc(
			newHostingAddonWorksBuilder(options.HostedModeEnabled, c.workBuilder),
			addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied,
		),
		applyWork:       c.applyWork,
		deleteWork:      c.workApplier.Delete,
		getCluster:      getCluster,
		getWorkByAddon:  c.getWorksByAddonFn(index.ManifestWorkByHostedAddon),
		listWorkByAddon: c.listWorksByAddonFn(index.AddonWorkRef.IsHostedDeployWork),
		agentAddon:      agentAddon,
	}

	// the syncer which deletes the works of the previous install mode runs first.
	syncers := make([]addonDeploySyncer, 2, 5)
	syncers[0], syncers[1] = hostedDeploySyncer, defaultDeploySyncer
	if installMode, _ := options.InstallMode(addon, cluster); installMode == constants.InstallModeHosted {
		syncers[0], syncers[1] = syncers[1], syncers[0]
	}

	syncers = append(syncers,
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
			applyWork:       c.applyWork,
			deleteWork:      c.workApplier.Delete,
			getCluster:      getCluster,
			getWorkByAddon:  c.getWorksByAddonFn(index.ManifestWorkHookByHostedAddon),
			listWorkByAddon: c.listWorksByAddonFn(index.AddonWorkRef.IsHostedHookWork),
			agentAddon:      agentAddon},
		&healthCheckSyncer{
			getWorkByAddon:       c.getWorksByAddonFn(index.ManifestWorkByAddon),
			getWorkByHostedAddon: c.getWorksByAddonFn(index.ManifestWorkByHostedAddon),
			agentAddon:           agentAddon,
		},
	)

	oldAddon := addon
	addon = addon.DeepCopy()

	// the managed cluster is back, the condition set by syncMissingManagedCluster is stale.
	if condition := meta.FindStatusCondition(addon.Status.Conditions,
		addonapiv1beta1.ManagedClusterAddOnConditionAvailable); condition != nil &&
		condition.Reason == managedClusterNotFoundReason {
		meta.RemoveStatusCondition(&addon.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnConditionAvailable)
	}

	var syncErr error
	for _, s := range syncers {
		addon, syncErr = s.sync(ctx, syncCtx, cluster, addon)
		if syncErr != nil {
			break
		}
		// persist a finalizer change before the next syncer runs.
		if !equality.Semantic.DeepEqual(addon.GetFinalizers(), oldAddon.GetFinalizers()) {
			break
		}
	}
	// a hook syncer only reports its own hook, the aggregated condition is set once all of them are done.
	if !addon.DeletionTimestamp.IsZero() && addonHasPreDeleteHookFinalizer(oldAddon) &&
		!addonHasPreDeleteHookFinalizer(addon) {
		meta.SetStatusCondition(&addon.Status.Conditions, metav1.Condition{
			Type:    addonapiv1beta1.ManagedClusterAddOnHookManifestCompleted,
			Status:  metav1.ConditionTrue,
			Reason:  "HookManifestIsCompleted",
			Message: "all the hook manifestWorks are completed.",
		})
	}

	if err = c.updateAddon(ctx, addon, oldAddon); err != nil {
		return fmt.Errorf("failed to update addon %s/%s: %w", addon.Namespace, addon.Name, err)
	}
	return syncErr
}

// updateAddon updates finalizers and conditions of addon.
// to avoid conflict updateAddon updates finalizers firstly if a finalizer is added, and updates the
// status firstly if a finalizer is removed, since an Update does not persist the status subresource.
func (c *addonDeployController) updateAddon(ctx context.Context, new, old *addonapiv1beta1.ManagedClusterAddOn) error {
	finalizersChanged := !equality.Semantic.DeepEqual(new.GetFinalizers(), old.GetFinalizers())
	statusChanged := !equality.Semantic.DeepEqual(new.Status, old.Status)
	finalizerAdded := !sets.New(old.GetFinalizers()...).HasAll(new.GetFinalizers()...)

	if finalizersChanged && (finalizerAdded || !statusChanged) {
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

		mode, _ := agentAddon.GetAgentAddonOptions().InstallMode(addon, cluster)

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

		mode, _ := agentAddon.GetAgentAddonOptions().InstallMode(addon, cluster)
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
