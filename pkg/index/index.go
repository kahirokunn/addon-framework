package index

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"

	addonv1beta1 "open-cluster-management.io/api/addon/v1beta1"
	workapiv1 "open-cluster-management.io/api/work/v1"

	"open-cluster-management.io/addon-framework/pkg/addonmanager/constants"
)

const (
	ManagedClusterAddonByNamespace = "managedClusterAddonByNamespace"
)

//nolint:revive
func IndexManagedClusterAddonByNamespace(obj interface{}) ([]string, error) {
	mca, ok := obj.(*addonv1beta1.ManagedClusterAddOn)

	if !ok {
		return []string{}, fmt.Errorf("obj %T is not a ManagedClusterAddon", obj)
	}

	return []string{mca.Namespace}, nil
}

const (
	ManifestWorkByAddon           = "manifestWorkByAddon"
	ManifestWorkByHostedAddon     = "manifestWorkByHostedAddon"
	ManifestWorkHookByHostedAddon = "manifestWorkHookByHostedAddon"
)

//nolint:revive
func IndexManifestWorkByAddon(obj interface{}) ([]string, error) {
	work, ok := obj.(*workapiv1.ManifestWork)
	if !ok {
		return []string{}, fmt.Errorf("obj is supposed to be a ManifestWork, but is %T", obj)
	}

	ref, ok := ExtractAddonFromWork(work)
	if !ok || !ref.IsDefaultDeployWork() {
		return []string{}, nil
	}

	return []string{ref.Key()}, nil
}

//nolint:revive
func IndexManifestWorkByHostedAddon(obj interface{}) ([]string, error) {
	work, ok := obj.(*workapiv1.ManifestWork)
	if !ok {
		return []string{}, fmt.Errorf("obj is supposed to be a ManifestWork, but is %T", obj)
	}

	ref, ok := ExtractAddonFromWork(work)
	if !ok || !ref.IsHostedDeployWork() {
		return []string{}, nil
	}

	return []string{ref.Key()}, nil
}

//nolint:revive
func IndexManifestWorkHookByHostedAddon(obj interface{}) ([]string, error) {
	work, ok := obj.(*workapiv1.ManifestWork)
	if !ok {
		return []string{}, fmt.Errorf("obj is supposed to be a ManifestWork, but is %T", obj)
	}

	ref, ok := ExtractAddonFromWork(work)
	if !ok || !ref.IsHostedHookWork() {
		return []string{}, nil
	}

	return []string{ref.Key()}, nil
}

// AddonWorkRef is the addon a manifestWork is applied for.
type AddonWorkRef struct {
	Name      string
	Namespace string
	Hosted    bool
	Deploy    bool
	Hook      bool
}

// Key returns the index key of the addon, in format: addonNamespace/addonName.
func (r AddonWorkRef) Key() string {
	return fmt.Sprintf("%s/%s", r.Namespace, r.Name)
}

// IsAddonWork returns true if the manifestWork is applied by the addon framework.
func (r AddonWorkRef) IsAddonWork() bool {
	return r.Deploy || r.Hook
}

// IsDefaultDeployWork returns true if the manifestWork is a deploy work in the addon namespace.
func (r AddonWorkRef) IsDefaultDeployWork() bool {
	return r.Deploy && !r.Hosted
}

// IsHostedDeployWork returns true if the manifestWork is a deploy work in a hosting cluster namespace.
func (r AddonWorkRef) IsHostedDeployWork() bool {
	return r.Deploy && r.Hosted
}

// IsHostedHookWork returns true if the manifestWork is a pre-delete hook work in a hosting cluster namespace.
func (r AddonWorkRef) IsHostedHookWork() bool {
	return r.Hook && r.Hosted
}

// ExtractAddonFromWork returns the addon a manifestWork is applied for, and whether the work is
// applied by the addon framework.
func ExtractAddonFromWork(work *workapiv1.ManifestWork) (AddonWorkRef, bool) {
	addonName, ok := work.Labels[addonv1beta1.AddonLabelKey]
	if !ok {
		return AddonWorkRef{}, false
	}

	// in hosted mode, the addon namespace is recorded in the AddonNamespaceLabel, because the
	// namespaces of manifestWork and addon may be different.
	// in default mode, the addon and manifestWork are in the cluster namespace.
	addonNamespace := work.Labels[addonv1beta1.AddonNamespaceLabelKey]
	if addonNamespace == "" {
		addonNamespace = work.Namespace
	}

	return AddonWorkRef{
		Name:      addonName,
		Namespace: addonNamespace,
		Hosted:    addonNamespace != work.Namespace,
		Deploy:    strings.HasPrefix(work.Name, constants.DeployWorkNamePrefix(addonName)),
		Hook:      strings.HasPrefix(work.Name, constants.PreDeleteHookWorkName(addonName)),
	}, true
}

const (
	AddonByConfig = "addonByConfig"
)

//nolint:revive
func IndexAddonByConfig(obj interface{}) ([]string, error) {
	addon, ok := obj.(*addonv1beta1.ManagedClusterAddOn)
	if !ok {
		return nil, fmt.Errorf("obj is supposed to be a ManagedClusterAddOn, but is %T", obj)
	}

	getIndex := func(config addonv1beta1.ConfigSpecHash, gr addonv1beta1.ConfigGroupResource) string {
		if config.Namespace != "" {
			return fmt.Sprintf("%s/%s/%s/%s", gr.Group, gr.Resource, config.Namespace, config.Name)
		}

		return fmt.Sprintf("%s/%s/%s", gr.Group, gr.Resource, config.Name)
	}

	configNames := []string{}
	for _, configReference := range addon.Status.ConfigReferences {
		if configReference.DesiredConfig == nil || configReference.DesiredConfig.Name == "" {
			// bad config reference, ignore
			continue
		}

		configNames = append(configNames, getIndex(*configReference.DesiredConfig, configReference.ConfigGroupResource))
	}

	return configNames, nil
}

const (
	ClusterManagementAddonByConfig = "clusterManagementAddonByConfig"
)

//nolint:revive
func IndexClusterManagementAddonByConfig(obj interface{}) ([]string, error) {
	cma, ok := obj.(*addonv1beta1.ClusterManagementAddOn)
	if !ok {
		return nil, fmt.Errorf("obj is supposed to be a ClusterManagementAddOn, but is %T", obj)
	}

	getIndex := func(gr addonv1beta1.ConfigGroupResource, configSpecHash addonv1beta1.ConfigSpecHash) string {
		if configSpecHash.Namespace != "" {
			return fmt.Sprintf("%s/%s/%s/%s", gr.Group, gr.Resource, configSpecHash.Namespace, configSpecHash.Name)
		}

		return fmt.Sprintf("%s/%s/%s", gr.Group, gr.Resource, configSpecHash.Name)
	}

	configNames := sets.New[string]()
	for _, defaultConfigRef := range cma.Status.DefaultConfigReferences {
		if defaultConfigRef.DesiredConfig == nil || defaultConfigRef.DesiredConfig.Name == "" {
			// bad config reference, ignore
			continue
		}

		configNames.Insert(getIndex(defaultConfigRef.ConfigGroupResource, *defaultConfigRef.DesiredConfig))
	}

	for _, installProgression := range cma.Status.InstallProgressions {
		for _, configReference := range installProgression.ConfigReferences {
			if configReference.DesiredConfig == nil || configReference.DesiredConfig.Name == "" {
				// bad config reference, ignore
				continue
			}

			configNames.Insert(getIndex(configReference.ConfigGroupResource, *configReference.DesiredConfig))
		}
	}

	return configNames.UnsortedList(), nil
}
