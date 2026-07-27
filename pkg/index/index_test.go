package index

import (
	"fmt"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	workapiv1 "open-cluster-management.io/api/work/v1"

	addonv1beta1 "open-cluster-management.io/api/addon/v1beta1"

	"open-cluster-management.io/addon-framework/pkg/addonmanager/constants"
)

func TestManifestWorkIndexesUseSourceIdentity(t *testing.T) {
	cases := []struct {
		name          string
		work          *workapiv1.ManifestWork
		indexFunc     func(interface{}) ([]string, error)
		expectedIndex []string
	}{
		{
			name: "default work without source namespace label",
			work: newIndexedWork(
				"cluster1", fmt.Sprintf("%s-%d", constants.DeployWorkNamePrefix("test"), 0), "",
			),
			indexFunc:     IndexManifestWorkByAddon,
			expectedIndex: []string{"cluster1/test"},
		},
		{
			name: "hosted work",
			work: newIndexedWork(
				"cluster2", fmt.Sprintf("%s-%d", constants.DeployHostingWorkNamePrefix("cluster1", "test"), 0), "cluster1",
			),
			indexFunc:     IndexManifestWorkByHostedAddon,
			expectedIndex: []string{"cluster1/test"},
		},
		{
			name: "hosted hook",
			work: newIndexedWork(
				"cluster2", constants.PreDeleteHookHostingWorkName("cluster1", "test"), "cluster1",
			),
			indexFunc:     IndexManifestWorkHookByHostedAddon,
			expectedIndex: []string{"cluster1/test"},
		},
		{
			name: "hosted work is not indexed as a default work",
			work: newIndexedWork(
				"cluster2", fmt.Sprintf("%s-%d", constants.DeployHostingWorkNamePrefix("cluster1", "test"), 0), "cluster1",
			),
			indexFunc:     IndexManifestWorkByAddon,
			expectedIndex: []string{},
		},
		{
			name: "hook work is not indexed as a deploy work",
			work: newIndexedWork(
				"cluster2", constants.PreDeleteHookHostingWorkName("cluster1", "test"), "cluster1",
			),
			indexFunc:     IndexManifestWorkByHostedAddon,
			expectedIndex: []string{},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual, err := c.indexFunc(c.work)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, c.expectedIndex) {
				t.Fatalf("expected %v, got %v", c.expectedIndex, actual)
			}
		})
	}
}

func newIndexedWork(namespace, name, addonNamespace string) *workapiv1.ManifestWork {
	labels := map[string]string{addonv1beta1.AddonLabelKey: "test"}
	if addonNamespace != "" {
		labels[addonv1beta1.AddonNamespaceLabelKey] = addonNamespace
	}
	return &workapiv1.ManifestWork{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      name,
		Labels:    labels,
	}}
}
