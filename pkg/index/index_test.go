package index

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	workapiv1 "open-cluster-management.io/api/work/v1"

	addonv1beta1 "open-cluster-management.io/api/addon/v1beta1"

	"open-cluster-management.io/addon-framework/pkg/addonmanager/constants"
)

func TestManifestWorkIndexesUseSourceIdentity(t *testing.T) {
	tests := []struct {
		name          string
		work          *workapiv1.ManifestWork
		indexFunc     func(interface{}) ([]string, error)
		expectedIndex []string
	}{
		{
			name: "default work with explicit source namespace",
			work: newIndexedWork(
				"cluster1", constants.DeployWorkNamePrefix("test")+"-0", "cluster1",
			),
			indexFunc:     IndexManifestWorkByAddon,
			expectedIndex: []string{"cluster1/test"},
		},
		{
			name: "hosted work",
			work: newIndexedWork(
				"cluster2", constants.DeployHostingWorkNamePrefix("cluster1", "test")+"-0", "cluster1",
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
			name: "all framework work inventory",
			work: newIndexedWork(
				"cluster2", constants.PreDeleteHookHostingWorkName("cluster1", "test"), "cluster1",
			),
			indexFunc:     IndexManifestWorkByAddonIdentity,
			expectedIndex: []string{"cluster1/test"},
		},
		{
			name: "foreign work is excluded from framework inventory",
			work: newIndexedWork(
				"cluster2", "foreign-work", "cluster1",
			),
			indexFunc: IndexManifestWorkByAddonIdentity,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := test.indexFunc(test.work)
			if err != nil {
				t.Fatal(err)
			}
			if len(actual) != len(test.expectedIndex) {
				t.Fatalf("expected %v, got %v", test.expectedIndex, actual)
			}
			for i := range actual {
				if actual[i] != test.expectedIndex[i] {
					t.Fatalf("expected %v, got %v", test.expectedIndex, actual)
				}
			}
		})
	}
}

func newIndexedWork(namespace, name, addonNamespace string) *workapiv1.ManifestWork {
	return &workapiv1.ManifestWork{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      name,
		Labels: map[string]string{
			addonv1beta1.AddonLabelKey:          "test",
			addonv1beta1.AddonNamespaceLabelKey: addonNamespace,
		},
	}}
}
