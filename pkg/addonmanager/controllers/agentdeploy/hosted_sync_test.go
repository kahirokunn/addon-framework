package agentdeploy

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"open-cluster-management.io/addon-framework/pkg/addonmanager/addontesting"
	"open-cluster-management.io/addon-framework/pkg/addonmanager/constants"
	"open-cluster-management.io/addon-framework/pkg/agent"
	"open-cluster-management.io/addon-framework/pkg/index"
	addonapiv1beta1 "open-cluster-management.io/api/addon/v1beta1"
	fakeaddon "open-cluster-management.io/api/client/addon/clientset/versioned/fake"
	addoninformers "open-cluster-management.io/api/client/addon/informers/externalversions"
	fakecluster "open-cluster-management.io/api/client/cluster/clientset/versioned/fake"
	clusterv1informers "open-cluster-management.io/api/client/cluster/informers/externalversions"
	fakework "open-cluster-management.io/api/client/work/clientset/versioned/fake"
	workinformers "open-cluster-management.io/api/client/work/informers/externalversions"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workapiv1 "open-cluster-management.io/api/work/v1"
	workapplier "open-cluster-management.io/sdk-go/pkg/apis/work/v1/applier"
	workbuilder "open-cluster-management.io/sdk-go/pkg/apis/work/v1/builder"
)

type testHostedAgent struct {
	name               string
	objects            []runtime.Object
	err                error
	ConfigCheckEnabled bool
	hostedModeDisabled bool
}

func (t *testHostedAgent) Manifests(ctx context.Context, cluster *clusterv1.ManagedCluster, addon *addonapiv1beta1.ManagedClusterAddOn) (
	[]runtime.Object, error) {
	return t.objects, t.err
}

func (t *testHostedAgent) GetAgentAddonOptions() agent.AgentAddonOptions {
	return agent.AgentAddonOptions{
		AddonName:          t.name,
		HostedModeEnabled:  !t.hostedModeDisabled,
		HostedModeInfoFunc: constants.GetHostedModeInfo,
		ConfigCheckEnabled: t.ConfigCheckEnabled,
	}
}

func TestHostingReconcile(t *testing.T) {
	cases := []struct {
		name                 string
		key                  string
		existingWork         []runtime.Object
		addon                []runtime.Object
		testaddon            *testHostedAgent
		cluster              []runtime.Object
		liveCluster          []runtime.Object
		blockWorkDeletion    bool
		validateAddonActions func(t *testing.T, actions []clienttesting.Action)
		validateWorkActions  func(t *testing.T, actions []clienttesting.Action)
	}{
		{
			name: "no cluster",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddon("test", "cluster1", "cluster2",
				registrationAppliedCondition)},
			cluster:      []runtime.Object{},
			existingWork: []runtime.Object{},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertSourceClusterBlocked(t, actions[0])
			},
			validateWorkActions: addontesting.AssertNoActions,
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewUnstructured("v1", "ConfigMap", "default", "test"),
			}},
		},
		{
			name: "no managed cluster",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddon("test", "cluster1", "cluster2",
				registrationAppliedCondition)},
			cluster:      []runtime.Object{addontesting.NewManagedCluster("cluster2")},
			existingWork: []runtime.Object{},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertSourceClusterBlocked(t, actions[0])
			},
			validateWorkActions: addontesting.AssertNoActions,
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewUnstructured("v1", "ConfigMap", "default", "test"),
			}},
		},
		{
			name: "do not clean up when only the cluster informer is stale",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddon(
				"test", "cluster1", "cluster2", registrationAppliedCondition,
			)},
			cluster:              []runtime.Object{},
			liveCluster:          []runtime.Object{addontesting.NewManagedCluster("cluster1")},
			existingWork:         []runtime.Object{getHostedDeployWork()},
			validateAddonActions: addontesting.AssertNoActions,
			validateWorkActions:  addontesting.AssertNoActions,
			testaddon:            &testHostedAgent{name: "test"},
		},
		{
			name: "use a live hosting cluster when only its informer is stale",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer(
				"test", "cluster1", "cluster2", registrationAppliedCondition,
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
			},
			liveCluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			existingWork: []runtime.Object{getHostedDeployWork()},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertHostingClusterValid(t, actions[0])
			},
			validateWorkActions: addontesting.AssertNoActions,
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}},
		},
		{
			name: "clear the source cluster blocked condition after the cluster returns",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer(
				"test",
				"cluster1",
				"cluster2",
				registrationAppliedCondition,
				metav1.Condition{
					Type:               addonapiv1beta1.ManagedClusterAddOnConditionAvailable,
					Status:             metav1.ConditionUnknown,
					ObservedGeneration: 0,
					Reason:             managedClusterNotFoundReason,
					Message:            "source managed cluster cluster1 is not found",
				},
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertSourceClusterUnblocked(t, actions[0])
			},
			validateWorkActions: addontesting.AssertNoActions,
			testaddon:           &testHostedAgent{name: "test"},
		},
		{
			name: "no hosting cluster",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddon("test", "cluster1", "cluster2",
				registrationAppliedCondition)},
			cluster:      []runtime.Object{addontesting.NewManagedCluster("cluster1")},
			existingWork: []runtime.Object{},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				// Update addon condition
				addontesting.AssertActions(t, actions, "patch")
				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				err := json.Unmarshal(patch, addOn)
				if err != nil {
					t.Fatal(err)
				}
				addOnCond := meta.FindStatusCondition(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnHostingClusterValidity)
				if addOnCond == nil {
					t.Fatal("condition should not be nil")
				}
				if addOnCond.Reason != addonapiv1beta1.HostingClusterValidityReasonInvalid {
					t.Errorf("Condition Reason is not correct: %v", addOnCond.Reason)
				}
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				// default sync creates a manifestWork in managed cluster
				addontesting.AssertActions(t, actions, "create")
				actual := actions[0].(clienttesting.CreateActionImpl).Object
				deployWork := actual.(*workapiv1.ManifestWork)
				if deployWork.Namespace != "cluster1" || deployWork.Name != fmt.Sprintf("%s-%v", constants.DeployWorkNamePrefix("test"), 0) {
					t.Errorf("the manifestWork %v/%v is not in managed cluster ns.", deployWork.Namespace, deployWork.Name)
				}
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewUnstructured("v1", "ConfigMap", "default", "test"),
			}},
		},
		{
			name: "keep the hosting cleanup finalizer until the work deletion is observable",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.SetAddonDeletionTimestamp(
				addontesting.NewHostedModeAddonWithFinalizer(
					"test", "cluster1", "cluster2", registrationAppliedCondition,
				),
				time.Now(),
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			existingWork:      []runtime.Object{getHostedDeployWork()},
			blockWorkDeletion: true,
			testaddon:         &testHostedAgent{name: "test"},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "delete", "list")
			},
		},
		{
			name: "clean hosted works after the hosting cluster is confirmed absent",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.SetAddonFinalizers(
				addontesting.NewHostedModeAddon(
					"test", "cluster1", "cluster2", registrationAppliedCondition,
				),
				addonapiv1beta1.AddonHostingManifestFinalizer,
				addonapiv1beta1.AddonHostingPreDeleteHookFinalizer,
			)},
			cluster:      []runtime.Object{addontesting.NewManagedCluster("cluster1")},
			existingWork: []runtime.Object{getHostedDeployWork()},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				if err := json.Unmarshal(patch, addOn); err != nil {
					t.Fatal(err)
				}
				condition := meta.FindStatusCondition(
					addOn.Status.Conditions,
					addonapiv1beta1.ManagedClusterAddOnHostingClusterValidity,
				)
				if condition == nil || condition.Reason != addonapiv1beta1.HostingClusterValidityReasonInvalid {
					t.Fatalf("expected invalid hosting cluster condition, got %#v", condition)
				}
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "delete", "list", "list")
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}},
		},
		{
			name: "add finalizer",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddon("test", "cluster1", "cluster2",
				registrationAppliedCondition)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},

			existingWork: []runtime.Object{},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				// Update finalizer
				addontesting.AssertActions(t, actions, "update")
				update := actions[0].(clienttesting.UpdateActionImpl).Object
				addOn := update.(*addonapiv1beta1.ManagedClusterAddOn)
				if len(addOn.Finalizers) != 1 {
					t.Errorf("expected 1 finalizer, but got %v", len(addOn.Finalizers))
				}
				if !addonHasFinalizer(addOn, addonapiv1beta1.AddonHostingManifestFinalizer) {
					t.Errorf("expected hosting manifest finalizer")
				}
			},
			validateWorkActions: addontesting.AssertNoActions,
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}},
		},
		{
			name: "clean all owned Works after the source cluster is confirmed absent",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.SetAddonFinalizers(
				addontesting.NewHostedModeAddon(
					"test",
					"cluster1",
					"cluster2",
					registrationAppliedCondition,
					metav1.Condition{
						Type:               addonapiv1beta1.ManagedClusterAddOnConditionAvailable,
						Status:             metav1.ConditionUnknown,
						ObservedGeneration: 0,
						Reason:             managedClusterNotFoundReason,
						Message:            "source managed cluster cluster1 is not found",
					},
				),
				addonapiv1beta1.AddonHostingManifestFinalizer,
				addonapiv1beta1.AddonHostingPreDeleteHookFinalizer,
			)},
			cluster: []runtime.Object{addontesting.NewManagedCluster("cluster2")},
			existingWork: []runtime.Object{
				getHostedDeployWork(),
				getHostedHookWork(true),
			},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "update")
				updated := actions[0].(clienttesting.UpdateActionImpl).Object.(*addonapiv1beta1.ManagedClusterAddOn)
				if len(updated.Finalizers) != 0 {
					t.Fatalf("expected framework finalizers to be removed, got %v", updated.Finalizers)
				}
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "list", "delete", "delete", "list")
			},
			testaddon: &testHostedAgent{name: "test"},
		},
		{
			name: "keep cleanup blocked when a deleting addon cannot render its protected hook",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.SetAddonFinalizers(
				addontesting.SetAddonDeletionTimestamp(
					addontesting.NewHostedModeAddon(
						"test",
						"cluster1",
						"cluster2",
						registrationAppliedCondition,
						metav1.Condition{
							Type:               addonapiv1beta1.ManagedClusterAddOnConditionAvailable,
							Status:             metav1.ConditionUnknown,
							ObservedGeneration: 0,
							Reason:             managedClusterNotFoundReason,
							Message:            "source managed cluster cluster1 is not found",
						},
					),
					time.Now(),
				),
				addonapiv1beta1.AddonHostingManifestFinalizer,
				addonapiv1beta1.AddonHostingPreDeleteHookFinalizer,
			)},
			cluster:              []runtime.Object{addontesting.NewManagedCluster("cluster2")},
			existingWork:         []runtime.Object{getHostedDeployWork()},
			validateAddonActions: addontesting.AssertNoActions,
			validateWorkActions:  addontesting.AssertNoActions,
			testaddon:            &testHostedAgent{name: "test"},
		},
		{
			name: "deploy manifests for an addon",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer("test", "cluster1", "cluster2",
				registrationAppliedCondition)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")

				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				err := json.Unmarshal(patch, addOn)
				if err != nil {
					t.Fatal(err)
				}
				addOnCond := meta.FindStatusCondition(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnHostingClusterValidity)
				if addOnCond == nil {
					t.Fatal("condition should not be nil")
				}
				if addOnCond.Reason != addonapiv1beta1.HostingClusterValidityReasonValid {
					t.Errorf("Condition Reason is not correct: %v", addOnCond.Reason)
				}
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				// default sync deletes the deploy work since there is no manifests needed deploy in the managed cluster
				// hosted sync creates the deploy work in the hosting cluster ns
				addontesting.AssertActions(t, actions, "create")
			},
		},
		{
			name: "update manifest for an addon",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer("test", "cluster1", "cluster2",
				registrationAppliedCondition)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
				addontesting.NewHostingUnstructured("v1", "Deployment", "default", "test"),
			}},
			existingWork: []runtime.Object{func() *workapiv1.ManifestWork {
				work := addontesting.NewManifestWork(
					constants.DeployHostingWorkNamePrefix("cluster1", "test"),
					"cluster2",
					addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test1"),
					addontesting.NewHostingUnstructured("v1", "Deployment", "default", "test1"),
				)
				setAddonWorkOwnership(work, "test", "cluster1")
				work.Status.Conditions = []metav1.Condition{
					{
						Type:   workapiv1.WorkApplied,
						Status: metav1.ConditionTrue,
					},
				}
				return work
			}()},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				// hosted sync updates the deploy work in the hosting cluster ns
				addontesting.AssertActions(t, actions, "patch")
			},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")

				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				err := json.Unmarshal(patch, addOn)
				if err != nil {
					t.Fatal(err)
				}
				if meta.IsStatusConditionFalse(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied) {
					t.Errorf("Condition Reason is not correct: %v", addOn.Status.Conditions)
				}

				manifestAppliyedCondition := meta.FindStatusCondition(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnManifestApplied)
				if manifestAppliyedCondition == nil {
					t.Fatal("manifestapplied condition should not be nil")
				}
				if manifestAppliyedCondition.Reason != addonapiv1beta1.AddonManifestAppliedReasonManifestsApplied {
					t.Errorf("Condition Reason is not correct: %v", manifestAppliyedCondition.Reason)
				}
				if manifestAppliyedCondition.Message != "no manifest need to apply" {
					t.Errorf("Condition Message is not correct: %v", manifestAppliyedCondition.Message)
				}
				if manifestAppliyedCondition.Status != metav1.ConditionTrue {
					t.Errorf("Condition Status is not correct: %v", manifestAppliyedCondition.Status)
				}
			},
		},
		{
			name: "do not update manifest for an addon",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer("test", "cluster1", "cluster2",
				registrationAppliedCondition)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
				addontesting.NewHostingUnstructured("v1", "Deployment", "default", "test"),
			}},
			existingWork: []runtime.Object{func() *workapiv1.ManifestWork {
				work := addontesting.NewManifestWork(
					constants.DeployHostingWorkNamePrefix("cluster1", "test"),
					"cluster2",
					addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
					addontesting.NewHostingUnstructured("v1", "Deployment", "default", "test"),
				)
				setAddonWorkOwnership(work, "test", "cluster1")
				work.Status.Conditions = []metav1.Condition{
					{
						Type:   workapiv1.WorkApplied,
						Status: metav1.ConditionTrue,
					},
				}
				return work
			}()},
			validateWorkActions: addontesting.AssertNoActions,
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")

				assertHostingClusterValid(t, actions[0])

				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				err := json.Unmarshal(patch, addOn)
				if err != nil {
					t.Fatal(err)
				}
				if meta.IsStatusConditionFalse(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied) {
					t.Errorf("Condition Reason is not correct: %v", addOn.Status.Conditions)
				}
			},
		},
		{
			name: "get error when run manifest from agent",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer("test", "cluster1", "cluster2",
				registrationAppliedCondition)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			testaddon: &testHostedAgent{
				name: "test",
				objects: []runtime.Object{
					addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
				},
				err: fmt.Errorf("run manifest failed"),
			},
			existingWork: []runtime.Object{func() *workapiv1.ManifestWork {
				work := addontesting.NewManifestWork(
					constants.DeployHostingWorkNamePrefix("cluster1", "test"),
					"cluster2",
					addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
					addontesting.NewHostingUnstructured("v1", "Deployment", "default", "test"),
				)
				work.Status.Conditions = []metav1.Condition{
					{
						Type:   workapiv1.WorkApplied,
						Status: metav1.ConditionTrue,
					},
				}
				return work
			}()},
			validateWorkActions: addontesting.AssertNoActions,
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")

				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				err := json.Unmarshal(patch, addOn)
				if err != nil {
					t.Fatal(err)
				}
				if !meta.IsStatusConditionFalse(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnManifestApplied) {
					t.Errorf("Condition Reason is not correct: %v", addOn.Status.Conditions)
				}
			},
		},
		{
			name: "default to hosted deletes the default Work before creating the hosted Work",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer(
				"test", "cluster1", "cluster2", registrationAppliedCondition,
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			existingWork: []runtime.Object{getDeployWork()},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertHostingClusterValid(t, actions[0])
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "delete", "create")
				if actions[0].GetNamespace() != "cluster1" {
					t.Errorf("expected default Work deletion first, got namespace %s", actions[0].GetNamespace())
				}
				if actions[1].GetNamespace() != "cluster2" {
					t.Errorf("expected hosted Work creation second, got namespace %s", actions[1].GetNamespace())
				}
			},
		},
		{
			name: "delete stale work before creating it on a new hosting cluster",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer(
				"test", "cluster1", "cluster3", registrationAppliedCondition,
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
				addontesting.NewManagedCluster("cluster3"),
			},
			existingWork: []runtime.Object{getHostedDeployWork()},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertHostingClusterValid(t, actions[0])
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "delete", "create")
				deleteAction := actions[0].(clienttesting.DeleteActionImpl)
				if deleteAction.Namespace != "cluster2" {
					t.Errorf("expected stale Work deletion from cluster2, got %s", deleteAction.Namespace)
				}
				created := actions[1].(clienttesting.CreateActionImpl).Object.(*workapiv1.ManifestWork)
				if created.Namespace != "cluster3" {
					t.Errorf("expected desired Work creation in cluster3, got %s", created.Namespace)
				}
			},
		},
		{
			name: "delete the last hosted work when desired manifests become empty",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer(
				"test", "cluster1", "cluster2", registrationAppliedCondition,
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			existingWork: []runtime.Object{getHostedDeployWork()},
			testaddon:    &testHostedAgent{name: "test"},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertHostingClusterValid(t, actions[0])
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "delete")
			},
		},
		{
			name: "hosted mode disablement cleans work before removing its cleanup finalizer",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.SetAddonFinalizers(
				addontesting.NewHostedModeAddon("test", "cluster1", "cluster2", registrationAppliedCondition),
				addonapiv1beta1.AddonHostingManifestFinalizer,
				addonapiv1beta1.AddonHostingPreDeleteHookFinalizer,
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			existingWork: []runtime.Object{getHostedDeployWork()},
			testaddon: &testHostedAgent{
				name:               "test",
				hostedModeDisabled: true,
				objects: []runtime.Object{
					addontesting.NewUnstructured("v1", "ConfigMap", "default", "test"),
				},
			},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "update")
				updated := actions[0].(clienttesting.UpdateActionImpl).Object.(*addonapiv1beta1.ManagedClusterAddOn)
				if addonHasFinalizer(updated, addonapiv1beta1.AddonHostingManifestFinalizer) {
					t.Error("expected hosting cleanup finalizer to be removed")
				}
				if addonHasFinalizer(updated, addonapiv1beta1.AddonHostingPreDeleteHookFinalizer) {
					t.Error("expected hosted hook finalizer to be removed after hook cleanup")
				}
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "delete", "list", "create", "list")
			},
		},
		{
			name: "persist status before deleting finalizer",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.SetAddonDeletionTimestamp(
				addontesting.NewHostedModeAddonWithFinalizer("test", "cluster1", "cluster2",
					registrationAppliedCondition),
				time.Now(),
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
				addontesting.NewHostingUnstructured("v1", "Deployment", "default", "test"),
			}},
			existingWork: []runtime.Object{func() *workapiv1.ManifestWork {
				work := addontesting.NewManifestWork(
					constants.DeployHostingWorkNamePrefix("cluster1", "test"),
					"cluster2",
					addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
					addontesting.NewHostingUnstructured("v1", "Deployment", "default", "test"),
				)
				setAddonWorkOwnership(work, "test", "cluster1")
				work.Status.Conditions = []metav1.Condition{
					{
						Type:   workapiv1.WorkApplied,
						Status: metav1.ConditionTrue,
					},
				}
				return work
			}()},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "delete", "list", "list")
			},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
			},
		},
		{
			name: "deploy manifests for an addon when ConfigCheckEnabled is true",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer("test", "cluster1", "cluster2",
				registrationAppliedCondition, configuredCondition)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}, ConfigCheckEnabled: true},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertHostingClusterValid(t, actions[0])

				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				err := json.Unmarshal(patch, addOn)
				if err != nil {
					t.Fatal(err)
				}
				addOnCond := meta.FindStatusCondition(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnHostingClusterValidity)
				if addOnCond == nil {
					t.Fatal("condition should not be nil")
				}
				if addOnCond.Reason != addonapiv1beta1.HostingClusterValidityReasonValid {
					t.Errorf("Condition Reason is not correct: %v", addOnCond.Reason)
				}
			},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "create")
			},
		},
		{
			name: "not deploy manifests for an addon when ConfigCheckEnabled is true",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer("test", "cluster1", "cluster2",
				registrationAppliedCondition)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}, ConfigCheckEnabled: true},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertHostingClusterValid(t, actions[0])

				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				err := json.Unmarshal(patch, addOn)
				if err != nil {
					t.Fatal(err)
				}
				addOnCond := meta.FindStatusCondition(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnHostingClusterValidity)
				if addOnCond == nil {
					t.Fatal("condition should not be nil")
				}
				if addOnCond.Reason != addonapiv1beta1.HostingClusterValidityReasonValid {
					t.Errorf("Condition Reason is not correct: %v", addOnCond.Reason)
				}
			},
			validateWorkActions: addontesting.AssertNoActions,
		},
		{
			name: "clear stale False HostingManifestApplied condition when WorkApplied is nil",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer("test", "cluster1", "cluster2",
				registrationAppliedCondition,
				metav1.Condition{
					Type:    addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied,
					Status:  metav1.ConditionFalse,
					Reason:  addonapiv1beta1.AddonManifestAppliedReasonWorkApplyFailed,
					Message: "failed to build manifestwork: some old error",
				},
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}},
			existingWork: []runtime.Object{func() *workapiv1.ManifestWork {
				work := addontesting.NewManifestWork(
					constants.DeployHostingWorkNamePrefix("cluster1", "test"),
					"cluster2",
					addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
				)
				setAddonWorkOwnership(work, "test", "cluster1")
				// ManifestWork created but work-agent hasn't reported status yet (WorkApplied is nil)
				work.Status.Conditions = []metav1.Condition{}
				return work
			}()},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				// May patch work to update ownerReferences
				if len(actions) > 0 {
					addontesting.AssertActions(t, actions, "patch")
				}
			},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertHostingClusterValid(t, actions[0])

				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				err := json.Unmarshal(patch, addOn)
				if err != nil {
					t.Fatal(err)
				}
				// The stale False condition should be removed
				manifestAppliedCond := meta.FindStatusCondition(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied)
				if manifestAppliedCond != nil {
					t.Errorf("Expected HostingManifestApplied condition to be removed, but got: %v", manifestAppliedCond)
				}
			},
		},
		{
			name: "keep True HostingManifestApplied condition when WorkApplied is nil",
			key:  "cluster1/test",
			addon: []runtime.Object{addontesting.NewHostedModeAddonWithFinalizer("test", "cluster1", "cluster2",
				registrationAppliedCondition,
				metav1.Condition{
					Type:    addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied,
					Status:  metav1.ConditionTrue,
					Reason:  addonapiv1beta1.AddonManifestAppliedReasonManifestsApplied,
					Message: "manifests of addon are applied successfully",
				},
			)},
			cluster: []runtime.Object{
				addontesting.NewManagedCluster("cluster1"),
				addontesting.NewManagedCluster("cluster2"),
			},
			testaddon: &testHostedAgent{name: "test", objects: []runtime.Object{
				addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
			}},
			existingWork: []runtime.Object{func() *workapiv1.ManifestWork {
				work := addontesting.NewManifestWork(
					constants.DeployHostingWorkNamePrefix("cluster1", "test"),
					"cluster2",
					addontesting.NewHostingUnstructured("v1", "ConfigMap", "default", "test"),
				)
				setAddonWorkOwnership(work, "test", "cluster1")
				// ManifestWork exists but work-agent hasn't reported new status yet
				work.Status.Conditions = []metav1.Condition{}
				return work
			}()},
			validateWorkActions: func(t *testing.T, actions []clienttesting.Action) {
				// May patch work to update ownerReferences
				if len(actions) > 0 {
					addontesting.AssertActions(t, actions, "patch")
				}
			},
			validateAddonActions: func(t *testing.T, actions []clienttesting.Action) {
				addontesting.AssertActions(t, actions, "patch")
				assertHostingClusterValid(t, actions[0])

				patch := actions[0].(clienttesting.PatchActionImpl).Patch
				addOn := &addonapiv1beta1.ManagedClusterAddOn{}
				err := json.Unmarshal(patch, addOn)
				if err != nil {
					t.Fatal(err)
				}
				// The True condition should remain unchanged
				manifestAppliedCond := meta.FindStatusCondition(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnHostingManifestApplied)
				if manifestAppliedCond == nil || manifestAppliedCond.Status != metav1.ConditionTrue {
					t.Errorf("Expected HostingManifestApplied condition to remain True, but got: %v", manifestAppliedCond)
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fakeWorkClient := fakework.NewSimpleClientset(c.existingWork...)
			clusterObjects := c.cluster
			if c.liveCluster != nil {
				clusterObjects = c.liveCluster
			}
			fakeClusterClient := fakecluster.NewSimpleClientset(clusterObjects...)
			fakeAddonClient := fakeaddon.NewSimpleClientset(c.addon...)
			if c.blockWorkDeletion {
				blockManifestWorkDeletion(fakeWorkClient)
			}

			workInformerFactory := workinformers.NewSharedInformerFactory(fakeWorkClient, 10*time.Minute)
			addonInformers := addoninformers.NewSharedInformerFactory(fakeAddonClient, 10*time.Minute)
			clusterInformers := clusterv1informers.NewSharedInformerFactory(fakeClusterClient, 10*time.Minute)

			err := workInformerFactory.Work().V1().ManifestWorks().Informer().AddIndexers(
				cache.Indexers{
					index.ManifestWorkByAddon:           index.IndexManifestWorkByAddon,
					index.ManifestWorkByHostedAddon:     index.IndexManifestWorkByHostedAddon,
					index.ManifestWorkHookByHostedAddon: index.IndexManifestWorkHookByHostedAddon,
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			for _, obj := range c.cluster {
				if err := clusterInformers.Cluster().V1().ManagedClusters().Informer().GetStore().Add(obj); err != nil {
					t.Fatal(err)
				}
			}
			for _, obj := range c.addon {
				if err := addonInformers.Addon().V1beta1().ManagedClusterAddOns().Informer().GetStore().Add(obj); err != nil {
					t.Fatal(err)
				}
			}
			for _, obj := range c.existingWork {
				if err := workInformerFactory.Work().V1().ManifestWorks().Informer().GetStore().Add(obj); err != nil {
					t.Fatal(err)
				}
			}

			controller := addonDeployController{
				workClient:                fakeWorkClient,
				workApplier:               workapplier.NewWorkApplierWithTypedClient(fakeWorkClient, workInformerFactory.Work().V1().ManifestWorks().Lister()),
				workBuilder:               workbuilder.NewWorkBuilder(),
				addonClient:               fakeAddonClient,
				clusterClient:             fakeClusterClient,
				managedClusterLister:      clusterInformers.Cluster().V1().ManagedClusters().Lister(),
				managedClusterAddonLister: addonInformers.Addon().V1beta1().ManagedClusterAddOns().Lister(),
				workIndexer:               workInformerFactory.Work().V1().ManifestWorks().Informer().GetIndexer(),
				agentAddons:               map[string]agent.AgentAddon{c.testaddon.name: c.testaddon},
			}

			syncContext := addontesting.NewFakeSyncContext(t)
			err = controller.sync(context.TODO(), syncContext, c.key)
			switch {
			case c.blockWorkDeletion:
				if err == nil {
					t.Errorf("expected an error when the work deletion is not observable yet")
				}
			case (err == nil) != (c.testaddon.err == nil):
				t.Errorf("expected error %v when sync got %v", c.testaddon.err, err)
			case err != nil && err.Error() != c.testaddon.err.Error():
				t.Errorf("expected error %v when sync got %v", c.testaddon.err, err)
			}
			c.validateAddonActions(t, fakeAddonClient.Actions())
			c.validateWorkActions(t, fakeWorkClient.Actions())
		})
	}
}

func assertSourceClusterBlocked(t *testing.T, action clienttesting.Action) {
	addOnCond := patchedAvailableCondition(t, action)
	if addOnCond == nil {
		t.Fatal("condition should not be nil")
	}
	if addOnCond.Reason != managedClusterNotFoundReason {
		t.Errorf("Condition Reason is not correct: %v", addOnCond.Reason)
	}
}

func assertSourceClusterUnblocked(t *testing.T, action clienttesting.Action) {
	if addOnCond := patchedAvailableCondition(t, action); addOnCond != nil {
		t.Errorf("stale source cluster condition should be removed, but got: %v", addOnCond.Reason)
	}
}

func patchedAvailableCondition(t *testing.T, action clienttesting.Action) *metav1.Condition {
	patch := action.(clienttesting.PatchActionImpl).Patch
	addOn := &addonapiv1beta1.ManagedClusterAddOn{}
	if err := json.Unmarshal(patch, addOn); err != nil {
		t.Fatal(err)
	}
	return meta.FindStatusCondition(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnConditionAvailable)
}

func assertHostingClusterValid(t *testing.T, actions clienttesting.Action) {
	patch := actions.(clienttesting.PatchActionImpl).Patch
	addOn := &addonapiv1beta1.ManagedClusterAddOn{}
	err := json.Unmarshal(patch, addOn)
	if err != nil {
		t.Fatal(err)
	}
	addOnCond := meta.FindStatusCondition(addOn.Status.Conditions, addonapiv1beta1.ManagedClusterAddOnHostingClusterValidity)
	if addOnCond == nil {
		t.Fatal("condition should not be nil")
	}
	if addOnCond.Reason != addonapiv1beta1.HostingClusterValidityReasonValid {
		t.Errorf("Condition Reason is not correct: %v", addOnCond.Reason)
	}
}
