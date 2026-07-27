package agent

import (
	"testing"

	addonapiv1beta1 "open-cluster-management.io/api/addon/v1beta1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"

	"open-cluster-management.io/addon-framework/pkg/addonmanager/constants"
)

func TestAgentAddonOptionsInstallMode(t *testing.T) {
	addon := &addonapiv1beta1.ManagedClusterAddOn{}
	cluster := &clusterv1.ManagedCluster{}

	called := false
	options := AgentAddonOptions{
		HostedModeInfoFunc: func(*addonapiv1beta1.ManagedClusterAddOn, *clusterv1.ManagedCluster) (string, string) {
			called = true
			return constants.InstallModeHosted, "hosting-cluster"
		},
	}

	mode, hostingCluster := options.InstallMode(addon, cluster)
	if mode != constants.InstallModeDefault || hostingCluster != "" {
		t.Fatalf("expected disabled hosted mode to use default install mode, got %q on %q", mode, hostingCluster)
	}
	if called {
		t.Fatal("expected hosted mode callback not to be called when hosted mode is disabled")
	}

	options.HostedModeEnabled = true
	mode, hostingCluster = options.InstallMode(addon, cluster)
	if mode != constants.InstallModeHosted || hostingCluster != "hosting-cluster" {
		t.Fatalf("expected enabled hosted mode callback result, got %q on %q", mode, hostingCluster)
	}
}
