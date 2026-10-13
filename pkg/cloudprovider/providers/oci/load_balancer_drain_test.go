package oci

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/core"
	"go.uber.org/zap"
	authv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclient "k8s.io/client-go/kubernetes/fake"
	servicecontroller "k8s.io/cloud-provider/controllers/service"

	providercfg "github.com/oracle/oci-cloud-controller-manager/pkg/cloudprovider/providers/oci/config"
	"github.com/oracle/oci-cloud-controller-manager/pkg/oci/client"
)

const drainTestNodePort = 30080

// drainTestLB stands in for the OCI (N)LB API: it serves the stored load
// balancer, applies backend set updates to it the way OCI reports them back,
// and records every write.
type drainTestLB struct {
	client.GenericLoadBalancerInterface
	nlb     bool
	lb      *client.GenericLoadBalancer
	updates map[string][]client.GenericBackend
	writes  []string
	// replaced records backends an update would make OCI delete and recreate.
	replaced []string
}

// ociName is the name OCI gives a backend, which identifies it across updates.
func (f *drainTestLB) ociName(b client.GenericBackend) string {
	if f.nlb && b.TargetId != nil {
		return fmt.Sprintf("%s:%d", *b.TargetId, *b.Port)
	}
	return fmt.Sprintf("%s:%d", *b.IpAddress, *b.Port)
}

func (f *drainTestLB) backendNames(backendSet string) []string {
	var names []string
	for _, b := range f.lb.BackendSets[backendSet].Backends {
		names = append(names, f.ociName(b))
	}
	sort.Strings(names)
	return names
}

func (f *drainTestLB) GetLoadBalancerByName(ctx context.Context, compartmentID, name string) (*client.GenericLoadBalancer, error) {
	lb := *f.lb
	lb.BackendSets = make(map[string]client.GenericBackendSetDetails, len(f.lb.BackendSets))
	for k, bs := range f.lb.BackendSets {
		bs.Backends = append([]client.GenericBackend(nil), bs.Backends...)
		lb.BackendSets[k] = bs
	}
	return &lb, nil
}

func (f *drainTestLB) UpdateBackendSet(ctx context.Context, lbID, name string, details *client.GenericBackendSetDetails) (string, error) {
	f.updates[name] = append([]client.GenericBackend(nil), details.Backends...)
	bs := f.lb.BackendSets[name]
	existing := make(map[string]string, len(bs.Backends))
	for _, b := range bs.Backends {
		existing[fmt.Sprintf("%s:%d", *b.IpAddress, *b.Port)] = f.ociName(b)
	}
	for _, b := range f.asReportedByOCI(details.Backends) {
		if old, ok := existing[fmt.Sprintf("%s:%d", *b.IpAddress, *b.Port)]; ok && old != f.ociName(b) {
			f.replaced = append(f.replaced, fmt.Sprintf("%s: %s -> %s", name, old, f.ociName(b)))
		}
	}
	bs.Backends = f.asReportedByOCI(details.Backends)
	f.lb.BackendSets[name] = bs
	return "wr-update-" + name, nil
}

func (f *drainTestLB) CreateBackendSet(ctx context.Context, lbID, name string, details *client.GenericBackendSetDetails) (string, error) {
	f.writes = append(f.writes, "CreateBackendSet "+name)
	return "", nil
}

func (f *drainTestLB) DeleteBackendSet(ctx context.Context, lbID, name string) (string, error) {
	f.writes = append(f.writes, "DeleteBackendSet "+name)
	return "", nil
}

func (f *drainTestLB) CreateListener(ctx context.Context, lbID, name string, details *client.GenericListener) (string, error) {
	f.writes = append(f.writes, "CreateListener "+name)
	return "", nil
}

func (f *drainTestLB) UpdateListener(ctx context.Context, lbID, name string, details *client.GenericListener) (string, error) {
	f.writes = append(f.writes, "UpdateListener "+name)
	return "", nil
}

func (f *drainTestLB) DeleteListener(ctx context.Context, lbID, name string) (string, error) {
	f.writes = append(f.writes, "DeleteListener "+name)
	return "", nil
}

// OCI reports isDrain/drain as an explicit false on every backend, and LBaaS
// backends have no TargetId.
func (f *drainTestLB) asReportedByOCI(backends []client.GenericBackend) []client.GenericBackend {
	out := make([]client.GenericBackend, 0, len(backends))
	for _, b := range backends {
		if b.Drain == nil {
			b.Drain = common.Bool(false)
		}
		if !f.nlb {
			b.TargetId = nil
		}
		out = append(out, b)
	}
	return out
}

type drainTestNetworking struct {
	*MockVirtualNetworkClient
	subnet *core.Subnet
}

func (n *drainTestNetworking) GetSubnet(ctx context.Context, id string) (*core.Subnet, error) {
	return n.subnet, nil
}

func (n *drainTestNetworking) GetSubnetFromCacheByIP(ip client.IpAddresses) (*core.Subnet, error) {
	return n.subnet, nil
}

type drainTestOCIClient struct {
	MockOCIClient
	lb  *drainTestLB
	net *drainTestNetworking
}

func (c drainTestOCIClient) LoadBalancer(*zap.SugaredLogger, string, string, *authv1.TokenRequest) client.GenericLoadBalancerInterface {
	return c.lb
}

func (c drainTestOCIClient) Networking(*client.OCIClientConfig) client.NetworkingInterface {
	return c.net
}

type drainHarness struct {
	cp     *CloudProvider
	fake   *drainTestLB
	svc    *v1.Service
	lister *stubNodeLister
}

type drainTestNode struct {
	name, v4, v6 string
	tainted      bool
}

func (n drainTestNode) node() *v1.Node {
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: n.name},
		Spec:       v1.NodeSpec{ProviderID: "oci://ocid1.instance.oc1.." + n.name},
	}
	for _, ip := range []string{n.v4, n.v6} {
		if ip != "" {
			node.Status.Addresses = append(node.Status.Addresses, v1.NodeAddress{Type: v1.NodeInternalIP, Address: ip})
		}
	}
	if n.tainted {
		node.Spec.Taints = []v1.Taint{{Key: servicecontroller.ToBeDeletedTaint, Effect: v1.TaintEffectNoSchedule}}
	}
	return node
}

// newDrainHarness returns a CloudProvider whose load balancer currently has a
// backend for every one of initialNodes, as a previous sync would have left it.
func newDrainHarness(t *testing.T, lbType string, drainAnnotation bool, dualStack bool, initialNodes []*v1.Node) *drainHarness {
	t.Helper()
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: "web-uid", Annotations: map[string]string{}},
		Spec: v1.ServiceSpec{
			Type:            v1.ServiceTypeLoadBalancer,
			SessionAffinity: v1.ServiceAffinityNone,
			IPFamilies:      []v1.IPFamily{v1.IPv4Protocol},
			Ports:           []v1.ServicePort{{Protocol: v1.ProtocolTCP, Port: 80, NodePort: drainTestNodePort}},
		},
	}
	subnet := &core.Subnet{Id: common.String("ocid1.subnet.oc1..lb"), CidrBlock: common.String("10.0.0.0/16")}
	if dualStack {
		policy := v1.IPFamilyPolicyRequireDualStack
		svc.Spec.IPFamilyPolicy = &policy
		svc.Spec.IPFamilies = []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol}
		subnet.Ipv6CidrBlocks = []string{"2001:db8::/64"}
	}
	shape := lbDefaultShape
	if lbType == NLB {
		shape = flexible
		svc.Annotations[ServiceAnnotationLoadBalancerType] = NLB
		if drainAnnotation {
			svc.Annotations[ServiceAnnotationNetworkLoadBalancerDrainBackendsOnScaleDown] = "true"
		}
	} else if drainAnnotation {
		svc.Annotations[ServiceAnnotationLoadBalancerDrainBackendsOnScaleDown] = "true"
	}

	fake := &drainTestLB{nlb: lbType == NLB, updates: map[string][]client.GenericBackend{}}
	if lbType == NLB {
		fake.GenericLoadBalancerInterface = &MockNetworkLoadBalancerClient{}
	} else {
		fake.GenericLoadBalancerInterface = &MockLoadBalancerClient{}
	}
	lister := &stubNodeLister{nodes: initialNodes}
	cp := &CloudProvider{
		NodeLister: lister,
		client:     drainTestOCIClient{lb: fake, net: &drainTestNetworking{MockVirtualNetworkClient: &MockVirtualNetworkClient{}, subnet: subnet}},
		securityListManagerFactory: func(mode string) securityListManager {
			return MockSecurityListManager{}
		},
		config: &providercfg.Config{
			CompartmentID: "ocid1.compartment.oc1..cluster",
			LoadBalancer:  &providercfg.LoadBalancerConfig{Subnet1: *subnet.Id},
		},
		logger:     zap.S(),
		kubeclient: testclient.NewSimpleClientset(svc),
		lbLocks:    NewLoadBalancerLocks(),
	}

	ipVersions, err := cp.getOciIpVersions([]*core.Subnet{subnet}, []*core.Subnet{subnet}, svc)
	if err != nil {
		t.Fatalf("getOciIpVersions: %v", err)
	}
	spec, err := NewLBSpec(zap.S(), svc, initialNodes, []string{*subnet.Id}, nil, cp.securityListManagerFactory, ipVersions, nil, nil, cp.config.CompartmentID)
	if err != nil {
		t.Fatalf("NewLBSpec: %v", err)
	}
	backendSets := map[string]client.GenericBackendSetDetails{}
	for name, bs := range spec.BackendSets {
		bs.Backends = fake.asReportedByOCI(bs.Backends)
		backendSets[name] = bs
	}
	fake.lb = &client.GenericLoadBalancer{
		Id:             common.String("ocid1.loadbalancer.oc1..web"),
		CompartmentId:  common.String(cp.config.CompartmentID),
		DisplayName:    common.String(spec.Name),
		LifecycleState: common.String(lbLifecycleStateActive),
		ShapeName:      common.String(shape),
		IpVersion:      spec.IpVersions.LbEndpointIpVersion,
		SubnetIds:      []string{*subnet.Id},
		IpAddresses:    []client.GenericIpAddress{{IpAddress: common.String("192.0.2.10"), IsPublic: common.Bool(true)}},
		Listeners:      spec.Listeners,
		BackendSets:    backendSets,
	}
	return &drainHarness{cp: cp, fake: fake, svc: svc, lister: lister}
}

type syncFunc func(t *testing.T, h *drainHarness, nodes []*v1.Node) map[string][]client.GenericBackend

func viaUpdateLoadBalancer(t *testing.T, h *drainHarness, nodes []*v1.Node) map[string][]client.GenericBackend {
	t.Helper()
	h.fake.updates, h.fake.writes, h.fake.replaced = map[string][]client.GenericBackend{}, nil, nil
	if err := h.cp.UpdateLoadBalancer(context.Background(), "cluster", h.svc, nodes); err != nil {
		t.Fatalf("UpdateLoadBalancer: %v", err)
	}
	checkNoReplacedBackends(t, h)
	if len(h.fake.writes) != 0 {
		t.Fatalf("unexpected writes: %v", h.fake.writes)
	}
	return h.fake.updates
}

func viaEnsureLoadBalancer(t *testing.T, h *drainHarness, nodes []*v1.Node) map[string][]client.GenericBackend {
	t.Helper()
	h.fake.updates, h.fake.writes, h.fake.replaced = map[string][]client.GenericBackend{}, nil, nil
	if _, err := h.cp.EnsureLoadBalancer(context.Background(), "cluster", h.svc, nodes); err != nil {
		t.Fatalf("EnsureLoadBalancer: %v", err)
	}
	checkNoReplacedBackends(t, h)
	if len(h.fake.writes) != 0 {
		t.Fatalf("unexpected writes: %v", h.fake.writes)
	}
	return h.fake.updates
}

func checkNoReplacedBackends(t *testing.T, h *drainHarness) {
	t.Helper()
	if len(h.fake.replaced) != 0 {
		t.Fatalf("update changed the identity of existing backends: %v", h.fake.replaced)
	}
}

func assertBackendNames(t *testing.T, h *drainHarness, backendSet string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if got := h.fake.backendNames(backendSet); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("OCI backend names in %s:\n got  %q\n want %q", backendSet, got, want)
	}
}

// describeBackends renders a payload as sorted "ip:port target=<id> drain=<v>"
// lines so expectations read like the request sent to OCI.
func describeBackends(backends []client.GenericBackend) []string {
	out := make([]string, 0, len(backends))
	for _, b := range backends {
		target := "nil"
		if b.TargetId != nil {
			target = *b.TargetId
		}
		drain := "nil"
		if b.Drain != nil {
			drain = fmt.Sprint(*b.Drain)
		}
		out = append(out, fmt.Sprintf("%s:%d target=%s drain=%s", *b.IpAddress, *b.Port, target, drain))
	}
	sort.Strings(out)
	return out
}

func assertBackends(t *testing.T, got []client.GenericBackend, want ...string) {
	t.Helper()
	sort.Strings(want)
	gotDesc := describeBackends(got)
	if fmt.Sprint(gotDesc) != fmt.Sprint(want) {
		t.Fatalf("backends sent to OCI:\n got  %q\n want %q", gotDesc, want)
	}
}

func assertOnlyUpdated(t *testing.T, updates map[string][]client.GenericBackend, backendSets ...string) {
	t.Helper()
	if len(updates) != len(backendSets) {
		t.Fatalf("updated backend sets %v, want %v", keysOf(updates), backendSets)
	}
	for _, name := range backendSets {
		if _, ok := updates[name]; !ok {
			t.Fatalf("updated backend sets %v, want %v", keysOf(updates), backendSets)
		}
	}
}

func keysOf(m map[string][]client.GenericBackend) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func untainted(n drainTestNode) drainTestNode {
	n.tainted = false
	return n
}

func tainted(n drainTestNode) drainTestNode {
	n.tainted = true
	return n
}

var (
	nodeA = drainTestNode{name: "a", v4: "10.0.1.1", v6: "2001:db8::a"}
	nodeB = drainTestNode{name: "b", v4: "10.0.1.2", v6: "2001:db8::b"}
	nodeC = drainTestNode{name: "c", v4: "10.0.1.3", v6: "2001:db8::c"}
	nodeD = drainTestNode{name: "d", v4: "10.0.1.4", v6: "2001:db8::d"}
)

func v4Backend(n drainTestNode, drain string) string {
	return fmt.Sprintf("%s:%d target=ocid1.instance.oc1..%s drain=%s", n.v4, drainTestNodePort, n.name, drain)
}

func nlbName(n drainTestNode) string {
	return fmt.Sprintf("ocid1.instance.oc1..%s:%d", n.name, drainTestNodePort)
}

func lbName(n drainTestNode) string {
	return fmt.Sprintf("%s:%d", n.v4, drainTestNodePort)
}

func TestDrainBackendsOnScaleDown_TaintedNodeIsDrainedNotRemoved(t *testing.T) {
	for _, tc := range []struct {
		name        string
		lbType      string
		sync        syncFunc
		wantDrained string
		ociName     func(drainTestNode) string
	}{
		{"NLB via UpdateLoadBalancer", NLB, viaUpdateLoadBalancer, v4Backend(nodeC, "true"), nlbName},
		{"NLB via EnsureLoadBalancer", NLB, viaEnsureLoadBalancer, v4Backend(nodeC, "true"), nlbName},
		{"LB via UpdateLoadBalancer", LB, viaUpdateLoadBalancer, fmt.Sprintf("%s:%d target=nil drain=true", nodeC.v4, drainTestNodePort), lbName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDrainHarness(t, tc.lbType, true, false, []*v1.Node{nodeA.node(), nodeB.node(), nodeC.node()})
			h.lister.nodes = []*v1.Node{nodeA.node(), nodeB.node(), tainted(nodeC).node()}

			updates := tc.sync(t, h, []*v1.Node{nodeA.node(), nodeB.node()})

			assertOnlyUpdated(t, updates, "TCP-80")
			assertBackends(t, updates["TCP-80"], v4Backend(nodeA, "nil"), v4Backend(nodeB, "nil"), tc.wantDrained)
			assertBackendNames(t, h, "TCP-80", tc.ociName(nodeA), tc.ociName(nodeB), tc.ociName(nodeC))
		})
	}
}

func TestDrainBackendsOnScaleDown_UntaintedExcludedNodeIsRemoved(t *testing.T) {
	h := newDrainHarness(t, NLB, true, false, []*v1.Node{nodeA.node(), nodeB.node(), nodeC.node()})
	excluded := nodeC.node()
	excluded.Labels = map[string]string{excludeBackendFromLBLabel: "true"}
	h.lister.nodes = []*v1.Node{nodeA.node(), nodeB.node(), excluded}

	updates := viaUpdateLoadBalancer(t, h, []*v1.Node{nodeA.node(), nodeB.node()})

	assertOnlyUpdated(t, updates, "TCP-80")
	assertBackends(t, updates["TCP-80"], v4Backend(nodeA, "nil"), v4Backend(nodeB, "nil"))
}

func TestDrainBackendsOnScaleDown_DrainedBackendRemovedOnceNodeIsDeleted(t *testing.T) {
	h := newDrainHarness(t, NLB, true, false, []*v1.Node{nodeA.node(), nodeB.node(), nodeC.node()})
	h.lister.nodes = []*v1.Node{nodeA.node(), nodeB.node(), tainted(nodeC).node()}
	viaUpdateLoadBalancer(t, h, []*v1.Node{nodeA.node(), nodeB.node()})

	h.lister.nodes = []*v1.Node{nodeA.node(), nodeB.node()}
	updates := viaUpdateLoadBalancer(t, h, []*v1.Node{nodeA.node(), nodeB.node()})

	assertOnlyUpdated(t, updates, "TCP-80")
	assertBackends(t, updates["TCP-80"], v4Backend(nodeA, "nil"), v4Backend(nodeB, "nil"))
}

func TestDrainBackendsOnScaleDown_AbortedScaleDownUndrains(t *testing.T) {
	h := newDrainHarness(t, NLB, true, false, []*v1.Node{nodeA.node(), nodeB.node(), nodeC.node()})
	h.lister.nodes = []*v1.Node{nodeA.node(), nodeB.node(), tainted(nodeC).node()}
	viaUpdateLoadBalancer(t, h, []*v1.Node{nodeA.node(), nodeB.node()})

	all := []*v1.Node{nodeA.node(), nodeB.node(), untainted(nodeC).node()}
	h.lister.nodes = all
	updates := viaUpdateLoadBalancer(t, h, all)

	assertOnlyUpdated(t, updates, "TCP-80")
	assertBackends(t, updates["TCP-80"], v4Backend(nodeA, "nil"), v4Backend(nodeB, "nil"), v4Backend(nodeC, "false"))
	assertBackendNames(t, h, "TCP-80", nlbName(nodeA), nlbName(nodeB), nlbName(nodeC))

	if updates := viaUpdateLoadBalancer(t, h, all); len(updates) != 0 {
		t.Fatalf("expected no update once un-drained, got %v", keysOf(updates))
	}
}

func TestDrainBackendsOnScaleDown_AnnotationOffKeepsExistingBehavior(t *testing.T) {
	t.Run("tainted node is removed without drain fields", func(t *testing.T) {
		h := newDrainHarness(t, NLB, false, false, []*v1.Node{nodeA.node(), nodeB.node(), nodeC.node()})
		h.lister.nodes = []*v1.Node{nodeA.node(), nodeB.node(), tainted(nodeC).node()}

		updates := viaUpdateLoadBalancer(t, h, []*v1.Node{nodeA.node(), nodeB.node()})

		assertOnlyUpdated(t, updates, "TCP-80")
		assertBackends(t, updates["TCP-80"], v4Backend(nodeA, "nil"), v4Backend(nodeB, "nil"))
	})

	t.Run("drained backend that is desired is left alone", func(t *testing.T) {
		h := newDrainHarness(t, NLB, false, false, []*v1.Node{nodeA.node(), nodeB.node()})
		bs := h.fake.lb.BackendSets["TCP-80"]
		bs.Backends[0].Drain = common.Bool(true)
		h.fake.lb.BackendSets["TCP-80"] = bs

		if updates := viaUpdateLoadBalancer(t, h, []*v1.Node{nodeA.node(), nodeB.node()}); len(updates) != 0 {
			t.Fatalf("expected no update, got %v", keysOf(updates))
		}
	})
}

func TestDrainBackendsOnScaleDown_MultipleNodesDualStack(t *testing.T) {
	h := newDrainHarness(t, NLB, true, true, []*v1.Node{nodeA.node(), nodeB.node(), nodeC.node(), nodeD.node()})
	excludedD := nodeD.node()
	excludedD.Labels = map[string]string{excludeBackendFromLBLabel: "true"}
	h.lister.nodes = []*v1.Node{nodeA.node(), tainted(nodeB).node(), tainted(nodeC).node(), excludedD}

	updates := viaUpdateLoadBalancer(t, h, []*v1.Node{nodeA.node()})

	assertOnlyUpdated(t, updates, "TCP-80", "TCP-80-IPv6")
	assertBackends(t, updates["TCP-80"], v4Backend(nodeA, "nil"), v4Backend(nodeB, "true"), v4Backend(nodeC, "true"))
	v6 := func(n drainTestNode, drain string) string {
		return fmt.Sprintf("%s:%d target=nil drain=%s", n.v6, drainTestNodePort, drain)
	}
	assertBackends(t, updates["TCP-80-IPv6"], v6(nodeA, "nil"), v6(nodeB, "true"), v6(nodeC, "true"))
}

func TestDrainBackendsOnScaleDown_SteadyStateWhileDrainingSendsNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		sync syncFunc
	}{
		{"UpdateLoadBalancer", viaUpdateLoadBalancer},
		{"EnsureLoadBalancer", viaEnsureLoadBalancer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDrainHarness(t, NLB, true, false, []*v1.Node{nodeA.node(), nodeB.node(), nodeC.node()})
			h.lister.nodes = []*v1.Node{nodeA.node(), nodeB.node(), tainted(nodeC).node()}
			if updates := tc.sync(t, h, []*v1.Node{nodeA.node(), nodeB.node()}); len(updates) != 1 {
				t.Fatalf("expected the first sync to drain, got %v", keysOf(updates))
			}

			if updates := tc.sync(t, h, []*v1.Node{nodeA.node(), nodeB.node()}); len(updates) != 0 {
				t.Fatalf("expected no update while draining, got %v", keysOf(updates))
			}
		})
	}
}

func TestGetDrainBackendsOnScaleDown(t *testing.T) {
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		want        bool
		wantErr     bool
	}{
		{"unset", nil, false, false},
		{"NLB true", map[string]string{ServiceAnnotationLoadBalancerType: NLB, ServiceAnnotationNetworkLoadBalancerDrainBackendsOnScaleDown: "true"}, true, false},
		{"LB true", map[string]string{ServiceAnnotationLoadBalancerDrainBackendsOnScaleDown: "true"}, true, false},
		{"NLB ignores the LB annotation", map[string]string{ServiceAnnotationLoadBalancerType: NLB, ServiceAnnotationLoadBalancerDrainBackendsOnScaleDown: "true"}, false, false},
		{"invalid", map[string]string{ServiceAnnotationLoadBalancerDrainBackendsOnScaleDown: "yes please"}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getDrainBackendsOnScaleDown(&v1.Service{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}})
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("got (%v, %v), want (%v, err=%v)", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
