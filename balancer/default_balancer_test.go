package balancer

import (
	"fmt"
	"testing"
)

// A rack manager for testing purpose.
type MappedRackManager struct {
	// Maps servers to corresponding rack.
	serverMap map[string]string
	// Maps racks to its associated servers.
	rackMap map[string][]string
}

func NewMappedRackManager(serverMap map[string]string) *MappedRackManager {
	rackMap := make(map[string][]string)
	for h, r := range serverMap {
		slist := rackMap[r]
		slist = append(slist, h)
		rackMap[r] = slist
	}

	return &MappedRackManager{
		serverMap: serverMap,
		rackMap:   rackMap,
	}
}

func (rm *MappedRackManager) GetRack(host string) string {
	ret, _ := rm.serverMap[host]
	return ret
}

func (rm *MappedRackManager) GetServers(rack string) []string {
	ret, _ := rm.rackMap[rack]
	return ret
}

// A PlacementManager for testing.
type PassThroughPlacementManager struct {
	actions []*PlacementAction
}

func NewPassThroughPlacementManager() *PassThroughPlacementManager {
	return &PassThroughPlacementManager{}
}

func (pm *PassThroughPlacementManager) Place(task *PlacementAction) {
	pm.actions = append(pm.actions, task)
}

// A StateManager for testing.
type PassThroughStateManager struct {
	adds     [][]Region
	removals [][]Region
}

func NewPassThroughStateManager() *PassThroughStateManager {
	return &PassThroughStateManager{}
}

func (sm *PassThroughStateManager) Commit(adds, removals []Region) {
	sm.adds = append(sm.adds, adds)
	sm.removals = append(sm.removals, removals)
}

// Returns a default balancer for test. @serverMap maps servers to the rack
func DefaultBalancerForTest(serverMap map[ServerName]string) *DefaultBalancer {
	hostMap := make(map[string]string)
	servers := make([]ServerName, 0)

	for s, r := range serverMap {
		old, _ := hostMap[s.Host]
		if len(old) != 0 && old != r {
			panic("same host cannot belong to different rack!")
		}
		hostMap[s.Host] = r
		servers = append(servers, s)
	}

	opts := BalancerOptions{
		BalancerName:                "testBalancer",
		NumReplicas:                 3,
		MaxRegionsPerServer:         10,
		NumIterationPerBalanceRound: 3,
		NumServersInSmallDeployment: 3,
		RackManager:                 NewMappedRackManager(hostMap),
		PlacementManager:            NewPassThroughPlacementManager(),
		StateManager:                NewPassThroughStateManager(),
	}

	return NewDefaultBalancer(&opts, servers)
}

func TestDefaultBalancerInitialStats(t *testing.T) {
	// Setup servers in the test.
	serverMap := make(map[ServerName]string)

	serverMap[ServerName{Host: "a"}] = "r"
	serverMap[ServerName{Host: "b"}] = "r"
	serverMap[ServerName{Host: "c"}] = "r"

	b := DefaultBalancerForTest(serverMap)

	stats := make([]ServerStat, 0)
	for s, _ := range serverMap {
		stat := ServerStat{
			ServerName:  s,
			UpTimestamp: 1,
		}
		stats = append(stats, stat)
	}

	// Verify that no region has been created before first stat.
	if len(b.regionMap) != 0 {
		t.Error("There should be no region yet!")
	}

	b.UpdateServerStats(1, stats)

	// Verify that the first region has been created after first stats.
	if len(b.regionMap) != 1 {
		t.Error("length of region map should be 1!")
	}

	// Verify that 3 replicas has been assigned to the region.
	hosts := make(map[string]int)
	for _, slist := range b.regionMap {
		for _, s := range slist {
			hosts[s.Host] = 1
		}
	}
	if len(hosts) != 3 {
		t.Error("Fails to find 3 replicas!", len(hosts))
	}

	// Verify that there are 3 placement actions.
	pm := b.opts.PlacementManager.(*PassThroughPlacementManager)
	if len(pm.actions) != 3 {
		t.Error("Expect no placement action")
	}
}

func TestDefaultBalancerHostChoice(t *testing.T) {
	// Setup servers in the test.
	serverMap := make(map[ServerName]string)

	serverMap[ServerName{Host: "a", Port: 1000}] = "1"
	serverMap[ServerName{Host: "a", Port: 1001}] = "1"
	serverMap[ServerName{Host: "a", Port: 1002}] = "1"
	serverMap[ServerName{Host: "b"}] = "1"
	serverMap[ServerName{Host: "c"}] = "1"

	b := DefaultBalancerForTest(serverMap)

	stats := make([]ServerStat, 0)
	for s, _ := range serverMap {
		stat := ServerStat{
			ServerName:  s,
			UpTimestamp: 1,
		}
		stats = append(stats, stat)
	}

	// Verify that no region has been created before first stat.
	if len(b.regionMap) != 0 {
		t.Error("There should be no region yet!")
	}

	b.UpdateServerStats(1, stats)

	// Verify that 3 replicas has been assigned to the region.
	hosts := make(map[string]int)
	for _, slist := range b.regionMap {
		for _, s := range slist {
			hosts[s.Host] = 1
		}
	}
	if len(hosts) != 3 {
		t.Error("Fails to find 3 replicas!", len(hosts))
	}

	// Verify that "d" and "e" have been assigned.
	expected := []string{"b", "c"}
	for _, e := range expected {
		_, found := hosts[e]
		if !found {
			t.Error("Fails to find host ", e)
		}
	}
}

func TestDefaultBalancerRackChoice(t *testing.T) {
	// Setup servers in the test.
	serverMap := make(map[ServerName]string)

	serverMap[ServerName{Host: "a"}] = "1"
	serverMap[ServerName{Host: "b"}] = "1"
	serverMap[ServerName{Host: "c"}] = "1"
	serverMap[ServerName{Host: "d"}] = "2"
	serverMap[ServerName{Host: "e"}] = "3"

	b := DefaultBalancerForTest(serverMap)

	stats := make([]ServerStat, 0)
	for s, _ := range serverMap {
		stat := ServerStat{
			ServerName:  s,
			UpTimestamp: 1,
		}
		stats = append(stats, stat)
	}

	// Verify that no region has been created before first stat.
	if len(b.regionMap) != 0 {
		t.Error("There should be no region yet!")
	}

	b.UpdateServerStats(1, stats)

	// Verify that 3 replicas has been assigned to the region.
	hosts := make(map[string]int)
	for _, slist := range b.regionMap {
		for _, s := range slist {
			hosts[s.Host] = 1
		}
	}
	if len(hosts) != 3 {
		t.Error("Fails to find 3 replicas!", len(hosts))
	}

	// Verify that "d" and "e" have been assigned.
	expected := []string{"d", "e"}
	for _, e := range expected {
		_, found := hosts[e]
		if !found {
			t.Error("Fails to find host ", e)
		}
	}
}

func TestDefaultBalancerInitialSplit(t *testing.T) {
	// Setup servers in the test.
	serverMap := make(map[ServerName]string)

	serverMap[ServerName{Host: "a"}] = "r"
	serverMap[ServerName{Host: "b"}] = "r"
	serverMap[ServerName{Host: "c"}] = "r"

	b := DefaultBalancerForTest(serverMap)

	stats := make([]ServerStat, 0)
	for s, _ := range serverMap {
		stat := ServerStat{
			ServerName:  s,
			UpTimestamp: 1,
		}
		stats = append(stats, stat)
	}

	// Verify that no region has been created before first stat.
	if len(b.regionMap) != 0 {
		t.Error("There should be no region yet!")
	}

	b.UpdateServerStats(1, stats)

	// Clean up pass through objects.
	pm := b.opts.PlacementManager.(*PassThroughPlacementManager)
	sm := b.opts.StateManager.(*PassThroughStateManager)
	pm.actions = pm.actions[:0]
	sm.adds = sm.adds[:0]
	sm.removals = sm.removals[:0]

	// Now split the region.
	left := Region{EndKey: "hello"}
	right := Region{StartKey: "hello"}
	ok := b.SplitRegion(Region{}, left, right)

	if !ok {
		t.Error("Fails to split the region")
	}

	// Verify adds and removals actions.
	if len(sm.adds) != 1 || len(sm.removals) != 1 {
		t.Error("should have some activity in sm!")
	} else if len(sm.adds[0]) != 2 || len(sm.removals[0]) != 1 {
		t.Error("Not exact adds and removals!")
	}

	// Verify that balancer does not contact storage servers directly.
	if len(pm.actions) != 0 {
		t.Error("Does not expect activities on storage servers")
	}
}

func TestDefaultBalancerMultiSplit(t *testing.T) {
	// Setup servers in the test.
	serverMap := make(map[ServerName]string)

	serverMap[ServerName{Host: "a"}] = "r"
	serverMap[ServerName{Host: "b"}] = "r"
	serverMap[ServerName{Host: "c"}] = "r"

	b := DefaultBalancerForTest(serverMap)

	stats := make([]ServerStat, 0)
	for s, _ := range serverMap {
		stat := ServerStat{
			ServerName:  s,
			UpTimestamp: 1,
		}
		stats = append(stats, stat)
	}

	b.UpdateServerStats(1, stats)

	for i := 10; i < 20; i++ {
		endKey := ""
		midKey := fmt.Sprintf("%d", i)

		var preKey string
		if i != 10 {
			preKey = fmt.Sprintf("%d", i-1)
		}

		// Clean up pass through objects.
		pm := b.opts.PlacementManager.(*PassThroughPlacementManager)
		sm := b.opts.StateManager.(*PassThroughStateManager)
		pm.actions = pm.actions[:0]
		sm.adds = sm.adds[:0]
		sm.removals = sm.removals[:0]

		// Now split the region.
		left := Region{StartKey: preKey, EndKey: midKey}
		right := Region{StartKey: midKey, EndKey: endKey}
		orig := Region{StartKey: preKey, EndKey: endKey}
		ok := b.SplitRegion(orig, left, right)

		if !ok {
			t.Error("Fails to split the region")
		}

		// Verify adds and removals actions.
		if len(sm.adds) != 1 || len(sm.removals) != 1 {
			t.Error("should have some activity in sm!")
		} else if len(sm.adds[0]) != 2 || len(sm.removals[0]) != 1 {
			t.Error("Not exact adds and removals!")
		}

		// Verify that balancer does not contact storage servers directly.
		if len(pm.actions) != 0 {
			t.Error("Does not expect activities on storage servers")
		}
	}
}

func TestDefaultBalancerSameReplicationGroupMerge(t *testing.T) {
	// Setup servers in the test.
	serverMap := make(map[ServerName]string)

	serverMap[ServerName{Host: "a"}] = "r"
	serverMap[ServerName{Host: "b"}] = "r"
	serverMap[ServerName{Host: "c"}] = "r"

	b := DefaultBalancerForTest(serverMap)

	stats := make([]ServerStat, 0)
	for s, _ := range serverMap {
		stat := ServerStat{
			ServerName:  s,
			UpTimestamp: 1,
		}
		stats = append(stats, stat)
	}

	b.UpdateServerStats(1, stats)

	// Now split the region.
	left := Region{EndKey: "hello"}
	right := Region{StartKey: "hello"}
	ok := b.SplitRegion(Region{}, left, right)
	if !ok {
		t.Error("Fails to split the region")
	}

	// Clean up pass through objects.
	pm := b.opts.PlacementManager.(*PassThroughPlacementManager)
	sm := b.opts.StateManager.(*PassThroughStateManager)
	pm.actions = pm.actions[:0]
	sm.adds = sm.adds[:0]
	sm.removals = sm.removals[:0]

	// Now merge region.
	light := left
	b.MergeRegions(left, right, light)

	// Verify adds and removals actions.
	if len(sm.adds) != 1 || len(sm.removals) != 1 {
		t.Error("should have some activity in sm!")
	} else if len(sm.adds[0]) != 1 || len(sm.removals[0]) != 2 {
		t.Error("Not exact adds and removals!")
	}
}
