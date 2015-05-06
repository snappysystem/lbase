package balancer

import (
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
		NumServersInSmallDeployment: 6,
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
