package balancer

// A structure that uniquely identify a server.
type ServerName struct {
	Host string
	Port int
}

// Provide network topology.
type RackManager interface {
	// Given a host, return the racks that it belongs to.
	GetRack(host string) string
	// Given a rack, returns all hosts in the rack.
	GetServers(rack string) []string
}

// A region (i.e. a shard), is the smallest unit for data movement.
// A region is determined by the starting and the ending key
// (not inclusive).
// An empty StartKey means to start from the beginning.
// An empty EndKey means to include everything (no ending).
// A Region with both StartKey and EndKey set to empty represents
// the entire key space.
type Region struct {
	StartKey string
	EndKey   string
}

// Information that a server reports to the balancer periodically.
// A balancer starts and waits for servers to report their status.
// The balancer collects information regarding to which region on
// which server. After the service initialized, the balancer will
// search for under-replicated regions
type ServerStat struct {
	ServerName
	// The server's start time in milliseconds.
	UpTimestamp int64
	// Regions that are managed by the server.
	Regions []Region
}

// Region placement assigns vacant servers to under-replicated region.
type RegionPlacementAction struct {
	Region
	Source    ServerName
	Dest      ServerName
	HasSource bool
	Status    int
}

// An interface that handles RPCs from balancer to servers.
type RegionPlacementManager interface {
	Placement(task *RegionPlacementAction)
}

// Save region changes persistently.
// Region changes are normally saved in zookeeper. Related storage servers
// does not get the notification directly from load balancer. Instead,
// the initiating storage server monitors coresponding zookeeper region
// keys. If the keys are changed, the storage servers will further
// investigate if a region change (split or merge) has occured.
type BalancerStateManager interface {
	Commit(adds []Region, removals []Region)
}

// Options a caller can specify before creating a new balancer instance.
type BalancerOptions struct {
	BalancerName     string
	NumReplicas      int
	RackManager      RackManager
	PlacementManager RegionPlacementManager
	StateManager     BalancerStateManager
}

type Balancer interface {
	// Initialize a new lbase system.
	New(servers []ServerName) bool

	// Initialize an existing lbase system.
	// @storageAssignments maps each region to the list of servers
	// that stores the region;
	// @primaryAssignments maps each region to its primary (the server
	// that coordinate writes/updates).
	Reload(servers []ServerName, regions []Region) bool

	// Update server's load and status.
	UpdateServerStats(timestamp int64, stats []ServerStat)

	// Report that a server is not reachable.
	ReportOutage(server ServerName)

	// Report new servers are added.
	ReportNewServers(servers []ServerName)

	// Split a region into two new regions.
	SplitRegion(origin, left, right Region) bool

	// Merge two regions @left, and @right together.
	// @light indicate which region has less load.
	MergeRegions(left, right, light Region)
}
