package balancer

import (
	"container/heap"
	"math/rand"
	"reflect"
)

// Element type in a priority queue.
type Weight struct {
	value string
	count int
}

// A priority queue that DefaultBalancer uses to pick next region
// assignment.
type WeightHeap []Weight

func (h WeightHeap) Len() int {
	return len(h)
}

func (h WeightHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h WeightHeap) Less(i, j int) bool {
	// Priority queue pops the smallest value.
	return h[i].count < h[j].count
}

func (h *WeightHeap) Push(x interface{}) {
	val := x.(Weight)
	*h = append(*h, val)
}

func (h *WeightHeap) Pop() interface{} {
	last := len(*h) - 1
	ret := (*h)[last]
	(*h) = (*h)[:last]
	return ret
}

func (h *WeightHeap) Update(value string, delta int) {
	for idx, w := range *h {
		if w.value == value {
			w.count = w.count + delta
			(*h)[idx] = w
			heap.Fix(h, idx)
			return
		}
	}

	panic("value is not in heap!")
}

type DefaultBalancer struct {
	// Maps servers to the regions that they manages.
	serverMap map[ServerName][]Region
	// Maps a host to servers running in the host.
	hostMap map[string][]ServerName
	// Maps regions to the servers.
	regionMap map[Region][]ServerName
	// Use this priority queue to pick the best rack for a placement.
	globalQueue WeightHeap
	// Use this priority queue to pick best host within a rack.
	perRackQueue map[string]WeightHeap
	// How many servers form a region.
	numReplicas int
	// Various options for the balancer.
	opts *BalancerOptions
}

// Create a brand new balancer for a brand new system.
func NewDefaultBalancer(opts *BalancerOptions, servers []ServerName) *DefaultBalancer {
	b := &DefaultBalancer{
		serverMap:    make(map[ServerName][]Region),
		hostMap:      make(map[string][]ServerName),
		regionMap:    make(map[Region][]ServerName),
		perRackQueue: make(map[string]WeightHeap),
		opts:         opts,
	}

	for _, s := range servers {
		// Populate server map.
		b.serverMap[s] = []Region{}

		// Populate host map.
		list := b.hostMap[s.Host]
		list = append(list, s)
		b.hostMap[s.Host] = list

		// Populate globalQueue
		r := b.opts.RackManager.GetRack(s.Host)
		_, found := b.perRackQueue[r]
		if !found {
			w := Weight{value: r, count: 0}
			b.globalQueue = append(b.globalQueue, w)
		}

		// Populate per rack queue.
		h := b.perRackQueue[r]
		w := Weight{value: s.Host, count: 0}
		h = append(h, w)
		b.perRackQueue[r] = h
	}

	heap.Init(&b.globalQueue)
	for _, h := range b.perRackQueue {
		heap.Init(&h)
	}

	return b
}

// Create a new balancer that manages an existing system.
func ReloadDefaultBalancer(
	opts *BalancerOptions,
	servers []ServerName,
	regions []Region) *DefaultBalancer {

	b := NewDefaultBalancer(opts, servers)
	if b != nil {
		for _, r := range regions {
			b.regionMap[r] = []ServerName{}
		}
	}

	return b
}

func (b *DefaultBalancer) UpdateServerStats(
	timestamp int64,
	stats []ServerStat) {

	// First clean up existing data.
	for server, _ := range b.serverMap {
		b.serverMap[server] = []Region{}
	}

	for region, _ := range b.regionMap {
		b.regionMap[region] = []ServerName{}
	}

	b.globalQueue = WeightHeap{}
	for rack, _ := range b.perRackQueue {
		b.perRackQueue[rack] = WeightHeap{}
	}

	// Then build server map.
	for _, s := range stats {
		b.serverMap[s.ServerName] = s.Regions
	}

	// Then build the region map (reverse map).
	for server, regions := range b.serverMap {
		for _, r := range regions {
			servers := b.regionMap[r]
			servers = append(servers, server)
			b.regionMap[r] = servers
		}
	}

	// Build global and per rack queue.
	rackMap := make(map[string]int)
	for server, regions := range b.serverMap {
		rack := b.opts.RackManager.GetRack(server.Host)
		cnt := rackMap[rack]
		delta := len(regions)
		cnt = cnt + delta
		rackMap[rack] = cnt

		queue := b.perRackQueue[rack]
		w := Weight{value: server.Host, count: delta}
		queue = append(queue, w)
		b.perRackQueue[rack] = queue
	}

	// Finalize the queues.
	for r, cnt := range rackMap {
		w := Weight{value: r, count: cnt}
		b.globalQueue = append(b.globalQueue, w)
	}

	heap.Init(&b.globalQueue)

	for r, q := range b.perRackQueue {
		heap.Init(&q)
		b.perRackQueue[r] = q
	}

	// Create the first region if we have not done so yet.
	if len(b.regionMap) == 0 {
		// First pick N replicas from different racks.
		racks := make([]Weight, b.numReplicas)
		for i := 0; i < b.numReplicas; i++ {
			if len(b.globalQueue) > 0 {
				tmp := heap.Pop(&b.globalQueue)
				racks[i] = tmp.(Weight)
			} else if i > 0 {
				// If we run out of racks,
				// try to reuse last available one.
				racks[i] = racks[i-1]
			} else {
				// This should be rare.
				panic("No resource left!")
			}

			// If the rack does not have any hosts, try another one.
			r := racks[i].value
			slist, found := b.perRackQueue[r]
			if !found || len(slist) == 0 {
				i--
				continue
			}
		}

		// Then pick a server from each of rack.
		servers := make([]ServerName, b.numReplicas)
		for i := 0; i < b.numReplicas; i++ {
			r := racks[i].value
			q, found := b.perRackQueue[r]
			if !found {
				panic("rack is not found")
			}

			// Adjust per rack counter.
			w := heap.Pop(&q).(Weight)
			w.count = w.count + 1
			heap.Push(&q, w)
			b.perRackQueue[r] = q

			host := w.value
			var slist []ServerName
			slist, found = b.hostMap[host]
			if !found {
				panic("Miss a host in host map!")
			}
			idx := rand.Int31() % int32(len(slist))
			servers = append(servers, slist[idx])
		}

		// Adjust globalQueue weights.
		rackCountMap := make(map[string]int)
		for _, rack := range racks {
			cnt, found := rackCountMap[rack.value]
			if !found {
				rackCountMap[rack.value] = rack.count + 1
			} else {
				rackCountMap[rack.value] = cnt + 1
			}
		}
		for name, cnt := range rackCountMap {
			w := Weight{value: name, count: cnt}
			heap.Push(&b.globalQueue, w)
		}

		// Adjust serverMap.
		var r1 Region
		for _, s := range servers {
			list, found := b.serverMap[s]
			if !found {
				panic("server is not found")
			}
			list = append(list, r1)
			b.serverMap[s] = list
		}

		// Adjust regionMap.
		b.regionMap[r1] = servers

		// Notify servers of the change.
		for _, s := range servers {
			act := RegionPlacementAction{
				Region:    r1,
				Dest:      s,
				HasSource: false,
			}
			b.opts.PlacementManager.Placement(&act)
		}

		// Save change permanently.
		adds := []Region{r1}
		var removals []Region
		b.opts.StateManager.Commit(adds, removals)
	}
}

func (b *DefaultBalancer) ReportOutage(server ServerName) {
	// Find out affected regions, and update serverMap.
	regions, found := b.serverMap[server]
	if !found {
		return
	}
	b.serverMap[server] = []Region{}

	// We do not update hostMap for unavailable servers,
	// because the outage is considered temporary.

	// Update regionMap.
	for _, r := range regions {
		list, found := b.regionMap[r]
		if found {
			for i := 0; i < len(list); i++ {
				if list[i] == server {
					x := list[:i]
					y := list[(i + 1):]
					b.regionMap[r] = append(x, y...)
				}
			}
		}
	}

	// Update queues.
	rack := b.opts.RackManager.GetRack(server.Host)
	b.globalQueue.Update(rack, -len(regions))

	q, found := b.perRackQueue[rack]
	if found {
		q.Update(server.Host, -len(regions))
	}
}

func (b *DefaultBalancer) ReportNewServers(servers []ServerName) {
	// Add new servers to server map and hostMap.
	excludes := make(map[ServerName]int)
	for _, s := range servers {
		_, found := b.serverMap[s]
		if found {
			excludes[s] = 1
			continue
		}
		b.serverMap[s] = []Region{}

		slist := b.hostMap[s.Host]
		slist = append(slist, s)
		b.hostMap[s.Host] = slist
	}

	// Update queues.
	for _, s := range servers {
		_, found := excludes[s]
		if found {
			continue
		}

		rack := b.opts.RackManager.GetRack(s.Host)
		var q WeightHeap
		q, found = b.perRackQueue[rack]

		w := Weight{value: s.Host, count: 0}
		if !found {
			q = append(q, w)
			heap.Init(&q)
			// Update global queue as well.
			w2 := Weight{value: rack, count: 0}
			heap.Push(&b.globalQueue, w2)
		} else {
			heap.Push(&q, w)
		}
		b.perRackQueue[rack] = q
	}
}

func (b *DefaultBalancer) SplitRegion(origin, left, right Region) bool {
	// First find out all affected servers
	slist, found := b.regionMap[origin]
	if !found || left.EndKey != right.StartKey {
		return false
	}

	// Adjust regionMap.
	delete(b.regionMap, origin)
	b.regionMap[left] = slist
	b.regionMap[right] = slist

	// Adjust serverMap.
	for _, s := range slist {
		rlist, found := b.serverMap[s]
		if !found {
			panic("server not found in serverMap!")
		}

		found = false
		for i := 0; i < len(rlist); i++ {
			if rlist[i] == origin {
				rlist = append(rlist[:i], rlist[(i+1):]...)
				rlist = append(rlist, left, right)
				b.serverMap[s] = rlist
				found = true
				break
			}
		}

		if !found {
			panic("serverMap and regionMap out of sync!")
		}

		b.serverMap[s] = rlist
	}

	// Adjust globalQueue and perRackQueue.
	for _, s := range slist {
		rack := b.opts.RackManager.GetRack(s.Host)
		for i := 0; i < len(b.globalQueue); i++ {
			if b.globalQueue[i].value == rack {
				b.globalQueue[i].count = b.globalQueue[i].count + 1
				heap.Fix(&b.globalQueue, i)
				break
			}
		}

		q, found := b.perRackQueue[rack]
		if !found {
			panic("Fails to find the rack!")
		}
		for i := 0; i < len(q); i++ {
			if q[i].value == s.Host {
				q[i].count = q[i].count + 1
				heap.Fix(&q, i)
				break
			}
		}
	}

	// Save region changes.
	adds := []Region{left, right}
	removals := []Region{origin}
	b.opts.StateManager.Commit(adds, removals)

	return true
}

func (b *DefaultBalancer) MergeRegions(left, right, light Region) {
	if left.EndKey != right.StartKey {
		panic("Bad left and right key!")
	}
	if light != left && light != right {
		panic("@light much be either @left or @right!")
	}
	if !b.hasSameReplications(left, right) {
		b.moveRegionForMerge(left, right, light)
		return
	}

	newRegion := Region{StartKey: left.StartKey, EndKey: right.EndKey}

	// Both regions are on the same set of replicas.
	// First adjust serverMap.
	slist, _ := b.regionMap[left]
	for _, s := range slist {
		rlist, found := b.serverMap[s]
		if !found {
			panic("Fails to find expected server!")
		}
		for i := 0; i < len(rlist); i++ {
			if rlist[i] == left {
				x := rlist[:i]
				y := rlist[(i + 1):]
				rlist = append(x, y...)
				break
			}
		}
		for i := 0; i < len(rlist); i++ {
			if rlist[i] == right {
				x := rlist[:i]
				y := rlist[(i + 1):]
				rlist = append(x, y...)
				break
			}
		}
		rlist = append(rlist, newRegion)
		b.serverMap[s] = rlist
	}

	// Adjust regionMap.
	delete(b.regionMap, left)
	delete(b.regionMap, right)
	b.regionMap[newRegion] = slist

	// Adjust globalQueue and perRackQueue.
	for _, s := range slist {
		rack := b.opts.RackManager.GetRack(s.Host)
		for i := 0; i < len(b.globalQueue); i++ {
			if b.globalQueue[i].value == rack {
				b.globalQueue[i].count = b.globalQueue[i].count - 1
				heap.Fix(&b.globalQueue, i)
				break
			}
		}

		q, found := b.perRackQueue[rack]
		if !found {
			panic("Fails to find the rack!")
		}
		for i := 0; i < len(q); i++ {
			if q[i].value == s.Host {
				q[i].count = q[i].count - 1
				heap.Fix(&q, i)
				break
			}
		}
	}

	// Save region changes.
	adds := []Region{newRegion}
	removals := []Region{left, right}
	b.opts.StateManager.Commit(adds, removals)
}

func (b *DefaultBalancer) hasSameReplications(left, right Region) bool {
	rlist1, found := b.regionMap[left]
	if !found {
		panic("Fails to find a region")
	}
	var rlist2 []ServerName
	rlist2, found = b.regionMap[right]
	if !found {
		panic("Fails to find a region")
	}
	if len(rlist1) != len(rlist2) {
		return false
	}

	// Use map to compare two sets of list.
	tmp1 := make(map[ServerName]int)
	tmp2 := make(map[ServerName]int)

	for _, s := range rlist1 {
		tmp1[s] = 1
	}
	for _, s := range rlist2 {
		tmp2[s] = 1
	}

	return reflect.DeepEqual(tmp1, tmp2)
}

func (b *DefaultBalancer) moveRegionForMerge(left, right, light Region) {
	var bigger *Region
	var smaller *Region
	if left == light {
		bigger, smaller = &right, &left
	} else {
		bigger, smaller = &left, &right
	}

	smallerList, _ := b.regionMap[*smaller]
	biggerList, _ := b.regionMap[*bigger]

	// Use map to dedupe.
	smallerMap := make(map[ServerName]int)
	biggerMap := make(map[ServerName]int)
	for _, s := range smallerList {
		smallerMap[s] = 1
	}
	for _, s := range biggerList {
		biggerMap[s] = 1
	}

	for server, _ := range biggerMap {
		_, found := smallerMap[server]
		if found {
			delete(smallerMap, server)
			delete(biggerMap, server)
		}
	}

	// Convert deduped map back into list.
	smallerList = []ServerName{}
	biggerList = []ServerName{}
	for server, _ := range smallerMap {
		smallerList = append(smallerList, server)
	}
	for server, _ := range biggerMap {
		biggerList = append(biggerList, server)
	}

	// Send move instruction.
	for i := 0; i < len(biggerList); i++ {
		act := RegionPlacementAction{
			Region:    *smaller,
			Source:    smallerList[i],
			Dest:      biggerList[i],
			HasSource: true,
		}
		b.opts.PlacementManager.Placement(&act)
	}
}
