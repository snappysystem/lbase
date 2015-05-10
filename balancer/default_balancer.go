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
	// Use "greater than" here to get the largest value.
	return h[i].count > h[j].count
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
	// Various options for the balancer.
	opts *BalancerOptions
}

// Create a brand new balancer for a brand new system.
func NewDefaultBalancer(opts *BalancerOptions, servers []ServerName) *DefaultBalancer {
	return &DefaultBalancer{
		serverMap:    make(map[ServerName][]Region),
		hostMap:      make(map[string][]ServerName),
		regionMap:    make(map[Region][]ServerName),
		perRackQueue: make(map[string]WeightHeap),
		opts:         opts,
	}
}

func (b *DefaultBalancer) UpdateServerStats(timestamp int64, stats []ServerStat) {
	// First clean up existing data.
	b.serverMap = make(map[ServerName][]Region)
	b.hostMap = make(map[string][]ServerName)
	b.regionMap = make(map[Region][]ServerName)
	b.globalQueue = WeightHeap{}
	b.perRackQueue = make(map[string]WeightHeap)

	// Then build server map and host map.
	for _, s := range stats {
		b.serverMap[s.ServerName] = s.Regions
		slist := b.hostMap[s.ServerName.Host]
		slist = append(slist, s.ServerName)
		b.hostMap[s.ServerName.Host] = slist
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
	knownHostMap := make(map[string]int)

	for server, regions := range b.serverMap {
		rack := b.opts.RackManager.GetRack(server.Host)
		cnt := rackMap[rack]
		delta := b.opts.MaxRegionsPerServer - len(regions)
		cnt = cnt + delta
		rackMap[rack] = cnt

		_, hostAlreadyKnown := knownHostMap[server.Host]
		if !hostAlreadyKnown {
			knownHostMap[server.Host] = 1
			queue := b.perRackQueue[rack]
			w := Weight{value: server.Host, count: delta}
			queue = append(queue, w)
			b.perRackQueue[rack] = queue
		}
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
		racks := make([]Weight, b.opts.NumReplicas)
		for i := 0; i < b.opts.NumReplicas; i++ {
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
		servers := make([]ServerName, b.opts.NumReplicas)
		for i := 0; i < b.opts.NumReplicas; i++ {
			r := racks[i].value
			q, found := b.perRackQueue[r]
			if !found {
				panic("rack is not found")
			}

			// Adjust per rack counter.
			w := heap.Pop(&q).(Weight)
			w.count = w.count - 1
			heap.Push(&q, w)
			b.perRackQueue[r] = q

			host := w.value
			var slist []ServerName
			slist, found = b.hostMap[host]
			if !found {
				panic("Miss a host in host map!")
			}
			idx := rand.Int31() % int32(len(slist))
			servers[i] = slist[idx]
		}

		// Adjust globalQueue weights.
		rackCountMap := make(map[string]int)
		for _, rack := range racks {
			cnt, found := rackCountMap[rack.value]
			if !found {
				rackCountMap[rack.value] = rack.count - 1
			} else {
				rackCountMap[rack.value] = cnt - 1
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
			act := PlacementAction{
				Region:    r1,
				Dest:      s,
				HasSource: false,
			}
			b.opts.PlacementManager.Place(&act)
		}

		// Save change permanently.
		adds := []Region{r1}
		var removals []Region
		b.opts.StateManager.Commit(adds, removals)
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
	adds := []Region{newRegion}
	removals := []Region{left, right}
	b.opts.StateManager.Commit(adds, removals)
}

// TODO: If all regions are balanced, there is no need to run this.
func (b *DefaultBalancer) BalanceLoad(pendings []PlacementAction) {
	// Remember hosts that are involved in region move.
	// For bigger deployment, we want to avoid reuse hosts that are
	// already involved in data movements.
	used := make(map[string]int)
	if len(b.serverMap) > b.opts.NumServersInSmallDeployment {
		for _, act := range pendings {
			used[act.Dest.Host] = 1
			if act.HasSource {
				used[act.Source.Host] = 1
			}
		}
	}

	// Build a priority queue to find out the lest replicated regions.
	boundsMap := make(map[string]string)
	regionQueue := WeightHeap{}

	for r, slist := range b.regionMap {
		replicas := len(slist)
		if replicas < b.opts.NumReplicas {
			w := Weight{value: r.StartKey, count: -replicas}
			regionQueue = append(regionQueue, w)
			boundsMap[r.StartKey] = r.EndKey
		}
	}

	heap.Init(&regionQueue)

	for i := 0; i < b.opts.NumIterationPerBalanceRound; i++ {
		// If there is no candidates, just quit.
		if len(regionQueue) == 0 {
			break
		}

		// Find the most under-replicated region.
		w := heap.Pop(&regionQueue).(Weight)
		endKey, found := boundsMap[w.value]
		if !found {
			panic("Fails to find end key for a range!")
		}

		r := Region{StartKey: w.value, EndKey: endKey}
		slist, hasRegion := b.regionMap[r]
		if !hasRegion {
			// Maybe the region has been split or merged.
			// Anyway it is possible that a region disappears
			// over time. So let us look at the next candidate.
			i--
			continue
		}

		// Remember used hosts.
		for _, s := range slist {
			used[s.Host] = 1
		}

		// No racks are available, we cannot proceed.
		if len(b.globalQueue) == 0 {
			return
		}

		pendingRacks := make([]Weight, 0)
		var server ServerName

		for len(b.globalQueue) > 0 {
			rackWeight := heap.Pop(&b.globalQueue).(Weight)
			pendingRacks = append(pendingRacks, rackWeight)

			q, hasQueue := b.perRackQueue[rackWeight.value]
			if !hasQueue {
				panic("Fails to find the queue")
			}

			pendingHosts := make([]Weight, 0)
			for len(q) > 0 {
				hostWeight := heap.Pop(&q).(Weight)
				pendingHosts = append(pendingHosts, hostWeight)
				_, hasHost := used[hostWeight.value]
				if hasHost && (len(b.globalQueue) > 0 || len(q) > 0) {
					continue
				}

				slist, hasServerList := b.hostMap[hostWeight.value]
				if !hasServerList {
					panic("Miss a host in host map!")
				}
				if len(slist) == 0 {
					continue
				}

				idx := rand.Int31() % int32(len(slist))
				server = slist[idx]

				break
			}

			// If there is a candidate, adjust the last weight count.
			if len(server.Host) > 0 {
				last := len(pendingHosts) - 1
				cnt := pendingHosts[last].count
				pendingHosts[last].count = cnt - 1

				last = len(pendingRacks) - 1
				cnt = pendingRacks[last].count
				pendingRacks[last].count = cnt - 1
			}

			// Restore per rack queue.
			for _, hw := range pendingHosts {
				heap.Push(&q, hw)
			}
			b.perRackQueue[rackWeight.value] = q

			// Restore global queue.
			for _, w := range pendingRacks {
				heap.Push(&b.globalQueue, w)
			}

			// If we already had an allocation, break the loop.
			if len(server.Host) > 0 {
				used[server.Host] = 1
				break
			}
		}

		// If we fails to find an allocation, there is nothing we can do.
		if len(server.Host) == 0 {
			return
		}

		act := PlacementAction{
			Region:    r,
			Dest:      server,
			HasSource: false,
		}

		b.opts.PlacementManager.Place(&act)
	}
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
		act := PlacementAction{
			Region:    *smaller,
			Source:    smallerList[i],
			Dest:      biggerList[i],
			HasSource: true,
		}
		b.opts.PlacementManager.Place(&act)
	}
}
