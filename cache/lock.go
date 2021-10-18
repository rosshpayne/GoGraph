package cache

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	blk "github.com/GoGraph/block"
	"github.com/GoGraph/db"
	elog "github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/util"
)

// FetchBatch reads the cUIDs within an overflow block batch.
// Does not read from cache but uses cache as the lock entity.
// i - overflow block. 1..n. 0 is embedded uid-pred which does not execute FetchBatch()
// bid - overflow batch. 1..n
// ouid - overflow block uid
func (g *GraphCache) FetchBatch(i int, bid int64, ouid util.UID, wg *sync.WaitGroup, sortk string, bCh chan<- BatchPy) {

	defer wg.Done()

	g.Lock()
	ouids := ouid.String()
	e := g.cache[ouids]
	syslog(fmt.Sprintf("FetchBatch: i %d bid %d ouid %s  sortk: %s", i, bid, ouid.String(), sortk))
	// e is nill when UID not in cache (map), e.NodeCache is nill when node cache is cleared.
	if e == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[ouids] = e
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		g.Unlock()
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	e.Lock()
	var (
		sk string
	)
	for b := 1; b < int(bid)+1; b++ {
		sk = sortk + "%" + strconv.Itoa(b)
		nb, err := db.FetchNode(ouid, sk)
		if err != nil {
			elog.Add("FetchBatch: ", err)
			break
		}
		syslog(fmt.Sprintf("FetchBatch: ouid %s  b, sk: %d %s len(nb) %d ", ouid, b, sk, len(nb)))
		bCh <- BatchPy{Bid: i, Batch: b, Puid: ouid, DI: nb[0]}
	}
	close(bCh)
	e.Unlock()

}

// FetchUOB is redundant (most likely)
// concurrently fetches complete Overflow Block for each UID-PRED OUID entry.
// Called from cache.UnmarshalNodeCache() for Nd attribute only.
// TODO: need to decide when to release lock.
func (g *GraphCache) FetchUOB(ouid util.UID, wg *sync.WaitGroup, ncCh chan<- *NodeCache) {
	var sortk_ string

	defer wg.Done()

	sortk_ = "A#G#" // complete block - header items +  propagated scalar data belonging to Overflow block
	uid := ouid.String()

	g.Lock()
	e := g.cache[uid] // e will be nil if uids not in map

	if e == nil || e.NodeCache == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uid] = e
		g.Unlock()
		// nb: type blk.NodeBlock []*DataIte
		nb, err := db.FetchNode(ouid, sortk_)
		if err != nil {
			return
		}
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		en := e.NodeCache
		en.Uid = ouid
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// lock node cache.
	//
	e.RLock()
	// check if cache has been cleared while waiting to acquire lock, so try again
	if e.NodeCache == nil {
		g.FetchUOB(ouid, wg, ncCh)
	}
	var cached bool
	// check sortk is cached
	for k := range e.m {
		if strings.HasPrefix(k, sortk_) {
			cached = true
			break
		}
	}
	if !cached {
		e.Unlock()
		e.dbFetchSortK(sortk_)
		ncCh <- e.NodeCache
		return
	}
	// e.Unlock() - unlocked in cache.UnmarshalNodeCache
	fmt.Println("In FetchUOB..about to write to channel ncCH...", len(e.NodeCache.m))
	ncCh <- e.NodeCache
	fmt.Println("In FetchUOB..about to exit...")
}

// TODO : is this used. Nodes are locked when "Fetched"
func (g *GraphCache) LockNode(uid util.UID) {

	fmt.Printf("** Cache LockNode  Key Value: [%s]\n", uid.String())

	g.Lock()
	uids := uid.String()
	e := g.cache[uids]

	if e == nil {
		e = &entry{ready: make(chan struct{})}
		e.NodeCache = &NodeCache{}
		g.cache[uids] = e
		g.Unlock()
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// lock e . Note: e can only be acquired from outside of this package via the Fetch* api.
	//
	e.Lock()

}

// FetchForUpdate is used as a substitute for database lock. Provided all db access is via this API (or similar) then all updates will be serialised preventing
// mutliple concurrent updates from corrupting each other. It also guarantees consistency between cache and storage copies of the data.
// FetchForUpdate performs a Query so returns multiple items. Typically we us this to access all scalar node attribute (e.g. sortk of "A#A#")
// or all uid-pred and its propagated data (e.g. "A#:G")
func (g *GraphCache) FetchForUpdate(uid util.UID, sortk ...string) (*NodeCache, error) {
	var (
		sortk_ string
	)
	//
	//	g lock protects global cache with UID key
	//
	g.Lock()
	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = "A#"
	}

	slog.Log("FetchForUpdate: ", fmt.Sprintf("** Cache FetchForUpdate Cache Key Value: [%s]   sortk: %s", uid.String(), sortk_))
	e := g.cache[uid.String()]
	// e is nill when UID not in cache (map), e.NodeCache is nill when node cache is cleared.
	if e == nil || e.NodeCache == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uid.String()] = e
		g.Unlock()
		// nb: type blk.NodeBlock []*DataItem
		nb, err := db.FetchNode(uid, sortk_)
		if err != nil {
			slog.Log("FetchForUpdate: ", fmt.Sprintf("db fetchnode error: %s", err.Error()))
			return nil, err
		}
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		en := e.NodeCache
		en.Uid = uid
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// e lock protects Node Cache with sortk key.
	// lock e to prevent updates from other routines. Must explicitly Unlock() some stage later.
	//  Note: e can only be acquired from outside of this package via the Fetch* api.
	//
	e.Lock()
	// check if cache has been cleared while waiting to acquire lock, so try again
	if e.NodeCache == nil {
		g.FetchForUpdate(uid, sortk_)
	}
	e.fullLock = true
	// check sortk is cached
	// TODO: check only looks for one sortk prefix entry when
	// a node would typically have many. Need to sophisticated check ideal check each of the node predicate's
	// are cached.
	var cached bool
	for k := range e.m {
		if strings.HasPrefix(k, sortk_) {
			cached = true
			break
		}
	}
	if !cached {
		// perform a db fetch of sortk
		e.dbFetchSortK(sortk_)
	}
	return e.NodeCache, nil
}

// FetchUIDpredForUpdate is called as part of attach node processing. It performs a GetItem on the uid-pred attribute only. The cache copy will be
// read and updated according to its contents. The cache copy will then be written to storage (API: SaveCompleteUpred*()) before associated lock is released.
// At that point the cache and storage are in sync. Accessing the cache copy is valid from that point on.
func (g *GraphCache) FetchUIDpredForUpdate(uid util.UID, sortk string) (*NodeCache, error) {
	//
	//	g lock protects global cache
	//
	g.Lock()
	slog.Log("FetchUIDpredForUpdate: ", fmt.Sprintf("Acquired g.Lock(). FetchUIDpredForUpdate Cache Key Value: [%s]   sortk: %s", uid.String(), sortk))
	e := g.cache[uid.String()]
	// e is nill when UID not in cache (map), e.NodeCache is nill when node cache is cleared.
	if e == nil || e.NodeCache == nil {

		e = &entry{ready: make(chan struct{})}
		g.cache[uid.String()] = e
		g.Unlock()
		// nb: type blk.NodeBlock []*DataItem
		nb, err := db.FetchNodeItem(uid, sortk)
		if err != nil {
			slog.Log("FetchUIDpredForUpdate: ", fmt.Sprintf("db fetchnode error: %s", err.Error()))
			return nil, err
		}
		// create a NodeCache and populate with fetched blk.NodeBlock
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		en := e.NodeCache
		en.Uid = uid
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// e lock protects NodeCache.m
	// given we are about to read and potentially update m we must first get a lock
	// note: in this case the cache is acting a database lock on the node cache.
	// lock must be explicitly unlocked in calling routine
	//
	e.Lock()
	// check ifcache has been cleared while waiting to acquire lock..try again
	if e.NodeCache == nil {
		g.FetchUIDpredForUpdate(uid, sortk)
	}
	e.fullLock = true
	// check uid-pred is cached, if not fetch from db and add to cache
	if _, ok := e.m[sortk]; !ok {
		slog.Log("FetchUIDpredForUpdate: ", fmt.Sprintf("About to db.FetchNodeItem() for %s %s", uid, sortk))
		nb, err := db.FetchNodeItem(uid, sortk)
		if err != nil {
			slog.Log("FetchUIDpredForUpdate: ", fmt.Sprintf("db fetchnode error: %s", err.Error()))
			return nil, err
		}
		// add uid-pred item to node cache m
		for _, v := range nb {
			e.m[v.Sortk] = v
		}
	} else {
		slog.Log("FetchUIDpredForUpdate: ", fmt.Sprintf("uidPred is already cached. %s %s", uid, sortk))
	}
	return e.NodeCache, nil
}

// FetchNodeNonCache will perform a db fetch for each execution.
// Why? For testing purposes it's more realistic to access non-cached node data.
// This API is used in GQL testing.
// TODO: rename to FetchNodeNoCache
func (g *GraphCache) FetchNodeNonCache(uid util.UID, sortk ...string) (*NodeCache, error) {
	// 	if len(sortk) > 0 {
	// 		return g.FetchNode(uid, sortk...)
	// 	}
	// 	return g.FetchNode(uid)
	// }

	var sortk_ string

	g.Lock()
	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = "A#"
	}
	uids := uid.String()
	e := g.cache[uids]
	//
	// force db read by setting e to nil
	//
	e = nil
	//
	if e == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uids] = e
		g.Unlock()
		// nb: type blk.NodeBlock []*DataIte
		nb, err := db.FetchNode(uid, sortk_)
		if err != nil {
			return nil, err
		}

		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		en := e.NodeCache
		en.Uid = uid
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	e.Lock()
	// check ifcache has been cleared while waiting to acquire lock..try again
	e.fullLock = true
	return e.NodeCache, nil
}

// FetchNode complete or a subset of node data (for SORTK value) into cache.
// Performs lock for sync (shared resource) and transactional perhpas.
// If cache is already populated with Node data checks SORTK entry exists in cache - if not calls dbFetchSortK().
// Lock is held long enough to read cached result then unlocked in calling routine.
func (g *GraphCache) FetchNode(uid util.UID, sortk ...string) (*NodeCache, error) {
	var sortk_ string

	g.Lock()
	if len(sortk) > 0 {
		sortk_ = sortk[0]
	} else {
		sortk_ = "A#"
	}
	uids := uid.String()
	e := g.cache[uids]
	// e is nill when UID not in cache (map), e.NodeCache is nill when node cache is cleared.
	if e == nil || e.NodeCache == nil {
		e = &entry{ready: make(chan struct{})}
		g.cache[uids] = e
		g.Unlock()
		// nb: type blk.NodeBlock []*DataIte
		nb, err := db.FetchNode(uid, sortk_)
		if err != nil {
			return nil, err
		}
		e.NodeCache = &NodeCache{m: make(map[SortKey]*blk.DataItem), gc: g}
		en := e.NodeCache
		en.Uid = uid
		for _, v := range nb {
			en.m[v.Sortk] = v
		}
		close(e.ready)
	} else {
		g.Unlock()
		<-e.ready
	}
	//
	// lock node cache.
	//
	e.Lock()
	// check if cache has been cleared while waiting to acquire lock, so try again
	if e.NodeCache == nil {
		g.FetchNode(uid, sortk_)
	}
	e.fullLock = true
	// check sortk is cached
	var cached bool
	for k := range e.m {
		if strings.HasPrefix(k, sortk_) {
			cached = true
			break
		}
	}
	if !cached {
		// perform a db fetch of sortk
		e.dbFetchSortK(sortk_)
	}
	//e.Unlock()- unlocked in calling routine after read of node data

	return e.NodeCache, nil
}

//dbFetchSortK loads sortk attribute from database and enters into cache
func (nc *NodeCache) dbFetchSortK(sortk string) error {

	slog.Log("dbFetchSortK: ", fmt.Sprintf("dbFetchSortK for %s UID: [%s] \n", sortk, nc.Uid.String()))
	nb, err := db.FetchNode(nc.Uid, sortk)
	if err != nil {
		return err
	}
	// add data items to node cache
	for _, v := range nb {
		nc.m[v.Sortk] = v
	}

	return nil
}

func (n *NodeCache) ClearCache(sortk string, subs ...bool) error {

	slog.Log("ClearCache: ", fmt.Sprintf("sortk %s", sortk))
	//
	// check if node is cached
	//
	delete(n.m, sortk)
	// if clear all sub-sortk's
	if len(subs) > 0 && subs[0] {
		for k := range n.m {
			if strings.HasPrefix(k, sortk) {
				delete(n.m, k)
			}
		}
	}

	return nil
}

func (n *NodeCache) ClearNodeCache(sortk ...string) error {
	if len(sortk) > 0 {
		return n.gc.ClearNodeCache(n.Uid, sortk[0])
	} else {
		return n.gc.ClearNodeCache(n.Uid)
	}
}
func (g *GraphCache) ClearNodeCache(uid util.UID, sortk ...string) error {
	//
	// check if node is cached
	//
	var (
		// ty  string
		// tab blk.TyAttrBlock
		ok bool
		e  *entry
	)

	g.Lock()
	defer g.Unlock()

	if e, ok = g.cache[uid.String()]; !ok {
		slog.Log("Unlock: ", "Nothing to clear")
		return nil
	}
	nc := e.NodeCache
	delete(g.cache, uid.String())

	if len(sortk) > 0 {
		//
		// optional: remove overflow blocks for supplied sortk (UID-PRED) they exist
		//
		for _, uid := range nc.GetOvflUIDs(sortk[0]) {
			if _, ok = g.cache[uid.String()]; ok {
				// delete map entry will mean e is unassigned and allow GC to purge e and associated node cache.
				delete(g.cache, uid.String())
			}
		}
	}
	//
	// clear NodeCache forcing any waiting readers on uid node to refresh from db
	//
	e.NodeCache.m = nil
	e.NodeCache = nil

	return nil
}

// Unlock method shadows the RWMutex Unlock
func (nd *NodeCache) Unlock(s ...string) {

	// if len(s) > 0 {
	// 	slog.Log("Unlock: ", fmt.Sprintf("******* IN UNLOCK NC ********************  %s", s[0]))
	// } else {
	// 	slog.Log("Unlock: ", "******* IN UNLOCK NC ********************")
	// }

	if nd == nil {
		return
	}

	if nd.m != nil && len(nd.m) == 0 {
		// locked by LockNode() - without caching daa
		slog.Log("Unlock: ", "Success RWMutex.Unlock() len(nd.m)=0")
		nd.RWMutex.Unlock()
		return
	}
	//
	if nd.fullLock {
		//
		// Locked for update - full lock
		//
		nd.fullLock = false
		nd.RWMutex.Unlock()
		slog.Log("Unlock: ", "Success Unlock()")

	} else {
		//
		//	Read Lock
		//
		nd.RWMutex.RUnlock()
		//slog.Log("Unlock: ", "Success RUnlock()")
		slog.Log("Unlock: ", "Success Unlock()")
		//nd.locked = false
	}
}

// // ClearOverflowCache: remove overflow blocks from the cache
// func (g *GraphCache) ClearOverflowCache(uid util.UID) error {

// 	fmt.Println()
// 	fmt.Println("================================ CLEAR NODE CACHE =======================================")
// 	fmt.Printf(" Clear any Overflow caches for: %s", uid.String())
// 	fmt.Println()
// 	//
// 	// check if node is cached
// 	//
// 	var (
// 		ty  string
// 		tab blk.TyAttrBlock
// 		ok  bool
// 	)
// 	g.Lock()
// 	if _, ok := g.cache[uid.String()]; !ok {
// 		fmt.Println("Nothing to clear")
// 		g.Unlock()
// 		return nil
// 	}
// 	g.Unlock()
// 	//
// 	// lock node
// 	//
// 	nc, err := g.FetchForUpdate(uid)
// 	defer nc.Unlock()
// 	if err != nil {
// 		return err
// 	}
// 	//
// 	// remove any overflow blocks
// 	//
// 	// get type definition and list its uid-predicates (e.g. siblings, friends)
// 	if ty, ok = nc.GetType(); !ok {
// 		return NoNodeTypeDefinedErr
// 	}
// 	if tab, err = FetchType(ty); err != nil {
// 		return err
// 	}

// 	for _, c := range tab.GetUIDpredC() {
// 		sortk := "A#G#:" + c
// 		// get sortk's overflow Block UIDs if any
// 		for _, uid_ := range nc.GetOvflUIDs(sortk) {
// 			g.Lock()
// 			if _, ok = g.cache[uid_.String()]; ok {
// 				delete(g.cache, uid_.String())
// 				g.Unlock()
// 			} else {
// 				g.Unlock()
// 			}
// 		}
// 	}

// 	return nil
// }
