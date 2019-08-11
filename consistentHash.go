package ConsistentHash

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

var GConsistentHash *ConsistentHash

type uint32Type []uint32

/**
implementation of Sort Interface methods
*/
func (x uint32Type) Len() int {
	return len(x)
}

func (x uint32Type) Less(i, j int) bool {
	return x[i] < x[j]
}

func (x uint32Type) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

/**
Hash Ring capacity is 2^32, index from 0 - 2^32-1, so we use uint32
*/
type ConsistentHash struct {
	ring           map[uint32]string
	members        map[string]bool
	numberOfVNodes int
	sortedHashes   []uint32
	count          int32
	sync.RWMutex
}

func NewConsistentHash() *ConsistentHash {
	GConsistentHash = &ConsistentHash{
		ring:           make(map[uint32]string),
		members:        make(map[string]bool),
		numberOfVNodes: 20,
		sortedHashes:   nil,
		count:          0,
		RWMutex:        sync.RWMutex{},
	}
	return GConsistentHash
}

func GetGConsistentHash() *ConsistentHash {
	if GConsistentHash != nil {
		return GConsistentHash
	}
	return NewConsistentHash()
}

func (c *ConsistentHash) generateKey(ip string, index int) string {
	return ip + "#" + strconv.Itoa(index)
}

//based on the generate key above
func (c *ConsistentHash) generateHash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *ConsistentHash) SetVNodeNumber(num int) (err error) {
	if len(GConsistentHash.members) != 0 {
		err = errors.New("can't modify number of Vnode when the len(members) > 0,to avoid data migration")
		return
	}
	if num <= 0 {
		err = errors.New("ConsistentHash Vnodes must be more than 0,to balance the workload")
		return
	}
	c.numberOfVNodes = num
	err = nil
	return
}

func (c *ConsistentHash) AddNode(ip string) {
	c.Lock()
	defer c.Unlock()

	for i := 0; i < c.numberOfVNodes; i++ {
		c.ring[c.generateHash(c.generateKey(ip, i))] = ip
	}

	c.members[ip] = true
	c.updateSortedConsistentHash()
	c.count++
}

func (c *ConsistentHash) updateSortedConsistentHash() {
	hashes := uint32Type{}
	for k := range c.ring {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	c.sortedHashes = hashes
}
