package ConsistentHash

import (
	"sort"
	"strconv"
	"testing"
)

func TestNewConsistentHash(t *testing.T) {
	c := NewConsistentHash()
	if c == nil {
		t.Error("InitConsistentHash failed.")
	}
}

func TestConsistentHash_AddNode(t *testing.T) {
	c := NewConsistentHash()
	c.AddNode("10.140.104.15")
	if len(c.ring) != 20 || len(c.sortedHashes) != 20 {
		t.Error("number of Cubes is wrong")
	}
	if sort.IsSorted(c.sortedHashes) == false {
		t.Error("HashRing is not sorted ")
	}
}

func TestConsistentHash_AddNodes(t *testing.T) {
	c := NewConsistentHash()
	length := 5
	nodes := make([]string, length)
	for i := 1; i < 6; i++ {
		elem := "10.140.104." + strconv.Itoa(i)
		nodes = append(nodes, elem)
	}
	c.AddNodes(nodes)
	if sort.IsSorted(c.sortedHashes) == false {
		t.Error("HashRing is not sorted")
	}
}

func TestConsistentHash_RemoveNode(t *testing.T) {
	c := NewConsistentHash()
	c.AddNode("10.140.104.6")
	c.RemoveNode("10.140.104.6")
	if len(c.ring) != 0 || len(c.sortedHashes) != 0 {
		t.Error("number of Cubes is wrong")
	}
}
