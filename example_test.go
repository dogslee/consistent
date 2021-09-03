package consistent_test

import (
	"fmt"
	"hash/fnv"
	"log"
	"strconv"

	"github.com/dogslee/consistent"
)

func ExampleNew() {
	// default consistent hash function
	c := consistent.New()
	// add new node
	c.Add("node1")
	c.Add("node2")
	c.Add("node3")
	c.Add("node4")
	keyCase := []string{"user1", "user2", "user3", "user4"}
	for _, k := range keyCase {
		srvNode, err := c.Get(k)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("key: %s ==> srvNode: %s", k, srvNode)
	}
	// Output:
	// key: user1 ==> srvNode: node2
	// key: user2 ==> srvNode: node2
	// key: user3 ==> srvNode: node2
	// key: user4 ==> srvNode: node3
}

func ExampleNewOpt() {
	// custom consistent hash functions
	c := consistent.NewOpt(
		// set virtual node
		consistent.VirtualReplicas(50),
		// set hashFunc
		consistent.HashFunc(func(key string) (uint32, error) {
			h := fnv.New32a()
			h.Write([]byte(key))
			return h.Sum32(), nil
		}),
		// set gen key rule
		consistent.KeyRule(func(key string, idx int) (string, error) {
			return key + strconv.Itoa(idx), nil
		}))
	c.Add("node1")
	c.Add("node2")
	c.Add("node3")
	c.Add("node4")
	keyCase := []string{"user10", "user20", "user30", "user40"}

	for _, k := range keyCase {
		srvNode, err := c.Get(k)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("key: %s ==> srvNode: %s\n", k, srvNode)
	}
	// Output:
	// key: user10 ==> srvNode: node3
	// key: user20 ==> srvNode: node1
	// key: user30 ==> srvNode: node2
	// key: user40 ==> srvNode: node2
}
