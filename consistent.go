// Copyright (C) 2021 dogslee.
// Use of this source code is governed by an MIT-style license
// that can be found in the LICENSE file.

// This package provides a consistent hash implementation of consistent hash functions and custom hash methods.
//
// Consistent hashing is often used to distribute requests to a changing set of servers.  For example,
// say you have some cache servers cacheA, cacheB, and cacheC.  You want to decide which cache server
// to use to look up information on a user.
//
// You could use a typical hash table and hash the user id
// to one of cacheA, cacheB, or cacheC.  But with a typical hash table, if you add or remove a server,
// almost all keys will get remapped to different results, which basically could bring your service
// to a grinding halt while the caches get rebuilt.
//
// With a consistent hash, adding or removing a server drastically reduces the number of keys that
// get remapped.

package consistent

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Consistent is the consistency hash interface, used to hide complex implementation details
type Consistent interface {
	// Add provides the ability to add new nodes, returns an error message if the node does not exist.
	Add(string) error
	// Get returns the node corresponding to the current consistency hash
	Get(string) (string, error)
	// Del delete an already existing consistent hash node, returns an error message if the node does not exist.
	Del(string) error
}

type consistent struct {
	circle         map[uint32]string
	member         map[string]bool
	sortedHashKeys []uint32
	virtulReplicas int
	hashFunc       func(string) (uint32, error)
	keyRule        func(string, int) (string, error)
	mx             sync.RWMutex
}

// Add add a new node name for this consistent
func (c *consistent) Add(name string) error {
	c.mx.Lock()
	defer c.mx.Unlock()
	if _, ok := c.member[name]; ok {
		return fmt.Errorf("%s already existed", name)
	}
	c.member[name] = true
	for i := 0; i < c.virtulReplicas; i++ {
		rplKey, err := c.replicaKey(name, i)
		if err != nil {
			return nil
		}
		hashKey, err := c.hashKey(rplKey)
		if err != nil {
			return err
		}
		c.circle[hashKey] = name
		c.sortedHashKeys = append(c.sortedHashKeys, hashKey)
	}
	c.sortHashKeySlice()
	return nil
}

// Get returns an existing consistent hash node by node name
func (c *consistent) Get(name string) (string, error) {
	if len(c.circle) == 0 {
		return "", errors.New("consistent is nil")
	}
	hashKey, _ := c.hashKey(name)
	idx := sort.Search(len(c.sortedHashKeys), func(i int) bool {
		return c.sortedHashKeys[i] >= hashKey
	})
	if idx >= len(c.sortedHashKeys) {
		idx = 0
	}
	return c.circle[c.sortedHashKeys[idx]], nil
}

// Del delete an existing consistent hash node
func (c *consistent) Del(name string) error {
	c.mx.Lock()
	defer c.mx.Unlock()
	if _, ok := c.member[name]; !ok {
		return fmt.Errorf("%s not existed", name)
	}
	for i := 0; i < c.virtulReplicas; i++ {
		rplKey, err := c.replicaKey(name, i)
		if err != nil {
			return nil
		}
		hashKey, err := c.hashKey(rplKey)
		if err != nil {
			return err
		}

		delete(c.circle, hashKey)
	}
	// delete the hash value of a virtual Replica
	{
		c.sortedHashKeys = c.sortedHashKeys[:0]
		if cap(c.sortedHashKeys)/(c.virtulReplicas*4) > len(c.circle) {
			c.sortedHashKeys = nil
		}
		for v := range c.circle {
			c.sortedHashKeys = append(c.sortedHashKeys, v)
		}
		// reset sort this hashkey slice
		c.sortHashKeySlice()
	}
	delete(c.member, name)
	return nil
}

// sortHashKeySlice sort hash slice data
func (c *consistent) sortHashKeySlice() {
	sort.Slice(c.sortedHashKeys, func(i, j int) bool {
		return c.sortedHashKeys[i] < c.sortedHashKeys[j]
	})
}

// hashKey hash a string default used CRC-32
// the hash function can be set manually using the opertaion method
func (c *consistent) hashKey(key string) (uint32, error) {
	if c.hashFunc == nil {
		return c.defaultHashFunc(key)
	}
	return c.hashFunc(key)
}

// replicaKey replicators are called generators
// the function can be set manually using the opertaion method
func (c *consistent) replicaKey(key string, idx int) (string, error) {
	if c.keyRule == nil {
		return c.defaultKeyRule(key, idx)
	}
	return c.keyRule(key, idx)
}

// defaultHashFunc CRC-32
func (c *consistent) defaultHashFunc(key string) (uint32, error) {
	return crc32.ChecksumIEEE([]byte(key)), nil
}

// defaultKeyRule return key#idx
func (c *consistent) defaultKeyRule(key string, idx int) (string, error) {
	ret := key + "#" + strconv.Itoa(idx)
	return ret, nil
}

// New
func New() Consistent {
	return &consistent{
		circle:         make(map[uint32]string),
		member:         make(map[string]bool),
		virtulReplicas: 100,
	}
}

// Option consistent hash setting function type
type Option func(o *consistent)

// VirtualReplicas set the number of virtual node copies. This value defaults to 100
func VirtualReplicas(n int) Option {
	return func(o *consistent) { o.virtulReplicas = n }
}

// HashFunc set string hash function operation. This function uses CRC-32 by default.
func HashFunc(f func(string) (uint32, error)) Option {
	return func(o *consistent) { o.hashFunc = f }
}

// KeyRule set virtual node name generation rules. This function is generated by default as $key+"#"+string($idx)
func KeyRule(f func(string, int) (string, error)) Option {
	return func(o *consistent) { o.keyRule = f }
}

// NewOpt returns a custom set consistency hash.
// This defines the settings including: 1.the number of virtual node copies 2.basic string hash function 3.virtual node name generation rules
func NewOpt(opts ...Option) Consistent {
	opertaion := consistent{
		circle: make(map[uint32]string),
		member: make(map[string]bool),
	}
	for _, o := range opts {
		o(&opertaion)
	}
	if opertaion.virtulReplicas != 0 {
		fmt.Printf("Set virtulReplicas:%v \n", opertaion.virtulReplicas)
	} else {
		opertaion.virtulReplicas = 100
		fmt.Printf("Default virtulReplicas:100 \n")
	}
	if opertaion.hashFunc != nil {
		fmt.Println("Set hashFunc success")
	}
	if opertaion.keyRule != nil {
		fmt.Println("Set keyRule success")
	}
	return &opertaion
}
