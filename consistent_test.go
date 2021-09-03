package consistent

import (
	"fmt"
	"hash/fnv"
	"log"
	"strconv"
	"testing"
)

func Test_Add(t *testing.T) {
	c := New()
	for i := 0; i < 100; i++ {
		c.Add(strconv.Itoa(i))
	}
	for i := 0; i < 1000; i++ {
		n, _ := c.Get("key" + strconv.Itoa(i))
		fmt.Printf("key%d => %s,", i, n)
	}

	if err := c.Add("51"); err == nil {
		t.Errorf("del error")
	}

	if err := c.Add("101"); err != nil {
		t.Errorf("del error")
	}
	fmt.Printf("\n-----\n-----\n")
	for i := 0; i < 1000; i++ {
		n, _ := c.Get("key" + strconv.Itoa(i))
		fmt.Printf("key%d => %s,", i, n)
	}
}

func Test_Get(t *testing.T) {
	c := New()
	if _, err := c.Get("somekey"); err == nil {
		t.Errorf("get error")
	}
	for i := 0; i < 100; i++ {
		c.Add(strconv.Itoa(i))
	}
	for i := 0; i < 100; i++ {
		n, _ := c.Get("key" + strconv.Itoa(i))
		fmt.Printf("key%d => %s\n", i, n)
	}
}

func Test_Del(t *testing.T) {
	c := New()
	for i := 0; i < 100; i++ {
		c.Add(strconv.Itoa(i))
	}
	for i := 0; i < 100; i++ {
		n, _ := c.Get("key" + strconv.Itoa(i))
		fmt.Printf("key%d => %s,", i, n)
	}

	if err := c.Del("xx"); err == nil {
		t.Errorf("del error")
	}

	if err := c.Del("51"); err != nil {
		t.Errorf("del error")
	}
	fmt.Printf("\n-----\n-----\n")
	for i := 0; i < 100; i++ {
		n, _ := c.Get("key" + strconv.Itoa(i))
		fmt.Printf("key%d => %s,", i, n)
	}
}

func Test_New(t *testing.T) {
	c := New()
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
		fmt.Printf("key: %s ==> srvNode: %s\n", k, srvNode)
	}

}

func Test_NewOpt(t *testing.T) {
	c := NewOpt(
		VirtualReplicas(50),
		HashFunc(func(key string) (uint32, error) {
			h := fnv.New32a()
			h.Write([]byte(key))
			return h.Sum32(), nil
		}),
		KeyRule(func(key string, idx int) (string, error) {
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
}
