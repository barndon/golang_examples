package main

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"path/filepath"
	"sort"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func connect() *zk.Conn {
	zks := []string{"ubuntu1:2181"}
	conn, _, err := zk.Connect(zks, time.Second)
	must(err)
	return conn
}

func wait_for_lock(write_lock string, conn *zk.Conn) {
	lock_node := filepath.Dir(write_lock)
	// There's an edge case where we may have to poll. We'll
	// use this timeout channel for that.
	timeout := make(chan bool, 1)
	for {
		// Get the list of existing lock files.  The lock names
		// are all in the form "write-lock-${SEQUENCE_NUM}", for
		// example "write-lock-0000000039", and we assume that
		// sorting the list will put them in proper locking order.
		children, _, err := conn.Children(lock_node)
		must(err)
		sort.Strings(children)

		// See if my lock is at the head of the queue...
		if (lock_node + "/" + children[0]) == write_lock {
			// Yes.  Done waiting.
			break
		} else if len(children) > 1 {
			// No, so put a watch on the next lock in the queue.
			exists, _, ch, err := conn.ExistsW(lock_node + "/" + children[1])
			must(err)
			if exists {
				// The lock we're watching may have been deleted before we
				// could put our watch on it.  Set a timer to wake us up
				// so we can try again if necessary.
				go func() {
					time.Sleep(100)
					timeout <- true
				}()
				select {
				case <-ch:
					// message received, so the lock we were watching has
					// been modified, probably deleted.  Loop around and
					// try again.
				case <-timeout:
					// This case happens when the lock we were waiting on
					// is created and destroyed before we put a watch
					// on it.  Loop around and try again.
					continue
				}
			}
		}
	}
}

func do_test(n int, c *zk.Conn, locknode string, flags int32, acl []zk.ACL, rchan chan bool) {

	// Create a lock on /my_data
	write_lock, err := c.Create(locknode+"/write-lock-", []byte("lock data..."), flags, acl)
	must(err)

	// Wait for the lock.
	fmt.Printf("[%d] I'm waiting for the lock.\n", n)
	wait_for_lock(write_lock, c)
	fmt.Printf("[%d] I have the lock. (%s)\n", n, write_lock)

	// Release the lock.
	err = c.Delete(write_lock, -1)
	fmt.Printf("[%d] I have deleted %s\n", n, write_lock)
	rchan <- true
}

func main() {
	// Create a few client connections.
	conns := []*zk.Conn{connect(), connect(), connect(), connect()}
	for _, c := range conns {
		defer c.Close()
	}

	// Our lock nodes will be ephemeral and they will have
	// sequence numbers on them so we can sort them.
	flags := int32(zk.FlagEphemeral | zk.FlagSequence)
	acl := zk.WorldACL(zk.PermAll)

	// This is the node we're going to fight to lock.  Make
	// sure it exists before we start.
	locknode := "/my_data"
	exists, _, err := conns[0].Exists(locknode)
	must(err)

	if exists == false {
		_, err := conns[0].Create(locknode, []byte("target node..."), int32(0), acl)
		must(err)
	}

	// Our test processes will signal their completion on
	// this channel.
	rchan := make(chan bool)

	// Launch the test processes.
	for n, c := range conns {
		go do_test(n, c, locknode, flags, acl, rchan)
	}

	// Wait for them to finish.
	var results []bool
	for range conns {
		results = append(results, <-rchan)
	}
}
