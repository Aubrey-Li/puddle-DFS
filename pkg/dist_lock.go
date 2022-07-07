package pkg

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/go-zookeeper/zk"
)

const readlockPrefix = "/r-"
const writelockPrefix = "/w-"

const initlock = "/initlock"

// DistLock is a distributed lock that can be initialized with a root Zookeeper
// path and a Zookeeper connection. It can write, via the Zookeeper connection,
// to the root path.
//
// CounterClients use DistLock to acquire control of certain paths in Zookeeper
// (most importantly /counter) and prevents other CounterClients from also modifying
// the data at t path concurrently.
type DistLock struct {
	root   string // root zk path in which the lock is placed
	path   string // full zk path of the lock
	zkConn *zk.Conn
}

// CreateDistLock creates a distributed lock
func CreateDistLock(root string, zkConn *zk.Conn) *DistLock {
	dlock := &DistLock{
		root:   root,
		path:   "",
		zkConn: zkConn,
	}
	return dlock
}

func createLockNode(path string, zkConn *zk.Conn) error {
	exist, _, err := zkConn.Exists(filepath.Join(LOCK, Hash(path)))
	if err == nil && !exist {
		_, err := zkConn.Create(filepath.Join(LOCK, Hash(path)), []byte{}, 0, zk.WorldACL(zk.PermAll))
		return err
	}
	return err
}

// func initLock(zkConn *zk.Conn) {
// 	for {
// 		_, err := zkConn.Create(initlock, []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
// 		if err == nil {
// 			return
// 		}
// 		exist, _, ch, err := zkConn.ExistsW(initlock)
// 		if err != nil {
// 			panic(err)
// 		}
// 		if exist {
// 			<-ch
// 		}
// 	}
// }

// func initUnlock(zkConn *zk.Conn) {
// 	err := zkConn.Delete(initlock, -1)
// 	if err != nil {
// 		panic(err)
// 	}
// }

func (d *DistLock) ReadLock() (err error) {
	lockNode := filepath.Join(LOCK, Hash(d.root))
	d.path, err = d.zkConn.Create(lockNode+readlockPrefix, []byte{}, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	for {
		list, _, err := d.zkConn.Children(lockNode)
		if err != nil {
			fmt.Println("Children error:", err)
			d.zkConn.Delete(d.path, -1)
			return err
		}

		if len(list) == 0 {
			fmt.Println("ReadLock: no lock nodes")
			d.zkConn.Delete(d.path, -1)
			return fmt.Errorf("ReadLock: no lock nodes")
		}

		sort.Slice(list, func(i, j int) bool {
			a := list[i][2:]
			b := list[j][2:]
			return a < b
		})

		watchFile := ""
		for _, child := range list {
			if child[0] == 'w' {
				watchFile = child
			}

			if lockNode+"/"+child == d.path {
				if watchFile == "" {
					return nil
				}
				break
			}
		}
		exist, _, ch, err := d.zkConn.ExistsW(lockNode + "/" + watchFile)
		if err != nil {
			fmt.Println("ExistsW error:", err)
			d.zkConn.Delete(d.path, -1)
			return err
		}

		if exist {
			<-ch
		}
	}
}

func (d *DistLock) WriteLock() (err error) {
	lockNode := filepath.Join(LOCK, Hash(d.root))
	d.path, err = d.zkConn.Create(lockNode+writelockPrefix, []byte{}, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	for {
		list, _, err := d.zkConn.Children(lockNode)
		if err != nil {
			fmt.Println("Children error:", err)
			d.zkConn.Delete(d.path, -1)
			return err
		}

		if len(list) == 0 {
			fmt.Println("WriteLock: no lock nodes")
			d.zkConn.Delete(d.path, -1)
			return fmt.Errorf("WriteLock: no lock nodes")
		}

		sort.Slice(list, func(i, j int) bool {
			a := list[i][2:]
			b := list[j][2:]
			return a < b
		})

		if lockNode+"/"+list[0] == d.path {
			return nil
		}

		var prev string
		for i, v := range list {
			if lockNode+"/"+v == d.path {
				prev = d.root + "/" + list[i-1]
				break
			}
		}

		exist, _, ch, err := d.zkConn.ExistsW(prev)
		if err != nil {
			fmt.Println("ExistsW error:", err)
			d.zkConn.Delete(d.path, -1)
			return err
		}
		if exist {
			<-ch
		}
	}
}

// The unlock protocol is very simple: clients wishing to release a lock simply delete the node they created in step 1.
func (d *DistLock) Release() (err error) {
	// TODO: Students should implement this method
	return d.zkConn.Delete(d.path, -1)
}
