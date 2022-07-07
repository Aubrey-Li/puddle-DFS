package pkg

import (
	"fmt"
	"math/rand"
	tapestry "tapestry/pkg"
	"time"

	"github.com/go-zookeeper/zk"
)

const TAPESTRY_ROOT = "/tapestry"
const ROOT = "/root"
const LOCK = "/lock"
const SEED = 12345

// Cluster is an interface for all nodes in a puddlestore cluster. One should be able to shutdown
// this cluster and create a client for this cluster
type Cluster struct {
	config Config
	nodes  []*Tapestry
	zkConn *zk.Conn
}

func (c *Cluster) GetNodes() []*Tapestry {
	return c.nodes
}

// Shutdown causes all tapestry nodes to gracefully exit
func (c *Cluster) Shutdown() {
	// tapestry exit
	for _, node := range c.nodes {
		node.GracefulExit()
	}
	// recursivey delete root, tapestry, lock
	cleanup(c.zkConn)

	time.Sleep(time.Second)
	c.zkConn.Close()
}

// NewClient creates a new Puddlestore client
func (c *Cluster) NewClient() (Client, error) {
	zkConn, err := ConnectZk(c.config.ZkAddr)
	if err != nil {
		return nil, err
	}

	var client *PuddleStoreClient = nil

	tapClients, err := GetTapestryClients(zkConn)
	if err != nil {
		zkConn.Close()
		return nil, err
	}

	if len(tapClients) < c.config.NumReplicas {
		zkConn.Close()
		return nil, fmt.Errorf("no enough nodes available")
	}

	client = &PuddleStoreClient{
		idx:       0,
		nodes:     tapClients,
		zkConn:    zkConn,
		stop:      make(chan bool),
		fdSeed:    0,
		files:     make(map[int]*File),
		config:    c.config,
		blockRead: 0,
		readCnt:   0,
	}
	go client.WatchTap()
	return client, nil
}

func CreateInitDir(path string, lockDir bool, zkConn *zk.Conn) error {
	dir := inode{
		Size:   0,
		IsDir:  true,
		Blocks: make([]string, 0),
	}
	data, err := encodeInode(dir)
	if err != nil {
		return err
	}
	if exists, _, err := zkConn.Exists(path); err != nil {
		return err
	} else if !exists {
		_, err := zkConn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		if lockDir {
			return createLockNode(path, zkConn)
		}
		return err
	}
	return nil
}

// CreateCluster starts all nodes necessary for puddlestore
func CreateCluster(config Config) (*Cluster, error) {
	// Start your tapestry cluster with size config.NumTapestry. You should
	// also use the zkAddr (zookeeper address) found in the config and pass it to
	// your Tapestry constructor method

	// start zookeeper connection
	zkConn, err := ConnectZk(config.ZkAddr)
	if err != nil {
		return nil, err
	}

	// initLock(zkConn)
	// defer initUnlock(zkConn)

	// create lock directory FIRST!
	err = CreateInitDir(LOCK, false, zkConn)
	if err != nil {
		return nil, err
	}

	// create tapestry directory
	err = CreateInitDir(TAPESTRY_ROOT, false, zkConn)
	if err != nil {
		return nil, err
	}

	// create file system root directory
	err = CreateInitDir(ROOT, true, zkConn)
	if err != nil {
		return nil, err
	}

	// generate tapestry nodes
	taps, err := tapestry.MakeRandomTapestries(SEED, config.NumTapestry)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Tapestry, config.NumTapestry)
	// add new tapestry nodes to cluster
	for i, tap := range taps {
		tapNode, err := NewTapestry(tap, config.ZkAddr)
		if err != nil {
			return nil, err
		}
		nodes[i] = tapNode
	}

	return &Cluster{
		config: config,
		nodes:  nodes,
		zkConn: zkConn,
	}, nil
}

// gets the node in a cluster based on path & index
func GetClusterNode(conn *zk.Conn, clusterPrefix string, index int) (path string, addr string, id string, found bool, err error) {
	children, _, err := conn.Children(clusterPrefix)
	if err != nil {
		return "", "", "", false, err
	}

	if len(children) > 0 && index < len(children) {
		path := clusterPrefix + "/"
		if index < 0 {
			path += children[rand.Intn(len(children))]
		} else {
			path += children[index]
		}

		data, _, err := conn.Get(path)
		if err != nil {
			return "", "", "", false, err
		}

		addr, id, err = deserializeNode(data)
		return path, addr, id, true, err
	}
	return "", "", "", false, nil
}

func GetTapestryClients(zkConn *zk.Conn) (tapClients []*tapestry.Client, err error) {
	children, _, err := zkConn.Children(TAPESTRY_ROOT)
	if err != nil {
		return nil, err
	}
	if len(children) == 0 {
		return []*tapestry.Client{}, nil
	}
	for _, i := range randomIndices(len(children)) {
		_, tapAddr, _, found, err := GetClusterNode(zkConn, TAPESTRY_ROOT, i)
		if err != nil || !found {
			continue
		}
		tapClient, err := tapestry.Connect(tapAddr)
		if err != nil {
			continue
		}
		tapClients = append(tapClients, tapClient)
	}
	return tapClients, nil
}
