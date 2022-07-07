package pkg

import (
	"fmt"
	tapestry "tapestry/pkg"

	"github.com/go-zookeeper/zk"
)

// Tapestry is a wrapper for a single Tapestry node. It is responsible for
// maintaining a zookeeper connection and implementing methods we provide
type Tapestry struct {
	tap    *tapestry.Node
	zkConn *zk.Conn
}

// NewTapestry creates a new tapestry struct
func NewTapestry(tap *tapestry.Node, zkAddr string) (*Tapestry, error) {
	//  TODO: Setup a zookeeper connection and return a Tapestry struct
	zkConn, err := ConnectZk(zkAddr)
	if err != nil {
		return nil, err
	}

	// add tap node to cluster
	err = AddNodetoCluster(zkConn, tap.Addr(), tap.ID(), TAPESTRY_ROOT)

	return &Tapestry{
		tap,
		zkConn,
	}, err
}

// GracefulExit closes the zookeeper connection and gracefully shuts down the tapestry node
func (t *Tapestry) GracefulExit() {
	t.zkConn.Close()
	t.tap.Leave()
}

func AddNodetoCluster(conn *zk.Conn, addr string, id string, clusterPrefix string) (err error) {
	path := fmt.Sprintf("%s/%v", clusterPrefix, id)
	data := serializeNode(addr, id)
	_, err = conn.Create(path, data, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	return err
}
