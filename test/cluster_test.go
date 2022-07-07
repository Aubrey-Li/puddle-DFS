package test

import (
	puddlestore "puddlestore/pkg"
	"testing"
)

func TestStartCluster(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	zkConn, _ := puddlestore.ConnectZk(puddlestore.DefaultConfig().ZkAddr)

	// check root dir
	exist, _, err := zkConn.Exists("/")
	if !exist || err != nil {
		t.Errorf("Root directory doesn't exist!")
	}

	// check tapestry
	tapExist, _, err := zkConn.Exists("/tapestry")
	if !tapExist || err != nil {
		t.Errorf("Tapestry doesn't exist!")
	}
	// check tapestry nodes
	tapChildren, _, err := zkConn.Children("/tapestry")
	if len(tapChildren) != puddlestore.DefaultConfig().NumTapestry {
		t.Errorf("Tapestry nodes missing!")
	}

	// check lock
	lock, _, err := zkConn.Exists("/lock")
	if !lock || err != nil {
		t.Errorf("distributed lock unset!")
	}
}
