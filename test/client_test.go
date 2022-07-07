package test

import (
	puddlestore "puddlestore/pkg"
	"testing"
)

func TestCreateClient(t *testing.T) {
	// create a cluster
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create a client
	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	client.Exit()
}

func TestCreateMultipleClients(t *testing.T) {
	// create a cluster
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create multiple client connections
	num_clients := 10
	for i := 0; i < num_clients; i++ {
		_, err = cluster.NewClient()
		if err != nil {
			t.Fatal(err)
		}
	}
}
