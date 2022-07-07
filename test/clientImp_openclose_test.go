package test

import (
	puddlestore "puddlestore/pkg"
	"testing"
)

func TestOpenRoot(t *testing.T) {
	// create a cluster
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create a client
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	// client opens root with create flag, should err
	fd, err := client1.Open("/", true, false)
	if err == nil {
		t.Fatal(err)
	}
	client1.Close(fd)

	client1.Exit()
}

func TestCreateFileOnRootNoFlag(t *testing.T) {
	// create a cluster
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create a client
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	// client opens root with no flag, should err
	fd, err := client1.Open("/", false, false)
	if err == nil {
		t.Fatal(err)
	}
	client1.Close(fd)

	client1.Exit()
}

func TestCreateFileOnRootWithFlag(t *testing.T) {
	// create a cluster
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create a client
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	// client opens root with no flag, should err
	fd, err := client1.Open("/a_file", true, false)
	if err != nil {
		t.Fatal(err)
	}
	client1.Close(fd)

	client1.Exit()
}

func TestCreateFileUnderFile(t *testing.T) {
	// create a cluster
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create a client
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	// client create file under file, should err
	fd, err := client1.Open("/paxos.rs", true, true)
	if err != nil {
		t.Fatal(err)
	}
	client1.Close(fd)

	fd2, err := client1.Open("/paxos.rs/raft.go", true, true)
	if err == nil {
		t.Fatal(err)
	}
	client1.Close(fd2)

	client1.Exit()
}

func TestOpenDir(t *testing.T) {
	// create a cluster
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create a client
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	err = client1.Mkdir("/chubby")
	if err != nil {
		t.Fatal(err)
	}

	// client opens dir with both flags set, should err
	_, err = client1.Open("/chubby", true, true)
	if err == nil {
		t.Fatal(err)
	}

	client1.Exit()
}

func TestCloseNonExist(t *testing.T) {
	// create a cluster
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create a client
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	err = client1.Close(1000000)
	if err == nil {
		t.Fatal(err)
	}

	client1.Exit()
}
