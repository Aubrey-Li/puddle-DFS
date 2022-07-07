package test

import (
	puddlestore "puddlestore/pkg"
	"testing"
)

func TestCreatedFileVisible(t *testing.T) {
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
	client2, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	client3, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	// client 1 adds a file
	fd, err := client1.Open("/puddlestore.go", true, true)
	if err != nil {
		t.Fatal(err)
	}
	client1.Close(fd)

	// client 2 and client 3 list the file, should be visible
	ls, err := client2.List("/puddlestore.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 1 && ls[0] != "puddlestore.go" {
		t.Errorf("client 2 don't see file")
	}

	ls, err = client3.List("/puddlestore.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 1 && ls[0] != "puddlestore.go" {
		t.Errorf("client 3 don't see file")
	}

	client1.Exit()
	client2.Exit()
	client3.Exit()
}

func TestCreatedDirVisible(t *testing.T) {
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
	client2, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	client3, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	// client 1 adds a file
	err = client1.Mkdir("/raft")
	if err != nil {
		t.Fatal(err)
	}

	// client 2 and client 3 list the file, should be visible
	ls, err := client2.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 1 && ls[0] != "raft" {
		t.Errorf("client 2 don't see dir")
	}

	ls, err = client3.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != 1 && ls[0] != "raft" {
		t.Errorf("client 3 don't see dir")
	}

	client1.Exit()
	client2.Exit()
	client3.Exit()
}

func TestSecondClientReads(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create first client, write
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	in := "test"
	if err := writeFile(client1, "/a", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}

	// create second client, read
	client2, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	var out []byte
	if out, err = readFile(client2, "/a", 0, 5); err != nil {
		t.Fatal(err)
	}

	if in != string(out) {
		t.Fatalf("Expected: %v, Got: %v", in, string(out))
	}

	client1.Exit()
	client2.Exit()
}

func TestSecondClientRemove(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	// create first client, write
	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	in := "test"
	if err := writeFile(client1, "/a", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}

	// create second client, remove
	client2, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	err = client2.Remove("/a")
	if err != nil {
		t.Fatal(err)
	}

	client2.Exit()

	// client 1 list "/a", should not exist
	ls, err := client1.List("/a")
	if err == nil || len(ls) != 0 {
		t.Fatal(err)
	}
	client1.Exit()
}
