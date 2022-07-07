package test

import (
	puddlestore "puddlestore/pkg"
	"testing"
)

func TestListRootFiles(t *testing.T) {
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

	// client call list under root dir, should return empty
	ls, err := client.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) > 0 {
		t.Fatal(err)
	}

	// client creates a file under root dir
	_, err = client.Open("/cs1380.pdf", true, true)
	if err != nil {
		t.Fatal(err)
	}

	// now call list under root dir, should return file name
	ls, err = client.List("/")
	if ls[0] != "cs1380.pdf" || err != nil {
		t.Fatal(err)
	}

	// client creates a file under root dir
	err = client.Mkdir("/cs1380")
	if err != nil {
		t.Fatal(err)
	}
	// now call list under root dir, should return file name
	ls, err = client.List("/")
	if len(ls) != 2 || err != nil {
		t.Fatal(err)
	}

	client.Exit()
}

func TestListRootDir(t *testing.T) {
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

	// client call list under root dir, should return empty
	ls, err := client.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) > 0 {
		t.Fatal(err)
	}

	// client creates a file under root dir
	err = client.Mkdir("/cs1380")
	if err != nil {
		t.Fatal(err)
	}
	// now call list under root dir, should return file name
	ls, err = client.List("/")
	if ls[0] != "cs1380" || err != nil {
		t.Fatal(err)
	}

	client.Exit()
}

func TestListFile(t *testing.T) {
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

	// client call list under root dir, should return empty
	ls, err := client.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) > 0 {
		t.Fatal(err)
	}

	// client creates a file under root dir
	_, err = client.Open("/cs.txt", true, false)
	if err != nil {
		t.Fatal(err)
	}

	// ls file, should return file name
	ls, err = client.List("/cs.txt")
	if ls[0] != "cs.txt" || err != nil {
		t.Fatal(err)
	}

	client.Exit()
}

func TestNonExistingPath(t *testing.T) {
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

	// client call list under root dir, should return empty
	ls, err := client.List("/404-not-found")
	if err == nil {
		t.Fatal(err)
	}
	if len(ls) > 0 {
		t.Fatal(err)
	}

	client.Exit()
}

func TestEmptyDir(t *testing.T) {
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

	// client creates a file under root dir
	err = client.Mkdir("/cs1380")
	if err != nil {
		t.Fatal(err)
	}

	ls, err := client.List("/cs1380")
	if err != nil || len(ls) != 0 {
		t.Fatal(err)
	}

	client.Exit()
}
