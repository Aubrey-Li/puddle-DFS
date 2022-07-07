package test

import (
	puddlestore "puddlestore/pkg"
	"testing"
)

func TestDirExisted(t *testing.T) {
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

	// try to make dir twice
	err = client.Mkdir("/cs1380")
	if err == nil {
		t.Fatal(err)
	}

	client.Exit()
}

func TestDirExistedAlt(t *testing.T) {
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

	// try to make dir twice (with "/" at the end)
	err = client.Mkdir("/cs1380/")
	if err == nil {
		t.Fatal(err)
	}

	client.Exit()
}

func TestDirNameExisted(t *testing.T) {
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

	// client creates a dir
	err = client.Mkdir("/cs1380")
	if err != nil {
		t.Fatal(err)
	}

	// try to make file with the same name as dir
	_, err = client.Open("/cs1380", true, false)
	if err == nil {
		t.Fatal(err)
	}

	client.Exit()
}

func TestMakeDirUnderFile(t *testing.T) {
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

	fd, err := client.Open("/cs1380.pdf", true, false)
	if err != nil {
		t.Fatal(err)
	}

	client.Close(fd)

	// try to create a directory under the file
	err = client.Mkdir("/cs1380.pdf/new_dir")
	if err == nil {
		t.Fatal(err)
	}

	client.Exit()
}
