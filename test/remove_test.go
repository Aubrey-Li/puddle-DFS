package test

import (
	puddlestore "puddlestore/pkg"
	"sync"
	"testing"
)

func TestRemoveNonExisting(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	err = client.Remove("/a")
	if err == nil {
		t.Fatal("RemoveNonExisting Expected error")
	}

	err = client.Mkdir("/a")
	if err != nil {
		t.Fatal(err)
	}
	err = client.Remove("/a/b")
	if err == nil {
		t.Fatal("RemoveNonExisting Expected error")
	}
}

func TestRemoveFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	fd, err := client.Open("/a", true, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Remove("/a")
	if err != nil {
		t.Fatal(err)
	}

	children, err := client.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 0 {
		t.Fatalf("Expected: [], Got: %v", children)
	}
}

func TestRemoveDir(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	err = client.Mkdir("/a")
	if err != nil {
		t.Fatal(err)
	}
	err = client.Mkdir("/a/b")
	if err != nil {
		t.Fatal(err)
	}
	fd, err := client.Open("/a/b/c", true, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Remove("/a")
	if err != nil {
		t.Fatal(err)
	}

	children, err := client.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 0 {
		t.Fatalf("Expected: [], Got: %v", children)
	}
}

func TestConcurrentRemove(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	client2, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	err = client.Mkdir("/a")
	if err != nil {
		t.Fatal(err)
	}
	err = client2.Mkdir("/a/b")
	if err != nil {
		t.Fatal(err)
	}
	fd, err := client.Open("/a/b/c", true, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		err = client.Remove("/a/b/c")
		defer wg.Done()
		if err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		err = client2.Remove("/a/b")
		defer wg.Done()
		if err != nil {
			t.Fatal(err)
		}
	}()
	wg.Wait()

	wg.Add(2)
	go func() {
		children, err := client.List("/")
		defer wg.Done()
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 1 || children[0] != "a" {
			t.Fatalf("Expected: [a], Got: %v", children)
		}
	}()

	go func() {
		children, err := client2.List("/a")
		defer wg.Done()
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 0 {
			t.Fatalf("Expected: [], Got: %v", children)
		}
	}()
	wg.Wait()
}
