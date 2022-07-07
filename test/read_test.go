package test

import (
	puddlestore "puddlestore/pkg"
	"sync"
	"testing"
)

func TestReadNonexistentFd(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Read(0, 0, 0)
	if err == nil {
		t.Fatal("Expected error")
	}
}

func TestReadFromEmptyFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	fd, err := client.Open("/a", true, false)
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.Read(fd, 0, 100)
	if err != nil {
		t.Fatal(err)
	}

	if out == nil || len(out) != 0 {
		t.Fatalf("Expected: [], Got: %v", out)
	}
}

func TestEndBeyondFileSize(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	in := "test12345"
	filename := "/0912"
	var offset uint64 = 2
	err = writeFile(client, filename, 0, []byte(in))
	if err != nil {
		t.Fatal(err)
	}

	out, err := readFile(client, filename, offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != in[offset:] {
		t.Fatalf("Expected: %v, Got: %v", in[offset:], string(out))
	}
}

func TestOffsetBeyondFileSize(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	in := "test12345"
	filename := "/asdluhb"
	var offset uint64 = 50
	err = writeFile(client, filename, 0, []byte(in))
	if err != nil {
		t.Fatal(err)
	}

	out, err := readFile(client, filename, offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("Expected: [], Got: %v", string(out))
	}
}

func TestReadAcrossBlocks(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.Config{
		BlockSize:   8,
		NumReplicas: 2,
		NumTapestry: 2,
		ZkAddr:      "localhost:2181",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	in := "test1234509876"
	filename := "/asdluhb"
	var offset uint64 = 5
	err = writeFile(client, filename, 0, []byte(in))
	if err != nil {
		t.Fatal(err)
	}

	out, err := readFile(client, filename, offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != in[offset:] {
		t.Fatalf("Expected: %v, Got: %v", in[offset:], string(out))
	}
}

func TestConcurrentCreateDiffFiles(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		_, err := client.Open("/a1", true, false)
		wg.Done()
		if err != nil {
			t.Fatal(err)
		}
	}()

	go func() {
		_, err := client.Open("/a2", true, false)
		wg.Done()
		if err != nil {
			t.Fatal(err)
		}
	}()
	wg.Wait()
}
