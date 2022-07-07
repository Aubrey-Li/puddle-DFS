package test

import (
	puddlestore "puddlestore/pkg"
	"testing"
)

func TestKillAllTapsButOne(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.Config{
		BlockSize:   64,
		NumReplicas: 2,
		NumTapestry: 3,
		ZkAddr:      "localhost:2181",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	nodes := cluster.GetNodes()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	for i, node := range nodes {
		if i == 0 {
			continue
		}
		node.GracefulExit()
	}

	writeFile(client, "/test", 0, []byte("test"))
	children, err := client.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0] != "test" {
		t.Fatalf("Expected: [test], Got: %v", children)
	}
}

func TestKill1in3BeforeWrite(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.Config{
		BlockSize:   64,
		NumReplicas: 2,
		NumTapestry: 3,
		ZkAddr:      "localhost:2181",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	nodes := cluster.GetNodes()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	fd, err := client.Open("/test", true, true)
	if err != nil {
		t.Fatal(err)
	}

	nodes[0].GracefulExit()

	err = client.Write(fd, 0, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}

	client2, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	out, err := readFile(client2, "/test", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "test" {
		t.Fatalf("Expected: test, Got: %v", string(out))
	}
}

func TestKill1in3AfterWrite(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.Config{
		BlockSize:   64,
		NumReplicas: 2,
		NumTapestry: 3,
		ZkAddr:      "localhost:2181",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	nodes := cluster.GetNodes()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	fd, err := client.Open("/test", true, true)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Write(fd, 0, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	nodes[0].GracefulExit()

	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}

	client2, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	out, err := readFile(client2, "/test", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "test" {
		t.Fatalf("Expected: test, Got: %v", string(out))
	}
}
