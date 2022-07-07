package test

import (
	"fmt"
	puddlestore "puddlestore/pkg"
	"sync"
	"testing"
	"time"
)

func TestConcurrentWrite(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	filename := "/ab"
	in := "test"

	for i := 0; i < 3; i++ {
		go writeFile(client, filename, 0, []byte(in+fmt.Sprintf("%d", i)))
	}

	defer client.Remove(filename)

	time.Sleep(time.Second)
	res, err := readFile(client, filename, 0, 6)
	if err != nil {
		t.Fatal(err)
	}

	var i int
	for i = 0; i < 3; i++ {
		if string(res) == in+fmt.Sprintf("%d", i) {
			break
		}
	}
	if i >= 3 {
		t.Fatalf("Get wrong res: %v", res)
	}
}

func TestConcurrentRead(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	filename := "/abc"
	in := "test"
	err = writeFile(client, filename, 0, []byte(in))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			out, err := readFile(client, filename, 0, 6)
			if err != nil {
				t.Fatal(err)
			}
			if string(out) != in {
				t.Fatalf("Expected: %v, Got: %v", in, string(out))
			}
			wg.Done()
		}()
	}
	// time.Sleep(time.Second)
	wg.Wait()
}

func TestConcurrentWriteRead(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	filename := "/abc"
	in := "test"
	err = writeFile(client, filename, 0, []byte(in))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func(i int) {
			if i%2 == 0 {
				out, err := readFile(client, filename, 0, 6)
				if err != nil {
					t.Fatal(err)
				}
				if string(out) != in {
					t.Fatalf("Expected: %v, Got: %v", in, string(out))
				}
			} else {
				err := writeFile(client, filename, 0, []byte(in))
				if err != nil {
					t.Fatal(err)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestTwoClientOpenSameFileShouldBlock(t *testing.T) {
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

	filename := "/abc"
	fd, err := client.Open(filename, true, false)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		cur := time.Now()
		fd, err := client2.Open(filename, true, true)
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(cur) < time.Second {
			t.Fatal("client2 should be block")
		}
		err = client2.Close(fd)
		if err != nil {
			t.Fatal(err)
		}
	}()

	time.Sleep(time.Second)
	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTwoClientOpenSerialized(t *testing.T) {
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

	filename := "/abc"
	fd, err := client.Open(filename, true, false)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		fd, err := client2.Open(filename, true, true)
		if err != nil {
			t.Fatal(err)
		}
		err = client2.Close(fd)
		if err != nil {
			t.Fatal(err)
		}
	}()

	for _, node := range cluster.GetNodes() {
		node.GracefulExit()
	}
	time.Sleep(time.Second)
	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}
}
