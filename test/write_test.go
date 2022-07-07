package test

import (
	"bytes"
	"fmt"
	puddlestore "puddlestore/pkg"
	"testing"
)

func TestWriteUnopenedFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	err = client.Write(0, 0, []byte("abcdefghijklmnopqrstuvwxyz"))
	if err == nil {
		t.Fatal("WriteUnopenedFile Expected error")
	}
}

func TestWriteToLargeOffset(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	filename := "/a"
	in := "abcdefghijklmnopqrstuvwxyz"
	offset := uint64(1000)
	err = writeFile(client, filename, offset, []byte(in))
	if err != nil {
		t.Fatal(err)
	}

	out, err := readFile(client, filename, offset, uint64(len(in)))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != in {
		t.Fatalf("Expected: %v, Got: %v", in, string(out))
	}
}

func TestWriteMultipleTimes(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	filename := "/a"
	in := "test"

	for i := 0; i < 10; i++ {
		err = writeFile(client, filename, 0, []byte(in+fmt.Sprintf("%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	out, err := readFile(client, filename, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != in+fmt.Sprintf("%d", 9) {
		t.Fatalf("Expected: %v, Got: %v", in+fmt.Sprintf("%d", 9), string(out))
	}
}

func TestRepeatedWrite(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	filename := "/a"
	in := "abcdefghijklmnopqrstuvwxyz"

	for i := 0; i < 10; i++ {
		err = writeFile(client, filename, 0, []byte(in))
		if err != nil {
			t.Fatal(err)
		}
	}

	out, err := readFile(client, filename, 0, uint64(len(in)))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != in {
		t.Fatalf("Expected: %v, Got: %v", in, string(out))
	}
}

func TestReadYourWriteWithoutClosing(t *testing.T) {
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

	filename := "/a"
	in := "abcdefghijklmnopqrstuvwxyz"

	fd, err := client.Open(filename, true, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Write(fd, 0, []byte(in))
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.Read(fd, 0, uint64(len(in)))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != in {
		t.Fatalf("Expected: %v, Got: %v", in, string(out))
	}

	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadYourOverwrite(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	filename := "/a"
	in := "abcdefghijklmnopqrstuvwxyz"
	cover := "1234567890"
	var offset uint64 = 20
	err = writeFile(client, filename, 0, []byte(in))
	if err != nil {
		t.Fatal(err)
	}

	err = writeFile(client, filename, offset, []byte(cover))
	if err != nil {
		t.Fatal(err)
	}
	out, err := readFile(client, filename, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != in[:offset]+cover {
		t.Fatalf("Expected: %v, Got: %v", in[:offset]+cover, string(out))
	}
}

func TestWriteAtOffsetInEmptyFile(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}

	filename := "/a"
	in := "abcdefghijklmnopqrstuvwxyz"
	var offset uint64 = 50
	pre := make([]byte, offset)

	fd, err := client.Open(filename, true, true)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Write(fd, offset, []byte(in))
	if err != nil {
		t.Fatal(err)
	}

	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}

	out, err := readFile(client, filename, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, append(pre, []byte(in)...)) {
		t.Fatalf("Expected: %v, Got: %v", append(pre, []byte(in)...), out)
	}
}
