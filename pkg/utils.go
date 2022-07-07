package pkg

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"strings"

	"github.com/go-zookeeper/zk"
	"github.com/hashicorp/go-msgpack/codec"
)

func Hash(msg string) string {
	hasher := sha256.New()
	hasher.Write([]byte(msg))
	return fmt.Sprintf("%d", binary.BigEndian.Uint64(hasher.Sum(nil)))
}

// Decode reverses the encode operation on a byte slice input
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

func encodeInode(in inode) ([]byte, error) {
	buf, err := encodeMsgPack(in)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeInode(data []byte) (*inode, error) {
	var in inode
	if err := decodeMsgPack(data, &in); err != nil {
		return nil, err
	}
	return &in, nil
}

func randomIndexGenerator(min int, max int) int {
	return rand.Intn(max-min) + min
}

// Returns the data stored under a cluster node in Zookeeper
func serializeNode(addr, id string) []byte {
	return []byte(fmt.Sprintf("%v,%v", addr, id))
}

// Returns the fields stored in the given data from a cluster node, or an error if the data is invalid
func deserializeNode(data []byte) (addr, id string, err error) {
	fields := strings.Split(string(data), ",")
	if len(fields) != 2 {
		return "", "", errors.New("invalid data")
	}
	return fields[0], fields[1], nil
}

// generates a list of shuffled indices from [1...n], after shuffling
func randomIndices(n int) []int {
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}
	for i := 0; i < n-2; i++ {
		exchangeI := rand.Intn(n-i) + i
		tmp := indices[i]
		indices[i] = indices[exchangeI]
		indices[exchangeI] = tmp
	}
	return indices
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func cleanup(conn *zk.Conn) error {
	err := recursiveDelete(conn, ROOT)
	if err != nil {
		return err
	}
	err = recursiveDelete(conn, TAPESTRY_ROOT)
	if err != nil {
		return err
	}
	err = recursiveDelete(conn, LOCK)
	if err != nil {
		return err
	}
	return nil
}

func recursiveDelete(conn *zk.Conn, path string) error {
	children, _, err := conn.Children(path)
	if err != nil {
		return err
	}
	for _, child := range children {
		err := recursiveDelete(conn, path+"/"+child)
		if err != nil && err != zk.ErrNoNode {
			return err
		}
	}

	// get version
	_, stat, err := conn.Get(path)
	if err != nil {
		return err
	}

	if err := conn.Delete(path, stat.Version); err != nil {
		return err
	}
	return nil
}
