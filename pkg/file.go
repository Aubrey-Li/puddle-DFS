package pkg

import (
	"fmt"
	"path/filepath"

	"github.com/go-zookeeper/zk"
	"github.com/google/uuid"
)

const (
	O_READ = 1 << iota
	O_WRITE
)

type File struct {
	flags int32
	dlock *DistLock
	cache map[string][]byte
	path  string
	in    *inode
}

type inode struct {
	Size   uint64
	IsDir  bool
	Blocks []string
}

func createNode(path string, data []byte, write bool, zkConn *zk.Conn) (*DistLock, error) {
	err := createLockNode(path, zkConn)
	if err != nil {
		return nil, err
	}

	dlock := CreateDistLock(path, zkConn)
	if write {
		dlock.WriteLock()
	} else {
		dlock.ReadLock()
	}

	_, err = zkConn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		dlock.Release()
		return nil, err
	}
	return dlock, nil
}

func removeNode(path string, zkConn *zk.Conn) error {
	dlock := CreateDistLock(path, zkConn)
	dlock.WriteLock()
	defer dlock.Release()

	data, _, err := zkConn.Get(path)
	if err == zk.ErrNoNode {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove: error when zookeeper is trying to find target, %v", err)
	}

	in, err := decodeInode(data)
	if err != nil {
		return fmt.Errorf("remove: error when decoding... %v", err)
	}
	if in.IsDir {
		children, _, err := zkConn.Children(path)
		if err != nil {
			return err
		}
		for _, child := range children {
			err = removeNode(filepath.Join(path, child), zkConn)
			if err != nil {
				return err
			}
		}
	}
	return zkConn.Delete(path, -1)
}

func (file *File) read(c *PuddleStoreClient, offset, size uint64) ([]byte, error) {
	var res []byte = make([]byte, 0)
	pos := offset % c.config.BlockSize
	blocknum := int(offset / c.config.BlockSize)
	prefetchCnt := 0
	var bytes uint64 = 0
	c.readCnt++
	c.blockRead += (size + c.config.BlockSize - 1) / c.config.BlockSize
	avg := int(c.blockRead / c.readCnt)

	for bytes < size && offset < file.in.Size {
		length := min(size-bytes, c.config.BlockSize-pos)
		length = min(length, file.in.Size-offset)
		guid := file.in.Blocks[blocknum]
		block, ok := file.cache[guid]
		var err error
		if !ok {
			block, err = c.Get(guid)
			if err != nil {
				return nil, err
			}
			file.cache[guid] = block
		}
		res = append(res, block[pos:pos+length]...)
		bytes += length
		offset += length
		blocknum++
		pos = 0
		prefetchCnt++
	}

	// prefetch
	for prefetchCnt < avg && blocknum < len(file.in.Blocks) {
		guid := file.in.Blocks[blocknum]
		if _, ok := file.cache[guid]; !ok {
			block, err := c.Get(guid)
			if err != nil {
				return nil, err
			}
			file.cache[guid] = block
		}
		blocknum++
		prefetchCnt++
	}
	return res, nil
}

func (file *File) createNewBlock(blocksize uint64) (string, []byte) {
	guid := uuid.NewString()
	block := make([]byte, blocksize)
	file.cache[guid] = block
	return guid, block
}

func (file *File) write(c *PuddleStoreClient, offset uint64, data []byte) error {
	pos := offset % c.config.BlockSize
	bytes := 0
	size := len(data)
	blocknum := int(offset / c.config.BlockSize)

	// fmt.Println("write: offset: ", offset, "size: ", size, "file size: ", file.in.Size)

	for bytes < size {
		var block []byte
		var guid string
		if blocknum < len(file.in.Blocks) {
			guid, block = file.createNewBlock(c.config.BlockSize)
			oldguid := file.in.Blocks[blocknum]
			oldblk, ok := file.cache[oldguid]
			if !ok {
				var err error
				oldblk, err = c.Get(oldguid)
				if err != nil {
					return err
				}
			}
			copy(block, oldblk)
			// delete(file.cache, file.in.blocks[blocknum])
			file.in.Blocks[blocknum] = guid
		} else {
			for blocknum >= len(file.in.Blocks) {
				guid, block = file.createNewBlock(c.config.BlockSize)
				file.in.Blocks = append(file.in.Blocks, guid)
			}
		}

		length := min(uint64(size-bytes), c.config.BlockSize-pos)

		// fmt.Println("length: ", length, "size", size, "bytes", bytes, "pos", pos, "blocksize", c.config.BlockSize)
		copy(block[pos:pos+length], data[bytes:bytes+int(length)])
		pos = 0
		bytes += int(length)
		blocknum++
	}
	if offset+uint64(bytes) > file.in.Size {
		file.in.Size = offset + uint64(bytes)
	}
	return nil
}
