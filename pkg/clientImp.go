package pkg

import (
	"fmt"
	"path/filepath"
	"sync"
	tapestry "tapestry/pkg"

	"github.com/go-zookeeper/zk"
)

type PuddleStoreClient struct {
	idx        int
	nodesMutex sync.Mutex
	nodes      []*tapestry.Client
	zkConn     *zk.Conn
	stop       chan bool

	files     map[int]*File
	fdSeed    int
	fdRecycle []int

	config    Config
	blockRead uint64
	readCnt   uint64
}

func (c *PuddleStoreClient) WatchTap() {
	for {
		_, _, ch, err := c.zkConn.ChildrenW(TAPESTRY_ROOT)
		if err != nil {
			return
		}

		select {
		case <-c.stop:
			return
		case <-ch:
		}

		taps, _, err := c.zkConn.Children(TAPESTRY_ROOT)
		if err != nil {
			return
		}

		var nodes []*tapestry.Client
		for _, tap := range taps {
			nodePath := filepath.Join(TAPESTRY_ROOT, tap)
			data, _, err := c.zkConn.Get(nodePath)
			if err != nil {
				continue
			}

			addr, _, err := deserializeNode(data)
			tap, err := tapestry.Connect(addr)
			if err != nil {
				continue
			}
			nodes = append(nodes, tap)
		}
		c.nodesMutex.Lock()
		c.nodes = nodes
		c.nodesMutex.Unlock()
	}
}

func (c *PuddleStoreClient) Get(key string) ([]byte, error) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()
	for i := 0; i < c.config.NumReplicas; i++ {
		node := c.nodes[c.idx]
		c.idx = (c.idx + 1) % len(c.nodes)
		value, err := node.Get(key)
		if err == nil {
			return value, nil
		}
	}
	return nil, fmt.Errorf("get: key %s not found", key)
}

func (c *PuddleStoreClient) Store(key string, value []byte) error {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()
	cnt := 0
	for i := 0; i < len(c.nodes) && cnt < c.config.NumReplicas; i++ {
		node := c.nodes[c.idx]
		c.idx = (c.idx + 1) % len(c.nodes)
		err := node.Store(key, value)
		if err == nil {
			cnt++
		}
	}
	if cnt == 0 {
		return fmt.Errorf("no node is available")
	}
	return nil
}

// `Open` opens a file and returns a file descriptor. If the `create` is true and the
// file does not exist, create the file. If `create` is false and the file does not exist,
// return an error. If `write` is true, then flush the resulting inode on Close(). If `write`
// is false, no need to flush the inode to zookeeper. If `Open` is successful, the returned
// file descriptor should be unique to the file. The client is responsible for keeping
// track of local file descriptors. Using `Open` allows for file-locking and
// multi-operation transactions.
func checkPath(path string) error {
	if path[len(path)-1] == '/' || path[0] != '/' {
		return fmt.Errorf("file name %s contains invalid character", path)
	}
	return nil
}

func (c *PuddleStoreClient) checkParent(path string) error {
	parent := filepath.Dir(path)
	exist, _, err := c.zkConn.Exists(parent)
	if err != nil {
		return err
	}
	if !exist {
		// No parent path
		return fmt.Errorf("the target file has no parent path")
	}

	parentInode, err := c.getInode(parent)
	if err != nil {
		return err
	}
	if !parentInode.IsDir {
		return fmt.Errorf("open: the parent is not a directory")
	}
	return nil
}

func (c *PuddleStoreClient) generateNewFd() int {
	if len(c.fdRecycle) > 0 {
		fd := c.fdRecycle[len(c.fdRecycle)-1]
		c.fdRecycle = c.fdRecycle[:len(c.fdRecycle)-1]
		return fd
	}
	fd := c.fdSeed
	c.fdSeed++
	return fd
}

func (c *PuddleStoreClient) getInode(path string) (*inode, error) {
	data, _, err := c.zkConn.Get(path)
	if err != nil {
		return nil, err
	}
	return decodeInode(data)
}

func (c *PuddleStoreClient) createFile(path string, write, dir bool) (*inode, *DistLock, error) {
	in := &inode{
		Size:   0,
		IsDir:  dir,
		Blocks: make([]string, 0),
	}

	inode, err := encodeInode(*in)
	if err != nil {
		return nil, nil, err
	}

	parentlock := CreateDistLock(filepath.Dir(path), c.zkConn)
	parentlock.WriteLock()
	dlock, err := createNode(path, inode, write, c.zkConn)
	parentlock.Release()

	if err != nil {
		return nil, nil, err
	}
	return in, dlock, nil
}

func (c *PuddleStoreClient) Open(path string, create, write bool) (int, error) {
	if c.zkConn == nil {
		return -1, fmt.Errorf("Client has already been exited")
	}
	if err := checkPath(path); err != nil {
		return -1, err
	}
	path = filepath.Join(ROOT, path)

	err := c.checkParent(path)
	if err != nil {
		return -1, err
	}

	parentlock := CreateDistLock(filepath.Dir(path), c.zkConn)
	parentlock.ReadLock()
	exist, _, err := c.zkConn.Exists(path)
	parentlock.Release()
	if err != nil {
		return -1, err
	}

	if !exist && !create {
		return -1, fmt.Errorf("open: the target file does not exist")
	}

	flags := O_READ
	if write {
		flags |= O_WRITE
	}
	var in *inode
	var dlock *DistLock

	if !exist && create {
		// fmt.Println("create file", path)
		in, dlock, err = c.createFile(path, write, false)
		if err != nil {
			return -1, err
		}
	} else {
		dlock = CreateDistLock(path, c.zkConn)
		if write {
			dlock.WriteLock()
		} else {
			dlock.ReadLock()
		}

		in, err = c.getInode(path)
		if err != nil {
			dlock.Release()
			return -1, err
		}
		if in.IsDir {
			dlock.Release()
			return -1, fmt.Errorf("open: the target is a directory")
		}
	}

	fd := c.generateNewFd()
	c.files[fd] = &File{
		path:  path,
		flags: int32(flags),
		dlock: dlock,
		in:    in,
		cache: make(map[string][]byte),
	}
	// fmt.Println("Open:", path, create, write, "fd:", fd)
	return fd, nil
}

// `Close` closes the file and flushes its contents to the distributed filesystem.
// The updated closed file should be able to be opened again after successfully closing it.
// We only flush changes to the file on close to ensure copy-on-write atomicity of operations.
// Refer to the handout for more information on why this is necessary.
func (c *PuddleStoreClient) Close(fd int) error {
	// fmt.Println("Close:", fd)
	if c.zkConn == nil {
		return fmt.Errorf("Client has already been exited")
	}
	if file, ok := c.files[fd]; ok {
		defer func() {
			file.dlock.Release()
			delete(c.files, fd)
			c.fdRecycle = append(c.fdRecycle, fd)
		}()

		if file.flags&O_WRITE != 0 {
			data, err := encodeInode(*file.in)
			if err != nil {
				return err
			}

			for _, guid := range file.in.Blocks {
				if block, ok := file.cache[guid]; ok {
					err = c.Store(guid, block)
					if err != nil {
						return err
					}
				}
			}

			_, err = c.zkConn.Set(file.path, data, -1)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return fmt.Errorf("close: file descriptor is not valid")
}

// `Read` returns a `size` amount of bytes starting at `offset` in an opened file.
// Reading at non-existent offset returns empty buffer and no error.
// If offset+size exceeds file boundary, return as much as possible with no error.
// Returns err if fd is not opened.
func (c *PuddleStoreClient) Read(fd int, offset, size uint64) ([]byte, error) {
	// fmt.Println("Read:", fd, offset, size)
	if c.zkConn == nil {
		return nil, fmt.Errorf("Client has already been exited")
	}
	if file, ok := c.files[fd]; ok {
		return file.read(c, offset, size)
	}
	return nil, fmt.Errorf("read: file descriptor is not valid")
}

// `Write` writes `data` starting at `offset` on an opened file. Writing beyond the
// file boundary automatically fills the file with zero bytes. Returns err if fd is not opened.
// If the file was opened with write = true flag, `Write` should return an error.
func (c *PuddleStoreClient) Write(fd int, offset uint64, data []byte) error {
	// fmt.Println("Write:", fd, offset, len(data))
	if c.zkConn == nil {
		return fmt.Errorf("Client has already been exited")
	}
	if file, ok := c.files[fd]; ok {
		if file.flags&O_WRITE == 0 {
			return fmt.Errorf("write: file is not opened for writing")
		}
		return file.write(c, offset, data)
	}
	return fmt.Errorf("write: file descriptor is not valid")
}

// `Mkdir` creates directory at the specified path.
// Returns error if any parent directory does not exist (non-recursive).
func (c *PuddleStoreClient) Mkdir(path string) error {
	// fmt.Println("Mkdir:", path)
	if c.zkConn == nil {
		return fmt.Errorf("Client has already been exited")
	}
	if path[0] != '/' {
		return fmt.Errorf("file name %s contains invalid character", path)
	}
	path = filepath.Join(ROOT, path)

	err := c.checkParent(path)
	if err != nil {
		// returns err if parent dir doesn't exist
		return err
	}
	parentlock := CreateDistLock(filepath.Dir(path), c.zkConn)
	parentlock.ReadLock()
	exist, _, err := c.zkConn.Exists(path)
	parentlock.Release()

	if err != nil {
		return err
	}
	if exist {
		return fmt.Errorf("mkdir: the target directory already exists")
	}
	_, dlock, err := c.createFile(path, false, true)
	if err != nil {
		return err
	}
	dlock.Release()

	return nil
}

// `Remove` removes a directory or file. Returns err if not exists.
func (c *PuddleStoreClient) Remove(path string) error {
	// fmt.Println("Remove:", path)
	if c.zkConn == nil {
		return fmt.Errorf("Client has already been exited")
	}
	path = filepath.Join(ROOT, path)
	exists, _, err := c.zkConn.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("remove: the target path does not exist")
	}

	return removeNode(path, c.zkConn)
}

// `List` lists file & directory names (not full names) under `path`. Returns err if not exists.
func (c *PuddleStoreClient) List(path string) ([]string, error) {
	// fmt.Println("List:", path)
	if c.zkConn == nil {
		return nil, fmt.Errorf("Client has already been exited")
	}
	path = filepath.Join(ROOT, path)

	dlock := CreateDistLock(path, c.zkConn)
	dlock.ReadLock()
	defer dlock.Release()

	ino, err := c.getInode(path)
	if err != nil {
		return nil, err
	}
	if ino.IsDir {
		children, _, err := c.zkConn.Children(path)
		if err != nil {
			return nil, err
		}
		return children, nil
	}
	return []string{filepath.Base(path)}, nil
}

// Release zk connection. Subsequent calls on Exit()-ed clients should return error.
func (c *PuddleStoreClient) Exit() {
	for fd := range c.files {
		c.Close(fd)
	}
	c.stop <- true
	c.zkConn.Close()
	c.zkConn = nil
}
