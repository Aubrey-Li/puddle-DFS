# Puddlestore

PuddleStore is a simple distributed filesystem based on Zookeeper and Tapestry.

## High-level Project Design

This project is to implement a basic functionality for a distributed filesystem, which provides open/read/write/mkdir/list/remove interface for users. The file hierarchy and metadata are stored on Zookeeper, and file data blocks are stored on a Distributed Object Location and Retrieval (DOLR) service, Tapestry. Each file has an iNode stored on Zookeeper, which holds its blocks' GUID and file size. Besides, we use Tapestry to store and replicate the actual file blocks. Since that Tapestry does not provide multi-object transactions, in order to providing atomicity for file modification, every modification of data blocks will create new objects in the DOLR.

A puddlestore client will holds some tapestry node clients to interact with tapestry cluster. All tapestry nodes will connect to the Zookeeper and store its address under znode '/tapestry' with Ephemeral&Sequential flags. In order to deal with the crash of tapestry node, puddlestore client will watch the children of znode '/tapestry', and update its tapestry node clients if there is any change.

### Extra feature

#### Flush on close
Every write operation will only store changes locally in the client's cache. Those changes will finally be flushed to DOLR when we call close interface.

#### Read/Write Distributed Lock
We implement the Read/Write lock based on Zookeeper. Many clients can simultaneously hold a read lock and do read operation for each file, while there is only one client can acquire a write lock for each file at a time.

#### Pre-fetching
Every puddlestore client will store its average number of blocks every read operation and pre-fetch these blocks and cache locally in the memory when user call read interface.

## Test Description
The test can cover about 83.7% of the pkg.
- `client_test`: Test create normal puddlestore client and test create multiple client connections.
- `list_test`: Test list dir/file/empty dir/non existing path
- `mkdir_test`: Test mkdir exist dir/collision dir/dir under file
- `open/close_test`: Test open root/file under root/file under file and close non existed path
- `cluster_test`: Test create cluster
- `lock_test`: Test concurrent open/read/write/write&read
- `multiclient_test`: Test created file/dir visible to another client. Test read/remove by another client.
- `read_test`: Similar to TA test, test read operation non existed fd/empty file/beyond file size/accross blocks
- `remove_test`: Test remove file/dir/non existing. Test concurrent remove.
- `tapestry_test`: Similar to TA test, test blocks available when kill one tapestry node.
- `write_test`: Test write accross blocks/offset in empty file/multi write/overwrite/invalid fd



## Collaboration
Qiye Tan 50%:
* File Inode
* Read/Write Lock
* Client read/write/open/close
* Readme

Aubrey Li 50%:
* Membership Init
* Test Code
* Client list/remove/mkdir/exit
* Readme
