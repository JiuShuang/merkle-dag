package merkledag

import (
	"encoding/json"
	"hash"
)

const (
	K          = 1 << 10
	BLOCK_SIZE = 256 * K
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	if node.Type() == FILE {
		nodeHash, _ := StoreFile(store, node.(File), h)
		return nodeHash
	} else if node.Type() == DIR {
		return StoreDir(store, node.(Dir), h)
	}
	return nil
}

// 得到数据的hash值
func getDataHash(data []byte, h hash.Hash) []byte {
	h.Reset()
	h.Write(data)
	return h.Sum(nil)
}

func StoreFile(store KVStore, node File, h hash.Hash) ([]byte, []byte) {

	fileType := []byte("blob")
	//文件不需要分片的时候
	if len(node.Bytes()) < BLOCK_SIZE {
		data := node.Bytes()
		blob := Object{
			Links: nil,
			Data:  data,
		}
		jsonMarshal, _ := json.Marshal(blob)
		blobHash := getDataHash(jsonMarshal, h)
		flag, _ := store.Has(blobHash)
		if flag == false {
			store.Put(blobHash, data)
		}
		return blobHash, fileType
	} else {
		//文件需要分片的时候
		obj := Object{}
		fileType = []byte("list")
		m := node.Size() / BLOCK_SIZE

		for i := 0; i < int(m); i++ {
			start := i * BLOCK_SIZE
			end := (i + 1) * BLOCK_SIZE
			if end > len(node.Bytes()) {
				end = len(node.Bytes())
			}
			blockData := node.Bytes()[start:end]
			blockObject := Object{Data: blockData}
			jsonMarshal, _ := json.Marshal(blockObject)
			blobHash := getDataHash(jsonMarshal, h)
			flag, _ := store.Has(blobHash)
			if flag == false {
				err := store.Put(blobHash, blockData)
				if err != nil {
					return nil, nil
				}
			}
			obj.Data = append(obj.Data, []byte("blob")...)
			obj.Links = append(obj.Links, Link{Hash: blobHash, Size: end - start})
		}

		objectData := Object{Data: obj.Data, Links: obj.Links}
		objectJson, _ := json.Marshal(objectData)
		objectHash := getDataHash(objectJson, h)
		err := store.Put(objectHash, objectJson)
		if err != nil {
			return nil, nil
		}
		return objectHash, fileType
	}
}

func StoreDir(store KVStore, node Dir, h hash.Hash) []byte {
	iter := node.It()
	treeObject := Object{}
	for iter.Next() {
		node := iter.Node()
		if node.Type() == FILE {
			file := node.(File)
			key, fileType := StoreFile(store, file, h)
			treeObject.Data = append(treeObject.Data, fileType...)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: key,
				Size: int(file.Size()),
				Name: file.Name(),
			})
			treeJson, _ := json.Marshal(treeObject)
			treeHash := getDataHash(treeJson, h)
			flag, _ := store.Has(treeHash)
			if !flag {
				err := store.Put(treeHash, treeJson)
				if err != nil {
					return nil
				}
			}
		} else if node.Type() == DIR {
			key := StoreDir(store, node.(Dir), h)
			fileType := "tree"
			treeObject.Links = append(treeObject.Links, Link{
				Size: int(node.Size()),
				Name: node.Name(),
				Hash: key})
			treeObject.Data = append(treeObject.Data, []byte(fileType)...)
		}
		treeJson, _ := json.Marshal(treeObject)
		treeHash := getDataHash(treeJson, h)
		flag, _ := store.Has(treeHash)
		if !flag {
			err := store.Put(treeHash, treeJson)
			if err != nil {
				return nil
			}
		}
	}
	treeJson, _ := json.Marshal(treeObject)
	treeHash := getDataHash(treeJson, h)
	flag, _ := store.Has(treeHash)
	if !flag {
		err := store.Put(treeHash, treeJson)
		if err != nil {
			return nil
		}
	}
	return treeHash
}
