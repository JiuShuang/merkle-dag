package merkledag

import (
	"encoding/json"
	"strings"
)

func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte {

	hasHash, _ := store.Has(hash)

	if hasHash {
		objectJson, _ := store.Get(hash)
		object := JsonToObject(objectJson)
		filePath := strings.Split(path, "/")
		index := 1
		return getFile(object, filePath, index, store)
	}
	return nil
}

func getFile(object *Object, filePath []string, pathIndex int, store KVStore) []byte {
	if pathIndex >= len(filePath) {
		return nil
	}
	index := 0
	for i := range object.Links {
		objectType := string(object.Data[index : index+4])
		index += 4
		objInfo := object.Links[i]
		if objInfo.Name != filePath[pathIndex] {
			continue
		}
		if objectType == "tree" {
			objDirJson, _ := store.Get(objInfo.Hash)
			objDir := JsonToObject(objDirJson)
			file := getFile(objDir, filePath, pathIndex+1, store)
			if file != nil {
				return file
			}
		} else if objectType == "blob" {
			file, _ := store.Get(objInfo.Hash)
			return file
		} else if objectType == "list" {
			objLinkJson, _ := store.Get(objInfo.Hash)
			objList := JsonToObject(objLinkJson)
			file := getSliceFile(objList, store)
			return file
		}
	}
	return nil
}

func getSliceFile(object *Object, store KVStore) []byte {
	file := make([]byte, 0)
	index := 0
	for i := range object.Links {
		objectType := string(object.Data[index : index+4])
		index += 4
		objectLink := object.Links[i]
		objectJson, _ := store.Get(objectLink.Hash)
		curObj := JsonToObject(objectJson)
		if objectType == "blob" {
			file = append(file, objectJson...)
		} else {
			tmp := getSliceFile(curObj, store)
			file = append(file, tmp...)
		}
	}
	return file
}

func JsonToObject(objectJson []byte) *Object {
	var object *Object
	err := json.Unmarshal(objectJson, &object)
	if err != nil {
		return nil
	}
	return object
}
