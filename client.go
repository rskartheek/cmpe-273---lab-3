package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"sort"
)

type HashRing []uint32

type KeyValue struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type Node struct {
	Id int
	IP string
}

func NewNode(id int, ip string) *Node {
	return &Node{
		Id: id,
		IP: ip,
	}
}

type ConsistentHash struct {
	Nodes     map[uint32]Node
	IsPresent map[int]bool
	hashRing  HashRing
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{
		Nodes:     make(map[uint32]Node),
		IsPresent: make(map[int]bool),
		hashRing:  HashRing{},
	}
}

func (hr *ConsistentHash) AddNode(node *Node) bool {

	if _, ok := hr.IsPresent[node.Id]; ok {
		return false
	}
	str := hr.ReturnNodeIP(node)
	hr.Nodes[hr.GetHashValue(str)] = *(node)
	hr.IsPresent[node.Id] = true
	hr.SortHashCircle()
	return true
}

func (hr *ConsistentHash) SortHashCircle() {
	hr.hashRing = HashRing{}
	for k := range hr.Nodes {
		hr.hashRing = append(hr.hashRing, k)
	}
	sort.Sort(hr.hashRing)
}

func (hr *ConsistentHash) ReturnNodeIP(node *Node) string {
	return node.IP
}

func (hr *ConsistentHash) GetHashValue(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (hr *ConsistentHash) Get(key string) Node {
	hash := hr.GetHashValue(key)
	i := hr.SearchForNode(hash)
	return hr.Nodes[hr.hashRing[i]]
}

func (hr *ConsistentHash) SearchForNode(hash uint32) int {
	i := sort.Search(len(hr.hashRing), func(i int) bool { return hr.hashRing[i] >= hash })
	if i < len(hr.hashRing) {
		if i == len(hr.hashRing)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(hr.hashRing) - 1
	}
}

func SaveKey(hash *ConsistentHash, keyID string, value string) {
	ipAddress := hash.Get(keyID)
	address := "http://" + ipAddress.IP + "/keys/" + keyID + "/" + value
	req, err := http.NewRequest("PUT", address, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer resp.Body.Close()
		fmt.Println(resp)
	}
}

func GetKey(key string, hash *ConsistentHash) {
	var keyValue KeyValue
	ipAddress := hash.Get(key)
	address := "http://" + ipAddress.IP + "/keys/" + key
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		bodyContent, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(bodyContent, &keyValue)
		response, _ := json.Marshal(keyValue)
		fmt.Println(string(response))
	}
}

func GetAllKeys(address string) {
	var keyValueArray []KeyValue
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		bodyContent, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(bodyContent, &keyValueArray)
		response, _ := json.Marshal(keyValueArray)
		fmt.Println(string(response))
	}
}

func (hr HashRing) Len() int {
	return len(hr)
}

func (hr HashRing) Less(i, j int) bool {
	return hr[i] < hr[j]
}

func (hr HashRing) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}

func main() {
	newHash := NewConsistentHash()
	newHash.AddNode(NewNode(0, "127.0.0.1:3000"))
	newHash.AddNode(NewNode(1, "127.0.0.1:3001"))
	newHash.AddNode(NewNode(2, "127.0.0.1:3002"))

	fmt.Println("Put keys in cache")
	SaveKey(newHash, "1", "a")
	SaveKey(newHash, "2", "b")
	SaveKey(newHash, "3", "c")
	SaveKey(newHash, "4", "d")
	SaveKey(newHash, "5", "e")
	SaveKey(newHash, "6", "f")
	SaveKey(newHash, "7", "g")
	SaveKey(newHash, "8", "h")
	SaveKey(newHash, "9", "i")
	SaveKey(newHash, "10", "j")

	fmt.Println("Get Key from Cache")

	GetKey("1", newHash)
	GetKey("2", newHash)
	GetKey("3", newHash)
	GetKey("4", newHash)
	GetKey("5", newHash)
	GetKey("6", newHash)
	GetKey("7", newHash)
	GetKey("8", newHash)
	GetKey("9", newHash)
	GetKey("10", newHash)

	fmt.Println("Get Keys from port 3000")
	GetAllKeys("http://127.0.0.1:3000/keys")
	fmt.Println("Get all keys from ort 3001")
	GetAllKeys("http://127.0.0.1:3001/keys")
	fmt.Println("Get all keys from port 3002")
	GetAllKeys("http://127.0.0.1:3002/keys")

}
