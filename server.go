package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
)

// Key struct
type Key struct {
	Key   int    `json:"key"`
	Value string `json:"value"`
}

var keyValueArray1, keyValueArray2, keyValueArray3 []Key

// GetKey to get key in server cache
func GetKey(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	portNumber := strings.Split(r.Host, ":")
	keyID := ps.ByName("key_id")
	intID, err := strconv.Atoi(keyID)
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(404)
		return
	}
	var tempArray []Key
	if portNumber[1] == "3000" {
		tempArray = keyValueArray1
	} else if portNumber[1] == "3001" {
		tempArray = keyValueArray2
	} else {
		tempArray = keyValueArray3
	}
	for i := 0; i < len(tempArray); i++ {
		if tempArray[i].Key == intID {
			uj, _ := json.Marshal(tempArray[i])
			// Write content-type, statuscode, payload
			fmt.Println("Found key in port:" + portNumber[1])
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fmt.Fprintf(w, "%s", uj)
			return
		}
	}
	w.WriteHeader(404)
	return

}

//GetAllKeys to get keys in the server
func GetAllKeys(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	portNumber := strings.Split(r.Host, ":")
	var allKeys []Key
	if portNumber[1] == "3000" {
		allKeys = keyValueArray1
	} else if portNumber[1] == "3001" {
		allKeys = keyValueArray2
	} else {
		allKeys = keyValueArray3
	}
	uj, _ := json.Marshal(allKeys)
	fmt.Println(allKeys)
	// Write content-type, statuscode, payload
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	fmt.Fprintf(w, "%s", uj)
}

func saveKeys(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	portNumber := strings.Split(r.Host, ":")
	keyID := ps.ByName("key_id")
	value := ps.ByName("value")
	var keyValue Key
	keyid, err := strconv.Atoi(keyID)
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(404)
		return
	}
	keyValue.Key = keyid
	keyValue.Value = value
	if portNumber[1] == "3000" {
		keyValueArray1 = append(keyValueArray1, keyValue)
	} else if portNumber[1] == "3001" {
		keyValueArray2 = append(keyValueArray2, keyValue)
	} else {
		keyValueArray3 = append(keyValueArray3, keyValue)
	}
	uj, _ := json.Marshal(keyValue)
	// Write content-type, statuscode, payload
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(201)
	fmt.Println("saved in Server with port number:" + portNumber[1])
	fmt.Fprintf(w, "%s", uj)
}

func main() {

	router := httprouter.New()
	router.GET("/keys", GetAllKeys)
	router.GET("/keys/:key_id", GetKey)
	router.PUT("/keys/:key_id/:value", saveKeys)
	go func() {
		http.ListenAndServe(":3001", router)
	}()
	go func() {
		http.ListenAndServe(":3002", router)
	}()

	http.ListenAndServe(":3000", router)

}
