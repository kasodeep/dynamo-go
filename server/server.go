// package server provides an interface to the client for performing get() and put() via standard http request.
package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/kasodeep/dynamo-go/store"
)

func Get(w http.ResponseWriter, req *http.Request) {

}

func Put(w http.ResponseWriter, req *http.Request) {
	var object store.Object
	json.NewDecoder(req.Body).Decode(&object)

	// we have the object at this stage to store.
}

func initializeRouter() {
	router := mux.NewRouter()

	router.HandleFunc("/get", Get).Methods("GET")
	router.HandleFunc("/put", Put).Methods("POST")

	log.Fatal(http.ListenAndServe(":8000", router))
}
