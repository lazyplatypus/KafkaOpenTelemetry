package main

import (
        "fmt"
        "net/http"
        "log"
)

func Parse (w http.ResponseWriter, req *http.Request) {
	//Get Email Values
	name := req.FormValue("name")
	question := req.FormValue("question")
	fmt.Println(name, question)

}

func main() {
	http.HandleFunc("/", Parse)
	err := http.ListenAndServe(":3003", nil)
	if err != nil {
			log.Fatal("ListenAndServe: ", err)
	}
}
