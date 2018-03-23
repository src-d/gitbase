package main

import (
	"flag"
	"fmt"

	"gopkg.in/bblfsh/client-go.v2"
)

var endpoint = flag.String("e", "localhost:9432", "endpoint of the babelfish server")
var filename = flag.String("f", "", "file to parse")
var query = flag.String("q", "", "xpath expression")

func main() {
	flag.Parse()
	if *filename == "" {
		fmt.Println("filename was not provided. Use the -f flag")
		return
	}

	client, err := bblfsh.NewClient(*endpoint)
	if err != nil {
		panic(err)
	}

	res, err := client.NewParseRequest().Language("python").ReadFile(*filename).Do()
	if err != nil {
		panic(err)
	}

	fmt.Println(res.Errors)
	if *query == "" {
		fmt.Println(res.UAST)
		return

	}


}
