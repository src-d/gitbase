package siva_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"gopkg.in/src-d/go-siva.v1"
)

func Example() {
	// Create a buffer to write our archive to.
	buf := new(bytes.Buffer)

	// Create a new siva archive.
	w := siva.NewWriter(buf)

	// Add some files to the archive.
	var files = []struct {
		Name, Body string
	}{
		{"readme.txt", "This archive contains some text files."},
		{"gopher.txt", "Gopher names:\nGeorge\nGeoffrey\nGonzo"},
		{"todo.txt", "Get animal handling license."},
	}
	for _, file := range files {
		hdr := &siva.Header{
			Name:    file.Name,
			Mode:    0600,
			ModTime: time.Now(),
		}
		if err := w.WriteHeader(hdr); err != nil {
			log.Fatalln(err)
		}
		if _, err := w.Write([]byte(file.Body)); err != nil {
			log.Fatalln(err)
		}
	}
	// Make sure to check the error on Close.
	if err := w.Close(); err != nil {
		log.Fatalln(err)
	}

	// Open the siva archive for reading.
	file := bytes.NewReader(buf.Bytes())
	r := siva.NewReader(file)

	// Get all files in the siva file.
	i, err := r.Index()
	if err != nil {
		log.Fatalln(err)
	}

	// Iterate through the files in the archive.
	for _, e := range i {
		content, err := r.Get(e)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("Contents of %s:\n", e.Name)
		if _, err := io.Copy(os.Stdout, content); err != nil {
			log.Fatalln(err)
		}
		fmt.Println()
	}

	// Output:
	// Contents of readme.txt:
	// This archive contains some text files.
	// Contents of gopher.txt:
	// Gopher names:
	// George
	// Geoffrey
	// Gonzo
	// Contents of todo.txt:
	// Get animal handling license.
}
