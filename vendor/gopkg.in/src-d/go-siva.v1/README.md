# śiva format शिव [![GoDoc](https://godoc.org/gopkg.in/src-d/go-siva.v1?status.svg)](https://godoc.org/gopkg.in/src-d/go-siva.v1) [![Build Status](https://travis-ci.org/src-d/go-siva.svg?branch=master)](https://travis-ci.org/src-d/go-siva) [![codebeat badge](https://codebeat.co/badges/a821494a-ff72-4756-9a70-652436e93485)](https://codebeat.co/projects/github-com-src-d-go-siva)

_śiva_ stand for <b>s</b>eekable <b>i</b>ndexed <b>b</b>lock <b>a</b>rchiver

_śiva_ is archive format very similar to tar or zip, focused on allowing: constant-time random file access, seekable access to the contained files and concatenable archive files 

![siva](https://cloud.githubusercontent.com/assets/1573114/19213424/8a97b7ee-8d6c-11e6-9c84-ddb58862dd94.png)

The library implements a very similar API to the go [tar package](https://golang.org/pkg/archive/tar/), allowing full control over and low level access to the contained files.

- [Library reference](http://godoc.org/gopkg.in/src-d/go-siva.v1)
- [Command-line interface](#cli)
- [Format specification](https://github.com/src-d/go-siva/blob/master/SPEC.md)


Installation
------------

The recommended way to install siva

```
go get -u gopkg.in/src-d/go-siva.v1/...
```

Example
-------

Creating a siva file:

```go
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
``` 


Reading from a siva file: 
```go
// Open the siva archive for reading.
file := bytes.NewReader(buf.Bytes())
r := siva.NewReader(file)

// Get all the files in the siva file.
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
```


<a name="cli"></a>Command-line interface
----------------------
siva cli interface, is a convenient command that helps you to creates and manipulates siva files.

Output from: `./siva --help`:

```
Usage:
  siva [OPTIONS] <command>

Help Options:
  -h, --help  Show this help message

Available commands:
  list     List the items contained on a file.
  pack     Create a new archive containing the specified items.
  unpack   Extract to disk from the archive.
  version  Show the version information.
```

License
-------

MIT, see [LICENSE](LICENSE)
