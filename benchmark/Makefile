${GOPATH}/src/github.com/src-d/datasets/PublicGitArchive/pga:
	go get -u github.com/src-d/datasets/PublicGitArchive/pga/...;

dependencies: ${GOPATH}/src/github.com/src-d/datasets/PublicGitArchive/pga

install:
	go build -o benchmark .;

.pga: dependencies
	mkdir .pga;
	cat list.txt | pga get -i -o ./.pga;

list.txt: dependencies
	./make_dataset.sh;

dataset: list.txt .pga

clean:
	rm -rf list.txt .pga;

benchmark: install dataset
	./benchmark;