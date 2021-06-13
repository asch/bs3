install: build
	sudo cp bs3 /usr/local/bin/

fmt:
	go fmt ./...

tidy:
	go mod tidy

build:
	go build

clean:
	rm bs3
