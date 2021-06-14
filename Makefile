install: bs3
	sudo cp bs3 /usr/local/bin/
	sudo mkdir -p /etc/bs3
	sudo cp config.toml /etc/bs3/
	sudo cp contrib/systemd/bs3.service /etc/systemd/system/

fmt:
	go fmt ./...

tidy:
	go mod tidy

bs3:
	go build

clean:
	rm bs3
