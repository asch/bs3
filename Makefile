build: bs3

bs3:
	go build

install: bs3
	install -D bs3 /usr/local/bin/bs3
	install -D -m 600 config.toml /etc/bs3/config.toml
	install -D -m 644 contrib/systemd/bs3.service /etc/systemd/system/bs3.service
	install -D -m 644 contrib/systemd/bs3-gc.service /etc/systemd/system/bs3-gc.service
	install -D -m 644 contrib/systemd/bs3-gc.timer /etc/systemd/system/bs3-gc.timer

fmt:
	go fmt ./...

tidy:
	go mod tidy

clean:
	rm bs3
