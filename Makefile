.PHONY = install fmt tidy clean

SOURCES := $(shell find . -name "*.go")
SYSTEMD_UNITS := $(wildcard contrib/systemd/*)

bs3: $(SOURCES)
	go build

install: bs3 $(SYSTEMD_UNITS)
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
	rm -f bs3
