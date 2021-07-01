.PHONY = install fmt tidy clean

SOURCES := $(shell find . -name "*.go")
SYSTEMD_UNITS := $(wildcard contrib/systemd/*)
SYSTEMD_PATH := /etc/systemd/system
SYSTEMD_CONTRIB_PATH := contrib/systemd

bs3: $(SOURCES)
	go build

install: bs3 $(SYSTEMD_UNITS)
	install -D bs3 /usr/local/bin/bs3
	install -D -m 600 config.toml /etc/bs3/config.toml
	install -D -m 644 $(SYSTEMD_CONTRIB_PATH)/bs3.service $(SYSTEMD_PATH)/bs3.service
	install -D -m 644 $(SYSTEMD_CONTRIB_PATH)/bs3-gc.service $(SYSTEMD_PATH)/bs3-gc.service
	install -D -m 644 $(SYSTEMD_CONTRIB_PATH)/bs3-gc.timer $(SYSTEMD_PATH)/bs3-gc.timer

fmt:
	go fmt ./...

tidy:
	go mod tidy

clean:
	rm -f bs3
