module github.com/asch/bs3

go 1.16

require (
	github.com/asch/buse/lib/go/buse v0.0.0-20210613113039-eca88af516b2
	github.com/aws/aws-sdk-go v1.38.60
	github.com/ilyakaznacheev/cleanenv v1.2.5
	github.com/rs/zerolog v1.22.0
	golang.org/x/net v0.0.0-20210610132358-84b48f89b13b
	libguestfs.org/libnbd v1.0.0
)

replace (
	github.com/asch/bs3/internal/nbd => ./internal/nbd
	github.com/asch/buse/lib/go/buse => ../buse/lib/go/buse
	libguestfs.org/libnbd => ./mod/libguestfs.org/libnbd@v1.11.3-4-g3226ff802/
)
