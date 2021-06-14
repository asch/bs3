// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

// bs3 is a userspace daemon using BUSE for creating a block device and S3
// protocol to communicate with object backend. It is designed for easy
// extension of all the important parts. Hence the S3 protocol can be easily
// replaced by RADOS or any other protocol.
//
// Project structure is following:
//
// - internal contains all packages used by this program. The name "internal"
// is reserved by go compiler and disallows its imports from different
// projects. Since we don't provide any reusable packages, we use internal
// directory.
//
// - internal/bs3 contains all packages related only to the bs3 implementation.
// See the package descriptions in the source code for more details.
//
// - internal/null contains trivial implementation of block device which does
// nothing but correctly. It can be used for benchmarking underlying buse
// library and kernel module. The null implementation is part of bs3 because it
// shares configuration and makes benchmarking easier and without code
// duplication.
//
// - internal/config contains configuration package which is common for both,
// bs3 and null implementations.
package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/asch/bs3/internal/bs3"
	"github.com/asch/bs3/internal/config"
	"github.com/asch/bs3/internal/null"
	"github.com/asch/buse/lib/go/buse"
)

// Parse configuration from file and environment variables, creates a
// BuseReadWriter and creates new buse device with it. The device is ran until
// it is signaled by SIGINT or SIGTERM to gracefully finish.
func main() {
	err := config.Configure()
	if err != nil {
		log.Panic().Err(err).Send()
	}

	loggerSetup(config.Cfg.Log.Pretty, config.Cfg.Log.Level)

	if config.Cfg.Profiler {
		runProfiler(config.Cfg.ProfilerPort)
	}

	buseReadWriter, err := getBuseReadWriter(config.Cfg.Null)
	if err != nil {
		log.Panic().Err(err).Send()
	}

	buse, err := buse.New(buseReadWriter, buse.Options{
		Durable:        config.Cfg.Write.Durable,
		WriteChunkSize: int64(config.Cfg.Write.ChunkSize),
		BlockSize:      int64(config.Cfg.BlockSize),
		Threads:        int(config.Cfg.Threads),
		Major:          int64(config.Cfg.Major),
		WriteShmSize:   int64(config.Cfg.Write.BufSize),
		ReadShmSize:    int64(config.Cfg.Read.BufSize),
		Size:           int64(config.Cfg.Size),
		CollisionArea:  int64(config.Cfg.Write.CollisionSize),
		QueueDepth:     int64(config.Cfg.QueueDepth),
		Scheduler:      config.Cfg.Scheduler,
	})

	if err != nil {
		log.Panic().Msg(err.Error())
	}

	log.Info().Msgf("BUSE device %d registered!", config.Cfg.Major)

	registerSigHandlers(buse)

	buse.Run()

	log.Info().Msgf("Removing buse%d", config.Cfg.Major)
	buse.RemoveDevice()
}

// Return null device if user wants it, otherwise returns bs3 device, which is
// default.
func getBuseReadWriter(wantNullDevice bool) (buse.BuseReadWriter, error) {
	if wantNullDevice {
		return null.NewNull(), nil
	}

	bs3, err := bs3.NewWithDefaults()

	return bs3, err
}

// Register handler for graceful stop when SIGINT or SIGTERM came in.
func registerSigHandlers(buse buse.Buse) {
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt)
	signal.Notify(stopChan, syscall.SIGTERM)
	go func() {
		<-stopChan
		log.Info().Msgf("Received interrupt, stopping buse%d device!", config.Cfg.Major)
		buse.StopDevice()
	}()
}

func loggerSetup(pretty bool, level int) {
	if pretty {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	zerolog.SetGlobalLevel(zerolog.Level(level))
}

// Enables remote profiling support. Useful for perfomance debugging.
func runProfiler(port int) {
	go func() {
		log.Info().Err(http.ListenAndServe(fmt.Sprintf("localhost:%d", port), nil)).Send()
	}()
}
