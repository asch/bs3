// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

// Package config is a singleton and provides global access to the
// configuration values.
package config

import (
	"flag"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

const (
	// Default config path. It does not need to exist, default values for all parameters will be
	// used instead.
	defaultConfig = "/etc/bs3/config.toml"
)

var Cfg Config

// Configuration structure for the program. We use toml format for file-based
// configuration and also all configuration options can be overriden by
// environment variable specified in this structure.
type Config struct {
	ConfigPath string

	Null       bool  `toml:"null" env:"BS3_NULL" env-default:"false" env-description:"Use null backend, i.e. immediate acknowledge to read or write. For testing BUSE raw performance."`
	Major      int   `toml:"major" env:"BS3_MAJOR" env-default:"0" env-description:"Device major. Decimal part of /dev/buse%d."`
	Threads    int   `toml:"threads" env:"BS3_THREADS" env-default:"0" env-description:"Number of user-space threads for serving queues."`
	Size       int64 `toml:"size" env:"BS3_SIZE" env-default:"8" env-description:"Device size in GB."`
	BlockSize  int   `toml:"block_size" env:"BS3_BLOCKSIZE" env-default:"4096" env-description:"Block size."`
	Scheduler  bool  `toml:"scheduler" env:"BS3_SCHEDULER" env-default:"false" env-description:"Use block layer scheduler."`
	QueueDepth int   `toml:"queue_depth" env:"BS3_QUEUEDEPTH" env-default:"128" env-description:"Device IO queue depth."`

	S3 struct {
		Bucket      string `toml:"bucket" env:"BS3_S3_BUCKET" env-description:"S3 Bucket name." env-default:"bs3"`
		Remote      string `toml:"remote" env:"BS3_S3_REMOTE" env-description:"S3 Remote address. Empty string for AWS S3 endpoint." env-default:""`
		Region      string `toml:"region" env:"BS3_S3_REGION" env-description:"S3 Region." env-default:"us-east-1"`
		AccessKey   string `toml:"access_key" env:"BS3_S3_ACCESSKEY" env-description:"S3 Access Key." env-default:""`
		SecretKey   string `toml:"secret_key" env:"BS3_S3_SECRETKEY" env-description:"S3 Secret Key." env-default:""`
		Uploaders   int    `toml:"uploaders" env:"BS3_S3_UPLOADERS" env-description:"S3 Max number of uploader threads." env-default:"16"`
		Downloaders int    `toml:"downloaders" env:"BS3_S3_DOWNLOADERS" env-description:"S3 Max number of downloader threads." env-default:"16"`
	} `toml:"s3"`

	Write struct {
		Durable       bool `toml:"durable" env:"BS3_WRITE_DURABLE" env-description:"Flush semantics. True means durable, false means barrier only." env-default:"false"`
		BufSize       int  `toml:"shared_buffer_size" env:"BS3_WRITE_BUFSIZE" env-description:"Write shared memory size in MB." env-default:"32"`
		ChunkSize     int  `toml:"chunk_size" env:"BS3_WRITE_CHUNKSIZE" env-description:"Chunk size in MB." env-default:"4"`
		CollisionSize int  `toml:"collision_chunk_size" env:"BS3_WRITE_COLSIZE" env-description:"Collision size in MB." env-default:"1"`
	} `toml:"write"`

	Read struct {
		BufSize int `toml:"shared_buffer_size" env:"BS3_READ_BUFSIZE" env-description:"Read shared memory size in MB." env-default:"32"`
	} `toml:"read"`

	GC struct {
		Step          int64   `toml:"step" env:"BS3_GC_STEP" env-description:"Step for traversing the extent map for living extents. In blocks." env-default:"1024"`
		LiveData      float64 `toml:"live_data" env:"BS3_GC_LIVEDATA" env-description:"Live data ratio threshold for threshold GC. This is for the threshold GC which is triggered by the user or systemd timer." env-default:"0.3"`
		IdleTimeoutMs int64   `toml:"idle_timeout" env:"BS3_GC_IDLETIMEOUT" env-description:"Idle timeout for running GC requests. In ms." env-default:"200"`
		Wait          int64   `toml:"wait" env:"BS3_GC_WAIT" env-description:"How many seconds wait before next dead GC round. This just for cleaning dead objects with minimal performance impact." env-default:"600"`
	} `toml:"gc"`

	Log struct {
		Level  int  `toml:"level" env:"BS3_LOG_LEVEL" env-description:"Log level." env-default:"-1"`
		Pretty bool `toml:"pretty" env:"BS3_LOG_PRETTY" env-description:"Pretty logging." env-default:"true"`
	} `toml:"log"`

	SkipCheckpoint bool `toml:"skip_checkpoint" env:"BS3_SKIP" env-description:"Skip restoring from and creating checkpoint." env-default:"false"`
	Profiler       bool `toml:"profiler" env:"BS3_PROFILER" env-description:"Enable golang web profiler." env-default:"false"`
	ProfilerPort   int  `toml:"profiler_port" env:"BS3_PROFILER_PORT" env-description:"Port to listen on." env-default:"6060"`
}

// Configure reads commandline flags and handles the configuration. The
// configuration file has the lower priotiry and the environment variables have
// the highest priority. It is perfetcly to fine to use just one of these or to
// combine them.
func Configure() error {
	flagSetup()
	err := parse()

	return err
}

// Parse the configuration file and reads the environment variable. After that
// it does some values postprocessing and fills the Cfg structure.
func parse() error {
	if err := cleanenv.ReadConfig(Cfg.ConfigPath, &Cfg); err != nil {
		if err := cleanenv.ReadEnv(&Cfg); err != nil {
			return err
		}
	}

	Cfg.Size *= 1024 * 1024 * 1024
	Cfg.Write.BufSize *= 1024 * 1024
	Cfg.Write.ChunkSize *= 1024 * 1024
	Cfg.Write.CollisionSize *= 1024 * 1024
	Cfg.Read.BufSize *= 1024 * 1024

	if Cfg.BlockSize != 512 {
		Cfg.BlockSize = 4096
	}

	return nil
}

// Handle program flags.
func flagSetup() {
	f := flag.NewFlagSet("bs3", flag.ExitOnError)
	f.StringVar(&Cfg.ConfigPath, "c", defaultConfig, "Path to configuration file")
	f.Usage = cleanenv.FUsage(f.Output(), &Cfg, nil, f.Usage)
	f.Parse(os.Args[1:])
}
