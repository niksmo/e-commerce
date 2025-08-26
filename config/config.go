package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type brokerConfig struct {
	SeedBrokers        []string `mapstructure:"seed_brokers"`
	CARootCert         string   `mapstructure:"ca_root_cert"`
	User               string   `mapstructure:"user"`
	Pass               string   `mapstructure:"pass"`
	SchemaRegistryURLs []string `mapstructure:"schema_registry_urls"`
}

type Config struct {
	LogLevel slog.Level   `mapstructure:"log_level"`
	Broker   brokerConfig `mapstructure:"broker"`
}

func Load() Config {
	viper.SetConfigFile(getConfigFilepath())

	err := viper.ReadInConfig()
	if err != nil {
		die(err)
	}

	var cfg Config
	err = viper.UnmarshalExact(&cfg)
	if err != nil {
		die(err)
	}

	print(cfg)

	return cfg
}

func getConfigFilepath() string {
	cmdLine := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	arg := cmdLine.String("config", "/config.yaml", "config file")
	_ = cmdLine.Parse(os.Args[1:])
	env, ok := os.LookupEnv("ECOM_CONFIG_FILE")
	if ok {
		return env
	}
	return *arg
}

func die(err error) {
	fmt.Printf("failed to load config file: %v\n", err)
	os.Exit(2)
}

func print(c Config) {
	tamplate := `
	LogLevel=%q
	SeedBrokers=%q
	CARootCert=%q
	User=%q
	Pass=%q
	SchemaRegistryURLs=%q

`
	fmt.Println("Loaded config:")
	fmt.Printf(
		strings.TrimLeft(tamplate, "\n"),
		c.LogLevel,
		c.Broker.SeedBrokers,
		c.Broker.CARootCert,
		c.Broker.User,
		c.Broker.Pass,
		c.Broker.SchemaRegistryURLs,
	)
}
