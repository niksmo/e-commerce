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
	SeedBrokers             []string `mapstructure:"seed_brokers"`
	SchemaRegistryURLs      []string `mapstructure:"schema_registry_urls"`
	ShopProductsTopic       string   `mapstructure:"shop_products_topic"`
	FilterProductStream     string   `mapstructure:"filter_product_stream"`
	FilterProductGroupTable string   `mapstructure:"filter_product_group_table"`
	SaveProductsTopic       string   `mapstructure:"save_products_topic"`
	ClientEventsTopic       string   `mapstructure:"client_events_topic"`
}

type Config struct {
	LogLevel       slog.Level   `mapstructure:"log_level"`
	HTTPServerAddr string       `mapstructure:"http_server_addr"`
	Broker         brokerConfig `mapstructure:"broker"`
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

func (c Config) Print() {
	tamplate := `
	LogLevel=%q
	HTTPServerAddr=%q
	SeedBrokers=%q
	SchemaRegistryURLs=%q
	ShopProductsTopic=%q
	FilterProductStream=%q
	FilterProductGroupTable=%q
	SaveProductsTopic=%q
	ClientEventsTopic=%q

`
	fmt.Println("Loaded config:")
	fmt.Printf(
		strings.TrimLeft(tamplate, "\n"),
		c.LogLevel,
		c.HTTPServerAddr,
		c.Broker.SeedBrokers,
		c.Broker.SchemaRegistryURLs,
		c.Broker.ShopProductsTopic,
		c.Broker.FilterProductStream,
		c.Broker.FilterProductGroupTable,
		c.Broker.SaveProductsTopic,
		c.Broker.ClientEventsTopic,
	)
}
