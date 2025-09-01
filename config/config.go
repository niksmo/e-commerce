package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const configFileEnvName = "ECOM_CONFIG_FILE"

type consumers struct {
	FilterProductGroup  string `mapstructure:"filter_product_group"`
	ProductBlockerGroup string `mapstructure:"product_blocker_group"`
	ProductSaverGroup   string `mapstructure:"product_saver_group"`
}

type topics struct {
	ProductsFromShop    string `mapstructure:"products_from_shop"`
	ProductsToStorage   string `mapstructure:"products_to_storage"`
	FilterProductStream string `mapstructure:"filter_product_stream"`
	FilterProductTable  string `mapstructure:"filter_product_table"`
	ClientEvents        string `mapstructure:"client_events"`
}

type broker struct {
	SeedBrokers        []string  `mapstructure:"seed_brokers"`
	SchemaRegistryURLs []string  `mapstructure:"schema_registry_urls"`
	Topics             topics    `mapstructure:"topics"`
	Consumers          consumers `mapstructure:"consumers"`
}

type Config struct {
	LogLevel       slog.Level `mapstructure:"log_level"`
	HTTPServerAddr string     `mapstructure:"http_server_addr"`
	SQLDB          string     `mapstructure:"sql_db"`
	Broker         broker     `mapstructure:"broker"`
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
	env, ok := os.LookupEnv(configFileEnvName)
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
	General:
	LogLevel=%q
	HTTPServerAddr=%q
	SQLDB=%q

	BrokerConfig:
	SeedBrokers=%q
	SchemaRegistryURLs=%q
	Topics:
		ProductsFromShop=%q
		ProductsToStorage=%q
		FilterProductStream=%q
		FilterProductTable=%q
		ClientEvents=%q
	Consumers:
		FilterProductGroup=%q
		ProductBlockerGroup=%q
		ProductSaverGroup=%q

`
	fmt.Println("Loaded config:")
	fmt.Printf(
		strings.TrimLeft(tamplate, "\n"),
		c.LogLevel,
		c.HTTPServerAddr,
		c.SQLDB,
		c.Broker.SeedBrokers,
		c.Broker.SchemaRegistryURLs,
		c.Broker.Topics.ProductsFromShop,
		c.Broker.Topics.ProductsToStorage,
		c.Broker.Topics.FilterProductStream,
		c.Broker.Topics.FilterProductTable,
		c.Broker.Topics.ClientEvents,
		c.Broker.Consumers.FilterProductGroup,
		c.Broker.Consumers.ProductBlockerGroup,
		c.Broker.Consumers.ProductSaverGroup,
	)
}
