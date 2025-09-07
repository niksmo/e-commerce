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

type saslssl struct {
	CACert  string `mapstructure:"ca_cert"`
	AppCert string `mapstructure:"app_cert"`
	AppKey  string `mapstructure:"app_key"`
	AppUser string `mapstructure:"app_user"`
	AppPass string `mapstructure:"app_pass"`
}

type consumers struct {
	FilterProductGroup  string `mapstructure:"filter_product_group"`
	ProductBlockerGroup string `mapstructure:"product_blocker_group"`
	ProductSaverGroup   string `mapstructure:"product_saver_group"`
	ClientEventsGroup   string `mapstructure:"client_events_group"`
}

type topics struct {
	ProductsFromShop        string `mapstructure:"products_from_shop"`
	ProductsToStorage       string `mapstructure:"products_to_storage"`
	FilterProductStream     string `mapstructure:"filter_product_stream"`
	FilterProductTable      string `mapstructure:"filter_product_table"`
	ClientFindProductEvents string `mapstructure:"client_find_product_events"`
	Recommendations         string `mapstructure:"recommendations"`
}

type broker struct {
	SeedBrokersPrimary   []string  `mapstructure:"seed_brokers_primary"`
	SeedBrokersSecondary []string  `mapstructure:"seed_brokers_secondary"`
	SchemaRegistryURLs   []string  `mapstructure:"schema_registry_urls"`
	Topics               topics    `mapstructure:"topics"`
	Consumers            consumers `mapstructure:"consumers"`
	SASLSSL              saslssl   `mapstructure:"sasl_ssl"`
}

type hdfs struct {
	Addr string `mapstructure:"addr"`
	User string `mapstructure:"user"`
	Host string `mapstructure:"host"`
}

type spark struct {
	Addr string `mapstructure:"addr"`
}

type Config struct {
	LogLevel       slog.Level `mapstructure:"log_level"`
	HTTPServerAddr string     `mapstructure:"http_server_addr"`
	SQLDB          string     `mapstructure:"sql_db"`
	Broker         broker     `mapstructure:"broker"`
	HDFS           hdfs       `mapstructure:"hdfs"`
	Spark          spark      `mapstructure:"spark"`
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
	SeedBrokersPrimary=%q
	SeedBrokersSecondary=%q
	SchemaRegistryURLs=%q
	Topics:
		ProductsFromShop=%q
		ProductsToStorage=%q
		FilterProductStream=%q
		FilterProductTable=%q
		ClientFindProductEvents=%q
		Recommendations=%q
	Consumers:
		FilterProductGroup=%q
		ProductBlockerGroup=%q
		ProductSaverGroup=%q
		ClientEventsGroup=%q
	SASLSSL:
		CACert=%q
		AppCert=%q
		AppKey=%q
		AppUser=%q
		AppPass=%q
	HDFS:
		Addr=%q
		User=%q
		Host=%q
	Spark:
		Addr=%q

`
	fmt.Println("Loaded config:")
	fmt.Printf(
		strings.TrimLeft(tamplate, "\n"),
		c.LogLevel,
		c.HTTPServerAddr,
		c.SQLDB,
		c.Broker.SeedBrokersPrimary,
		c.Broker.SeedBrokersSecondary,
		c.Broker.SchemaRegistryURLs,
		c.Broker.Topics.ProductsFromShop,
		c.Broker.Topics.ProductsToStorage,
		c.Broker.Topics.FilterProductStream,
		c.Broker.Topics.FilterProductTable,
		c.Broker.Topics.ClientFindProductEvents,
		c.Broker.Consumers.FilterProductGroup,
		c.Broker.Consumers.ProductBlockerGroup,
		c.Broker.Consumers.ProductSaverGroup,
		c.Broker.Consumers.ClientEventsGroup,
		c.Broker.SASLSSL.CACert,
		c.Broker.SASLSSL.AppCert,
		c.Broker.SASLSSL.AppKey,
		c.Broker.SASLSSL.AppUser,
		c.Broker.SASLSSL.AppPass,
		c.HDFS.Addr,
		c.HDFS.User,
		c.HDFS.Host,
		c.Spark.Addr,
	)
}
