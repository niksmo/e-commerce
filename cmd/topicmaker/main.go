package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/niksmo/e-commerce/config"
	"github.com/niksmo/e-commerce/internal/adapter"
	"github.com/niksmo/e-commerce/pkg/sigctx"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

const (
	partitions           = 3
	replicationFactor    = 3
	deleteCleanupPolicy  = "delete"
	compactCleanupPolicy = "compact"
	minISR               = "1"
)

func main() {
	sigCtx, closeApp := sigctx.NotifyContext()
	defer closeApp()

	cfg := config.Load()

	cl := createClient(
		cfg.Broker.SeedBrokersPrimary,
		tlsCfg{
			ca:   cfg.Broker.SASLSSL.CACert,
			cert: cfg.Broker.SASLSSL.AppCert,
			Key:  cfg.Broker.SASLSSL.AppKey,
		},
		sasl{
			user: cfg.Broker.SASLSSL.AppUser,
			pass: cfg.Broker.SASLSSL.AppPass,
		},
	)
	defer cl.Close()

	printStart(cfg)
	defer printComplete(time.Now())

	// with delete cleanup policy
	err := makeTopics(
		sigCtx, cl, deleteCleanupPolicy,
		cfg.Broker.Topics.ProductsFromShop,
		cfg.Broker.Topics.ProductsToStorage,
		cfg.Broker.Topics.FilterProductStream,
		cfg.Broker.Topics.ClientFindProductEvents,
	)
	if err != nil {
		printFail(err)
		return
	}

	// with compact cleanup policy
	err = makeTopics(
		sigCtx, cl, compactCleanupPolicy,
		cfg.Broker.Topics.FilterProductTable,
	)
	if err != nil {
		printFail(err)
		return
	}
}

type tlsCfg struct {
	ca, cert, Key string
}
type sasl struct {
	user, pass string
}

func createClient(
	seedBrokers []string, tlsCfg tlsCfg, sasl sasl,
) *kadm.Client {
	tlsConfig := adapter.MakeTLSConfig(tlsCfg.ca, tlsCfg.cert, tlsCfg.Key)

	cl, err := kadm.NewOptClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.DialTLSConfig(tlsConfig),
		kgo.SASL(plain.Auth{
			User: sasl.user,
			Pass: sasl.pass,
		}.AsMechanism()),
	)
	if err != nil {
		panic(err) // develop mistake
	}
	return cl
}

func makeTopics(
	ctx context.Context, cl *kadm.Client, cleanupPolicy string, topics ...string,
) error {
	var (
		isr = minISR
	)

	config := map[string]*string{
		"cleanup.policy":      &cleanupPolicy,
		"min.insync.replicas": &isr,
	}

	responses, err := cl.CreateTopics(
		ctx,
		partitions,
		replicationFactor,
		config,
		topics...,
	)

	if err != nil {
		return err
	}

	var errs []error
	for _, res := range responses.Sorted() {
		err := res.Err
		if err != nil {
			if errors.Is(res.Err, kerr.TopicAlreadyExists) {
				fmt.Printf("topic: %q already exists\n", res.Topic)
			} else {
				errs = append(errs, err)
			}
			continue
		}
		fmt.Printf("topic: %q successfully created\n", res.Topic)
	}

	return errors.Join(errs...)
}

func printStart(cfg config.Config) {
	fmt.Printf(`initializing topics...
	- %q
	- %q
	- %q
	- %q
	- %q

`,
		cfg.Broker.Topics.ProductsFromShop,
		cfg.Broker.Topics.ProductsToStorage,
		cfg.Broker.Topics.FilterProductStream,
		cfg.Broker.Topics.FilterProductTable,
		cfg.Broker.Topics.ClientFindProductEvents,
	)
}

func printComplete(start time.Time) {
	fmt.Printf("\ncomplete in %s\n", time.Since(start))
}

func printFail(err error) {
	fmt.Printf("failed to create topics: \n%s\n", err)
}
