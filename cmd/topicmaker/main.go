package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lovoo/goka"
	"github.com/niksmo/e-commerce/config"
	"github.com/niksmo/e-commerce/pkg/sigctx"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	partitions        = 3
	replicationFactor = 3
	delete            = "delete"
	compact           = "compact"
)

func main() {
	sigCtx, closeApp := sigctx.NotifyContext()
	defer closeApp()

	cfg := config.Load()

	cl := createClient(cfg.Broker.SeedBrokers)
	defer cl.Close()

	printStart(cfg)
	defer printComplete(time.Now())

	// regular topics
	err := makeTopics(
		sigCtx, cl, delete,
		cfg.Broker.ShopProductsTopic,
		cfg.Broker.FilterProductStream,
		cfg.Broker.SaveProductsTopic,
	)
	if err != nil {
		printFail(err)
		return
	}

	// group table topics
	err = makeTopics(
		sigCtx, cl, compact,
		toGroupTable(cfg.Broker.FilterProductGroupTable),
	)
	if err != nil {
		printFail(err)
		return
	}
}

func createClient(seedBrokers []string) *kadm.Client {
	cl, err := kadm.NewOptClient(
		kgo.SeedBrokers(seedBrokers...),
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
		minISR = "1"
	)

	config := map[string]*string{
		"cleanup.policy":      &cleanupPolicy,
		"min.insync.replicas": &minISR,
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

`,
		cfg.Broker.ShopProductsTopic,
		cfg.Broker.FilterProductStream,
		cfg.Broker.FilterProductGroupTable,
	)
}

func printComplete(start time.Time) {
	fmt.Printf("\ncomplete in %s\n", time.Since(start))
}

func printFail(err error) {
	fmt.Printf("failed to create topics: \n%s\n", err)
}

func toGroupTable(topic string) string {
	return string(goka.GroupTable(goka.Group(topic)))
}
