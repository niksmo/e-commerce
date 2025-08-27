package kafka

import "github.com/twmb/franz-go/pkg/kgo"

type ProductsProducer struct {
	cl *kgo.Client
}
