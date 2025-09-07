package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/lovoo/goka"
	"github.com/niksmo/e-commerce/pkg/schema"
)

// A productOfferCodec used for serde [schema.ProductOfferV1]
type productOfferCodec struct {
	serdeEncode Serde
	serdeDecode Serde
}

func newProductOfferCodec(sEncode, sDecode Serde) productOfferCodec {
	return productOfferCodec{
		serdeEncode: sEncode,
		serdeDecode: sDecode,
	}
}

func (c productOfferCodec) Encode(v any) ([]byte, error) {
	const op = "productOfferCodec.Encode"
	if _, ok := v.(schema.ProductOfferV1); !ok {
		return nil, opErr(ErrInvalidValueType, op)
	}
	return c.serdeEncode.Encode(v)
}

func (c productOfferCodec) Decode(data []byte) (any, error) {
	const op = "productEventCodec.Decode"
	var s schema.ProductOfferV1
	err := c.serdeDecode.Decode(data, &s)
	if err != nil {
		return nil, opErr(err, op)
	}
	return s, err
}

// A ProductOffersViewConfig used for setup [ProductOffersView].
//
// All fields are required.
type ProductOffersViewConfig struct {
	SeedBrokers []string
	Topic       string
	Encoder     Serde
	Decoder     Serde
	TLSConfig   *tls.Config
	User        string
	Pass        string
}

type ProductOffersView struct {
	gv *goka.View
}

func NewProductOffersView(
	config ProductOffersViewConfig,
) (ProductOffersView, error) {
	const op = "NewProductOffersView"

	applySASLTLS(config.TLSConfig, config.User, config.Pass)

	gv, err := goka.NewView(
		config.SeedBrokers,
		goka.Table(config.Topic),
		newProductOfferCodec(config.Encoder, config.Decoder),
	)
	if err != nil {
		return ProductOffersView{}, opErr(err, op)
	}

	return ProductOffersView{gv}, nil
}

func (v *ProductOffersView) Run(ctx context.Context) {
	const op = "ProductOffersView.Run"
	log := slog.With("op", op)

	err := v.gv.Run(ctx)
	if err != nil {
		log.Error("unexpected fail on run", "err", err)
	}
}

func (v *ProductOffersView) GetOffers(username string) {
	const op = "ProductOffersView.GetOffers"
	log := slog.With("op", op)

	offers, err := v.gv.Get(username)
	if err != nil {
		log.Error("failed to get view data", "err", err)
		return
	}

	if offers == nil {
		log.Warn("no content")
		return
	}

	offerst, ok := offers.([]schema.ProductOfferV1)
	if !ok {
		log.Error("unexpected type of data", "type", fmt.Sprintf("%T", offers))
		return
	}

	fmt.Println("OFFERS*****", offerst)
}
