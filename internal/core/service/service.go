package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/niksmo/e-commerce/internal/adapter/storage"
	"github.com/niksmo/e-commerce/internal/core/domain"
	"github.com/niksmo/e-commerce/internal/core/port"
)

var (
	ErrNotFound = errors.New("not found")
)

type Service struct {
	productFilterProc     port.ProductFilterProcessor
	productBlockerProc    port.ProductBlockerProcessor
	productsProducer      port.ProductsProducer
	productFilterProducer port.ProductFilterProducer
	productsStorage       port.ProductsStorage
	clientEventsStorage   port.ClientEventsStorage
	productReader         port.ProductReader
	clientEventsWorker    clientEventWorker
	analyticsWorker       analyticsWorker
}

type ServiceConfig struct {
	ProductFilterProc        port.ProductFilterProcessor
	ProductBlockerProc       port.ProductBlockerProcessor
	ProductsProducer         port.ProductsProducer
	ProductsFilterProducer   port.ProductFilterProducer
	ProductsStorage          port.ProductsStorage
	ProductReader            port.ProductReader
	FindProductEventProducer port.ClientFindProductEventProducer
	ClientEventsStorage      port.ClientEventsStorage
	ClientEventsAnalyzer     port.ClientEventsAnalyzer
	ProductOfferProducer     port.ProductOfferProducer
}

func New(
	config ServiceConfig,
) *Service {
	clientEventsWorker := newClientEventWorker(
		config.FindProductEventProducer,
	)

	analyticsWorker := newAnalyticsWorker(
		config.ClientEventsStorage,
		config.ClientEventsAnalyzer,
		config.ProductOfferProducer,
	)

	return &Service{
		productFilterProc:     config.ProductFilterProc,
		productBlockerProc:    config.ProductBlockerProc,
		productsProducer:      config.ProductsProducer,
		productFilterProducer: config.ProductsFilterProducer,
		productsStorage:       config.ProductsStorage,
		clientEventsStorage:   config.ClientEventsStorage,
		productReader:         config.ProductReader,
		clientEventsWorker:    clientEventsWorker,
		analyticsWorker:       analyticsWorker,
	}
}

// Run runs the services components in separate goroutines.
//
// Blocks current goroutine while components is preparing to ready state.
func (s *Service) Run(ctx context.Context, stopFn context.CancelFunc) {
	const op = "Service.Run"
	log := slog.With("op", op)

	var wg sync.WaitGroup
	wg.Add(2)
	go s.productFilterProc.Run(ctx, stopFn, &wg)
	go s.productBlockerProc.Run(ctx, stopFn, &wg)
	wg.Wait()

	go s.clientEventsWorker.run(ctx)
	go s.analyticsWorker.run(ctx)

	log.Info("running")
}

func (s *Service) Close() {
	const op = "Service.Close"
	log := slog.With("op", op)

	log.Info("service is closing...")
	s.productFilterProc.Close()
	s.productBlockerProc.Close()
	s.clientEventsWorker.close()
	log.Info("service is closed")
}

func (s *Service) SendProducts(ctx context.Context, vs []domain.Product) error {
	const op = "Service.SendProducts"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.productsProducer.ProduceProducts(ctx, vs)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (s *Service) SetRule(ctx context.Context, v domain.ProductFilter) error {
	const op = "Service.SetRule"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.productFilterProducer.ProduceFilter(ctx, v)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s *Service) SaveProducts(ctx context.Context, vs []domain.Product) error {
	const op = "Service.SaveProducts"
	log := slog.With("op", op)

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.productsStorage.StoreProducts(ctx, vs)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	var names []string
	for _, v := range vs {
		names = append(names, v.Name)
	}
	n := len(vs)
	log.Info("products saved", "nProducts", n, "products", names)
	return nil
}

func (s *Service) FindProduct(
	ctx context.Context, productName string, user string,
) (domain.Product, error) {
	const op = "Service.FindProduct"

	if err := ctx.Err(); err != nil {
		return domain.Product{}, fmt.Errorf("%s: %w", op, err)
	}

	v, err := s.productReader.ReadProduct(ctx, productName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return domain.Product{}, fmt.Errorf("%s: %w", op, ErrNotFound)
		}
		return domain.Product{}, fmt.Errorf("%s: %w", op, err)
	}

	if userEvent(user) {
		evt := productToEvt(user, v)
		s.emitEvent(evt)
	}

	return v, nil
}

func (s *Service) SaveEvents(
	ctx context.Context, evts []domain.ClientFindProductEvent,
) error {
	const op = "Service.SaveEvents"
	log := slog.With("op", op)

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if len(evts) == 0 {
		return nil
	}

	username := evts[0].Username
	err := s.clientEventsStorage.StoreEvents(ctx, username, evts)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	log.Info("client events saved", "nEvents", len(evts))
	return nil
}

func (s *Service) emitEvent(evt domain.ClientFindProductEvent) {
	s.clientEventsWorker.evtStream <- evt
}

// A analyticsWorker is used in core service for process client events.
type analyticsWorker struct {
	clientEventsStorage port.ClientEventsStorage
	analyzer            port.ClientEventsAnalyzer
	producer            port.ProductOfferProducer
}

func newAnalyticsWorker(
	clientEventsStorage port.ClientEventsStorage,
	analyzer port.ClientEventsAnalyzer,
	producer port.ProductOfferProducer,

) analyticsWorker {
	return analyticsWorker{
		clientEventsStorage: clientEventsStorage,
		analyzer:            analyzer,
		producer:            producer,
	}
}

func (w *analyticsWorker) run(ctx context.Context) {
	const op = "analyticsWorker.run"
	log := slog.With("op", op)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Info("analytics tick")
			srcPaths := w.clientEventsStorage.DataPaths()

			if len(srcPaths) == 0 {
				log.Info("no data for product offers analytics")
				continue
			}

			stream := w.analyzer.Do(ctx, srcPaths)
			for v := range stream {
				log.Info("make product offers", "username", v.Username)
				w.saveProductOffers(ctx, v)
			}
		}
	}
}

func (w *analyticsWorker) saveProductOffers(
	ctx context.Context, v domain.ProductOffer,
) {
	const op = "analyticsWorker.saveProductOffers"
	log := slog.With("op", op, "username", v.Username)
	err := w.producer.Produce(ctx, v)
	if err != nil {
		log.Error("failed to produce product offer", "err", err)
		return
	}
	log.Info("produce product offers")
}

// A clientEventWorker is used in core service for produce client event.
type clientEventWorker struct {
	evtStream chan domain.ClientFindProductEvent
	emitter   port.ClientFindProductEventProducer
}

func newClientEventWorker(
	emitter port.ClientFindProductEventProducer,
) clientEventWorker {
	return clientEventWorker{
		emitter:   emitter,
		evtStream: make(chan domain.ClientFindProductEvent, 1),
	}
}

func (w clientEventWorker) run(ctx context.Context) {
	const op = "Service.clientEventWorker.run"
	log := slog.With("op", op)

	for evt := range w.evtStream {
		err := w.emitter.Produce(ctx, evt)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			log.Error("failed to emit clients find product event", "err", err)
			return
		}
		log.Info("emit clients find product event", "user", evt.Username)
	}
}

func (w clientEventWorker) close() {
	close(w.evtStream)
}

func userEvent(user string) bool {
	return user != ""
}

func productToEvt(
	username string, v domain.Product,
) (evt domain.ClientFindProductEvent) {
	evt.Username = username
	evt.ProductName = v.Name
	evt.Brand = v.Brand
	evt.Category = v.Category
	evt.Price = v.Price
	evt.Tags = v.Tags
	evt.Specifications = v.Specifications
	evt.StoreID = v.StoreID

	return
}
