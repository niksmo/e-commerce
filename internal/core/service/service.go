package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

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
	evtProc               *clientEventWorker
	evtC                  chan<- domain.ClientFindProductEvent
}

func New(
	productFilterProc port.ProductFilterProcessor,
	productBlockerProc port.ProductBlockerProcessor,
	productsProducer port.ProductsProducer,
	productsFilterProducer port.ProductFilterProducer,
	productsStorage port.ProductsStorage,
	productReader port.ProductReader,
	findProductEventEmitter port.ClientFindProductEventEmitter,
	clientEventsStorage port.ClientEventsStorage,
) *Service {
	evtProc := &clientEventWorker{emitter: findProductEventEmitter}
	return &Service{
		productFilterProc:     productFilterProc,
		productBlockerProc:    productBlockerProc,
		productsProducer:      productsProducer,
		productFilterProducer: productsFilterProducer,
		productsStorage:       productsStorage,
		clientEventsStorage:   clientEventsStorage,
		productReader:         productReader,
		evtProc:               evtProc,
	}
}

// Run runs the services components in separate goroutines.
//
// Blocks current goroutine while components is preparing to ready state.
func (s *Service) Run(ctx context.Context, stopFn context.CancelFunc) {
	const op = "Service.Run"
	log := slog.With("op", op)

	s.evtC = s.evtProc.run(ctx)

	var wg sync.WaitGroup
	wg.Add(2)
	go s.productFilterProc.Run(ctx, stopFn, &wg)
	go s.productBlockerProc.Run(ctx, stopFn, &wg)
	wg.Wait()

	log.Info("running")
}

func (s *Service) Close() {
	const op = "Service.Close"
	log := slog.With("op", op)

	log.Info("service is closing...")
	s.productFilterProc.Close()
	s.productBlockerProc.Close()
	s.evtProc.close()
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
		s.evtC <- productToEvt(user, v)
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

// A clientEventWorker is used for process client event in core service.
type clientEventWorker struct {
	evtC    chan domain.ClientFindProductEvent
	emitter port.ClientFindProductEventEmitter
}

func (p *clientEventWorker) run(
	ctx context.Context,
) chan<- domain.ClientFindProductEvent {
	p.evtC = make(chan domain.ClientFindProductEvent, 1)
	go p.runProc(ctx)
	return p.evtC
}

func (p *clientEventWorker) runProc(ctx context.Context) {
	const op = "Service.clientEventWorker.runProc"
	log := slog.With("op", op)

	for evt := range p.evtC {
		err := p.emitter.Emit(ctx, evt)
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

func (p *clientEventWorker) close() {
	close(p.evtC)
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
