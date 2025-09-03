package storage

import (
	"context"

	"github.com/niksmo/e-commerce/internal/core/domain"
)

type ClientEventsRepository struct {
	hdfs hdfs
}

func NewClientEventsRepository(hdfs hdfs) ClientEventsRepository {
	return ClientEventsRepository{hdfs}
}

func (r ClientEventsRepository) StoreEvents(
	ctx context.Context, evts []domain.ClientFindProductEvent,
) error {
	const op = "ClientEventsRepository.StoreEvents"

	return nil
}
