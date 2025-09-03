package storage

import (
	"context"
	"fmt"

	"github.com/niksmo/e-commerce/internal/core/domain"
)

type ClientEventsRepository struct {
	hdfs hdfs
}

func NewClientEventsRepository(hdfs hdfs) ClientEventsRepository {
	return ClientEventsRepository{hdfs}
}

func (r ClientEventsRepository) StoreEvents(
	ctx context.Context, username string, evts []domain.ClientFindProductEvent,
) error {
	const op = "ClientEventsRepository.StoreEvents"

	filename := r.getFileName(username)
	stat, err := r.hdfs.Stat(filename)
	if err != nil {
		// if not exists > create file
		// if exists > append
		// else return err
		return fmt.Errorf("%s: %w", op, err)
	}

	// maybe save by json.Encoder
	// maybe csv

	fmt.Printf("fileStat: %#v\n", stat)
	return nil
}

func (r ClientEventsRepository) getFileName(username string) string {
	return username
}
