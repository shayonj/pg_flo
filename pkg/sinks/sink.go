package sinks

import (
	"github.com/jackc/pglogrepl"
)

type Sink interface {
	WriteBatch(data []interface{}) error
	GetLastLSN() (pglogrepl.LSN, error)
	SetLastLSN(lsn pglogrepl.LSN) error
	Close() error
}
