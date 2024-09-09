package sinks

import (
	"github.com/jackc/pglogrepl"
	"github.com/shayonj/pg_flo/pkg/utils"
)

type Sink interface {
	WriteBatch(data []*utils.CDCMessage) error
	GetLastLSN() (pglogrepl.LSN, error)
	SetLastLSN(lsn pglogrepl.LSN) error
	Close() error
}
