package sinks

import (
	"github.com/pgflo/pg_flo/pkg/utils"
)

type Sink interface {
	WriteBatch(data []*utils.CDCMessage) error
}
