package sinks

import (
	"github.com/shayonj/pg_flo/pkg/utils"
)

type Sink interface {
	WriteBatch(data []*utils.CDCMessage) error
}
