package sinks

import "github.com/jackc/pglogrepl"

type Status struct {
	LastLSN pglogrepl.LSN `json:"last_lsn"`
}
