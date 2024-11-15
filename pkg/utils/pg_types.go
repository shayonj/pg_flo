package utils

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

var defaultTypeMap = pgtype.NewMap()

type PostgresValue struct {
	Oid   uint32
	Data  []byte
	Value interface{}
}

func NewPostgresValue(oid uint32, value interface{}) (*PostgresValue, error) {
	if value == nil {
		return &PostgresValue{
			Oid:   oid,
			Data:  nil,
			Value: nil,
		}, nil
	}

	// Handle raw WAL data ([]byte)
	if rawData, ok := value.([]byte); ok {
		dt, ok := defaultTypeMap.TypeForOID(oid)
		if !ok {
			return nil, fmt.Errorf("unknown type OID: %d", oid)
		}

		// Try text format first for WAL data
		decoded, err := dt.Codec.DecodeValue(defaultTypeMap, oid, pgtype.TextFormatCode, rawData)
		if err != nil {
			// If text format fails, try binary format
			decoded, err = dt.Codec.DecodeValue(defaultTypeMap, oid, pgtype.BinaryFormatCode, rawData)
			if err != nil {
				return nil, fmt.Errorf("failed to decode value: %w", err)
			}
		}

		// Re-encode in binary format for consistency
		data, err := defaultTypeMap.Encode(oid, pgtype.BinaryFormatCode, decoded, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to re-encode value: %w", err)
		}

		return &PostgresValue{
			Oid:   oid,
			Data:  data,
			Value: decoded,
		}, nil
	}

	// Handle already decoded values
	data, err := defaultTypeMap.Encode(oid, pgtype.BinaryFormatCode, value, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %w", err)
	}

	return &PostgresValue{
		Oid:   oid,
		Data:  data,
		Value: value,
	}, nil
}

func (pv *PostgresValue) Get() interface{} {
	return pv.Value
}

func (pv *PostgresValue) Set(value interface{}) error {
	dt, ok := defaultTypeMap.TypeForOID(pv.Oid)
	if !ok {
		return fmt.Errorf("unknown type OID: %d", pv.Oid)
	}

	plan := dt.Codec.PlanEncode(defaultTypeMap, pv.Oid, pgtype.BinaryFormatCode, value)
	if plan == nil {
		return fmt.Errorf("no encode plan for type OID: %d", pv.Oid)
	}

	data, err := plan.Encode(value, nil)
	if err != nil {
		return err
	}

	pv.Data = data
	pv.Value = value
	return nil
}

func (pv *PostgresValue) IsNull() bool {
	return pv.Value == nil
}

func (pv *PostgresValue) GetOID() uint32 {
	return pv.Oid
}

func (pv *PostgresValue) GetBytes() []byte {
	return pv.Data
}

func DefaultTypeMap() *pgtype.Map {
	return defaultTypeMap
}
