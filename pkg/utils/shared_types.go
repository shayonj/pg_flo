package utils

import (
	"fmt"
)

// OperationType represents the type of database operation
type OperationType string

const (
	OperationInsert OperationType = "INSERT"
	OperationUpdate OperationType = "UPDATE"
	OperationDelete OperationType = "DELETE"
	OperationDDL    OperationType = "DDL"
)

// ValidateOperationType validates the operation type and returns the operation type if it is valid.
func ValidateOperationType(opType OperationType) (OperationType, error) {
	switch OperationType(opType) {
	case OperationInsert, OperationUpdate, OperationDelete, OperationDDL:
		return OperationType(opType), nil
	default:
		return "", fmt.Errorf("invalid operation type: %s", opType)
	}
}
