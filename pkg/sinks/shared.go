package sinks

import "github.com/shayonj/pg_flo/pkg/utils"

func buildDecodedMessage(message *utils.CDCMessage) (map[string]interface{}, error) {
	decodedMessage := make(map[string]interface{})
	decodedMessage["Type"] = message.Type
	decodedMessage["Schema"] = message.Schema
	decodedMessage["Table"] = message.Table
	decodedMessage["ReplicationKey"] = message.ReplicationKey
	decodedMessage["LSN"] = message.LSN
	decodedMessage["EmittedAt"] = message.EmittedAt

	if message.NewTuple != nil {
		newTuple := make(map[string]interface{})
		for _, col := range message.Columns {
			value, err := message.GetColumnValue(col.Name, false)
			if err != nil {
				return nil, err
			}
			newTuple[col.Name] = value
		}
		decodedMessage["NewTuple"] = newTuple
	}

	if message.OldTuple != nil {
		oldTuple := make(map[string]interface{})
		for _, col := range message.Columns {
			value, err := message.GetColumnValue(col.Name, true)
			if err != nil {
				return nil, err
			}
			oldTuple[col.Name] = value
		}
		decodedMessage["OldTuple"] = oldTuple
	}

	return decodedMessage, nil
}
