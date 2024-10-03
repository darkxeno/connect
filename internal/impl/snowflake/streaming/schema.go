/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package streaming

import (
	"fmt"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// See ParquetTypeGenerator
func constructParquetSchema(columns []columnMetadata) (*parquet.Schema, error) {
	groupNode := parquet.Group{}

	for i, column := range columns {
		id := i + 1
		if column.Ordinal != 0 {
			id = int(column.Ordinal)
		}
		var n parquet.Node
		switch strings.ToLower(column.LogicalType) {
		case "fixed":
			// TODO: It's not this simple :)
			n = parquet.Leaf(parquet.Int64Type)
		case "text":
			fallthrough
		case "char":
			fallthrough
		case "any":
			fallthrough
		case "binary":
			n = parquet.String()
		case "boolean":
			n = parquet.Leaf(parquet.BooleanType)
		case "real":
			n = parquet.Leaf(parquet.DoubleType)
		default:
			return nil, fmt.Errorf("unsupported logical column type: %s", column.LogicalType)
		}
		n = parquet.FieldID(n, id)
		if column.Nullable {
			n = parquet.Optional(n)
		}
		// TODO: Use the unquoted name
		groupNode[column.Name] = n
	}
	return parquet.NewSchema("bdec", groupNode), nil
}
