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
	"bytes"
	"context"
	"crypto/rsa"
	"fmt"
	"time"

	"github.com/parquet-go/parquet-go"
)

// ClientOptions
type ClientOptions struct {
	// Account name
	Account string
	// username
	User string
	// Snowflake Role (i.e. ACCOUNTADMIN)
	Role string
	// Private key for the user
	PrivateKey *rsa.PrivateKey
}

// SnowflakeServiceClient is a port from Java :)
type SnowflakeServiceClient struct {
	client        *restClient
	clientPrefix  string
	deploymentID  int64
	stageLocation fileLocationInfo
	options       ClientOptions
}

func NewSnowflakeServiceClient(ctx context.Context, opts ClientOptions) (*SnowflakeServiceClient, error) {
	client, err := newRestClient(
		opts.Account,
		opts.User,
		opts.PrivateKey,
	)
	if err != nil {
		return nil, err
	}
	resp, err := client.ConfigureClient(ctx, clientConfigureRequest{Role: opts.Role})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != responseSuccess {
		return nil, fmt.Errorf("unable to initialize client - status: %d, message: %s", resp.StatusCode, resp.Message)
	}
	return &SnowflakeServiceClient{
		client:        client,
		clientPrefix:  resp.Prefix,
		deploymentID:  resp.DeploymentID,
		stageLocation: resp.StageLocation,
		options:       opts,
	}, nil
}

func (c *SnowflakeServiceClient) Close() error {
	c.client.Close()
	return nil
}

// ChannelOptions the parameters to opening a channel using SnowflakeServiceClient
type ChannelOptions struct {
	// Name is the name of the channel
	Name string
	// DatabaseName is the name of the database
	DatabaseName string
	// SchemaName is the name of the schema
	SchemaName string
	// TableName is the name of the table
	TableName string
	// ContinueOnError if true, continue loading rows and return all errors in the response instead of aborting on the the first error
	ContinueOnError bool
	// The default timezone for TIMESTAMP_LTZ and TIMESTAMP_TZ columns, defaults to UTC
	DefaultTimeZone *time.Location
}

func (c *SnowflakeServiceClient) OpenChannel(ctx context.Context, opts ChannelOptions) (*SnowflakeIngestionChannel, error) {
	if opts.DefaultTimeZone == nil {
		opts.DefaultTimeZone = time.UTC
	}
	resp, err := c.client.OpenChannel(ctx, openChannelRequest{
		RequestID: fmt.Sprintf("%s_%d", c.clientPrefix, 1),
		Role:      c.options.Role,
		Channel:   opts.Name,
		Database:  opts.DatabaseName,
		Schema:    opts.SchemaName,
		Table:     opts.TableName,
		WriteMode: "CLOUD_STORAGE",
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != responseSuccess {
		return nil, fmt.Errorf("unable to open channel - status: %d, message: %s", resp.StatusCode, resp.Message)
	}
	schema, err := constructParquetSchema(resp.TableColumns)
	if err != nil {
		return nil, err
	}
	ch := &SnowflakeIngestionChannel{
		options: opts,
		schema:  schema,
	}
	return ch, nil
}

func (c *SnowflakeServiceClient) CloseChannel(ctx context.Context, channel *SnowflakeIngestionChannel) error {
	// Shutdown gracefully dude
	return nil
}

// SnowflakeIngestionChannel is a write connection to a single table in Snowflake
type SnowflakeIngestionChannel struct {
	options ChannelOptions
	schema  *parquet.Schema
}

// InsertRows creates a parquet file using the schema from the data,
// then writes that file into the Snowflake table
func (c *SnowflakeIngestionChannel) InsertRows(ctx context.Context, rows []any) error {
	ow := &bytes.Buffer{}
	pw := parquet.NewGenericWriter[any](
		ow,
		c.schema,
		parquet.CreatedBy("RedpandaConnect", version, "main"),
		parquet.DataPageStatistics(false),
		parquet.Compression(&parquet.Gzip),
	)
	err := writeWithoutPanic(pw, rows)
	if err != nil {
		return err
	}
	err = closeWithoutPanic(pw)
	if err != nil {
		return err
	}
	return nil
}

func writeWithoutPanic(pWtr *parquet.GenericWriter[any], rows []any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	_, err = pWtr.Write(rows)
	return
}

func closeWithoutPanic(pWtr *parquet.GenericWriter[any]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	err = pWtr.Close()
	return
}
