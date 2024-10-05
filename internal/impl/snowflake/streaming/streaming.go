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
	"crypto/aes"
	"crypto/rsa"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/segmentio/encoding/thrift"
	"golang.org/x/oauth2"
	gcsopt "google.golang.org/api/option"
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
		clientPrefix:  fmt.Sprintf("%s_%d", resp.Prefix, resp.DeploymentID),
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
		options:         opts,
		clientPrefix:    c.clientPrefix,
		schema:          schema,
		client:          c.client,
		stageLocation:   c.stageLocation,
		role:            c.options.Role,
		encryptionKeyID: resp.EncryptionKeyID,
		encryptionKey:   resp.EncryptionKey,
		clientSequencer: resp.ClientSequencer,
	}
	return ch, nil
}

// SnowflakeIngestionChannel is a write connection to a single table in Snowflake
type SnowflakeIngestionChannel struct {
	options         ChannelOptions
	role            string
	clientPrefix    string
	schema          *parquet.Schema
	client          *restClient
	stageLocation   fileLocationInfo
	encryptionKeyID int64
	encryptionKey   string
	clientSequencer int64
	rowSequencer    int64
}

// InsertRows creates a parquet file using the schema from the data,
// then writes that file into the Snowflake table
func (c *SnowflakeIngestionChannel) InsertRows(ctx context.Context, rows []any) error {
	startTime := time.Now()
	client, err := gcs.NewClient(ctx, gcsopt.WithTokenSource(
		oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: c.stageLocation.Creds["GCS_ACCESS_TOKEN"],
			TokenType:   "Bearer",
		}),
	))
	if err != nil {
		return err
	}
	bucketAndPath := strings.SplitN(c.stageLocation.Location, "/", 2)
	if len(bucketAndPath) != 2 {
		return fmt.Errorf("unexpected stage location: %s", c.stageLocation.Location)
	}
	bucket := client.Bucket(bucketAndPath[0])
	path := filepath.Join(
		bucketAndPath[1],
		generateBlobPath(c.clientPrefix, 32, 0),
	)
	buf := &bytes.Buffer{}
	pw := parquet.NewGenericWriter[any](
		buf,
		c.schema,
		parquet.CreatedBy("RedpandaConnect", version, "main"),
		parquet.DataPageStatistics(true),
		parquet.Compression(&parquet.Gzip),
	)
	err = writeWithoutPanic(pw, rows)
	if err != nil {
		return err
	}
	err = closeWithoutPanic(pw)
	if err != nil {
		return err
	}
	unencrypted := buf.Bytes()
	unencryptedLen := buf.Len()
	footerSize := binary.LittleEndian.Uint32(unencrypted[unencryptedLen-8:])
	metadata := format.FileMetaData{}
	err = thrift.Unmarshal(new(thrift.CompactProtocol), unencrypted[unencryptedLen-(int(footerSize)+8):unencryptedLen-8], &metadata)
	if err != nil {
		return err
	}
	padBuffer(buf, aes.BlockSize)
	encrypted, err := encrypt(buf, c.encryptionKey, path, 0)
	if err != nil {
		return err
	}
	uploadStartTime := time.Now()
	object := bucket.Object(path)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ow := object.NewWriter(ctx)
	_, err = ow.Write(encrypted)
	if err != nil {
		return err
	}
	err = ow.Close()
	if err != nil {
		return err
	}
	uploadFinishTime := time.Now()
	_, err = c.client.RegisterBlob(ctx, registerBlobRequest{
		RequestID: fmt.Sprintf("%s_%d", c.clientPrefix, 2),
		Role:      c.role,
		Blobs: []blobMetadata{
			{
				Path:        strings.TrimPrefix(path, "streaming_ingest/"),
				MD5:         md5Hash(encrypted),
				BDECVersion: 3,
				BlobStats: blobStats{
					FlushStartMs:     startTime.UnixMilli(),
					BuildDurationMs:  uploadStartTime.UnixMilli() - startTime.UnixMilli(),
					UploadDurationMs: uploadFinishTime.UnixMilli() - uploadStartTime.UnixMilli(),
				},
				Chunks: []chunkMetadata{
					{
						Database:         c.options.DatabaseName,
						Schema:           c.options.SchemaName,
						Table:            c.options.TableName,
						ChunkStartOffset: 0,
						ChunkLength:      int32(ow.Attrs().Size),
						// This is an estimate in the Java SDK
						ChunkLengthUncompressed: int32(unencryptedLen),
						ChunkMD5:                md5Hash(encrypted[:unencryptedLen]),
						EncryptionKeyID:         c.encryptionKeyID,
						FirstInsertTimeInMillis: startTime.UnixMilli(),
						LastInsertTimeInMillis:  startTime.UnixMilli(),
						EPS: &epInfo{
							Rows:    metadata.NumRows,
							Columns: computeColumnEpInfo(metadata),
						},
						Channels: []channelMetadata{
							{
								Channel:          c.options.Name,
								ClientSequencer:  c.clientSequencer,
								RowSequencer:     c.rowSequencer + 1,
								StartOffsetToken: nil,
								EndOffsetToken:   nil,
								OffsetToken:      nil,
							},
						},
					},
				},
			},
		},
	})
	return err
}

func computeColumnEpInfo(metadata format.FileMetaData) map[string]fileColumnProperties {
	fmt.Printf("%#v\n", metadata.Schema)
	info := map[string]fileColumnProperties{}
	if len(metadata.RowGroups) != 1 {
		panic("laksdjalkj")
	}
	for _, rowGroup := range metadata.RowGroups {
		i := -1
		for _, column := range rowGroup.Columns {
			idx := slices.IndexFunc(metadata.Schema[i+1:], func(e format.SchemaElement) bool { return e.NumChildren == 0 })
			if idx == -1 {
				panic("oops")
			}
			i += idx + 1
			schemaElement := metadata.Schema[i]
			path := column.MetaData.PathInSchema
			if len(path) != 1 {
				panic(path)
			}
			name := path[0]
			existing := info[name]
			fmt.Printf("%#v\n", schemaElement)
			existing.ColumnOrdinal = schemaElement.FieldID
			existing.DistinctValues = -1
			if name == "A" {
				existing.MinIntValue = 75
				existing.MaxIntValue = 75
			} else if name == "B" {
				val := "717578"
				existing.MinStrValue = &val
				existing.MaxStrValue = &val
				existing.MaxLength = 3
			}
			info[name] = existing
		}
	}
	return info
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
