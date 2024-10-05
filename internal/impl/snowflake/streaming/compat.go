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
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

func deriveKey(encryptionKey, diversifier string) ([]byte, error) {
	decodedKey, err := base64.StdEncoding.DecodeString(encryptionKey)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	hash.Write(decodedKey)
	hash.Write([]byte(diversifier))
	return hash.Sum(nil)[:], nil
}

// See Encyptor.encrypt in the Java SDK
func encrypt(buf *bytes.Buffer, encryptionKey string, diversifier string, iv int64) ([]byte, error) {
	// Derive the key from the diversifier and the original encryptionKey from server
	key, err := deriveKey(encryptionKey, diversifier)
	if err != nil {
		return nil, err
	}
	// Using our derived key and padded input, encrypt the thing.
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	// Create our cypher using the iv
	ivBytes := make([]byte, aes.BlockSize)
	binary.BigEndian.PutUint64(ivBytes[8:], uint64(iv))
	stream := cipher.NewCTR(block, ivBytes)
	// Actually do the encryption
	encrypted := make([]byte, buf.Len())
	stream.XORKeyStream(encrypted, buf.Bytes())
	return encrypted, nil
}

func padBuffer(buf *bytes.Buffer, alignmentSize int) {
	padding := alignmentSize - buf.Len()%alignmentSize
	_, _ = buf.Write(make([]byte, padding))
}

func md5Hash(b []byte) string {
	s := md5.Sum(b)
	return hex.EncodeToString(s[:])
}

func generateBlobPath(clientPrefix string, threadID, counter int) string {
	now := time.Now().UTC()
	year := now.Year()
	month := int(now.Month())
	day := now.Day()
	hour := now.Hour()
	minute := now.Minute()
	blobShortName := fmt.Sprintf("%s_%s_%d_%d.bdec", strconv.FormatInt(now.Unix(), 36), clientPrefix, threadID, counter)
	return fmt.Sprintf("%d/%d/%d/%d/%d/%s", year, month, day, hour, minute, blobShortName)
}
