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
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
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
	ivBytes := make([]byte, aes.BlockSize)
	binary.BigEndian.PutUint64(ivBytes[8:], uint64(iv))
	stream := cipher.NewCTR(block, ivBytes)
	encrypted := make([]byte, buf.Len())
	stream.XORKeyStream(encrypted, buf.Bytes())
	return encrypted, nil
}
