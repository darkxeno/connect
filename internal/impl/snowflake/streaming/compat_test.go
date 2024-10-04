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
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryption(t *testing.T) {
	data := bytes.NewBufferString("testEncryptionDecryption")
	key := base64.StdEncoding.EncodeToString([]byte("encryption_key"))
	diversifier := "2021/08/10/blob.bdec"
	actual, err := encrypt(data, key, diversifier, 0)
	require.NoError(t, err)
	// this value was obtained from the Cryptor unit tests in the Java SDK
	expected := []byte{133, 80, 92, 68, 33, 84, 54, 127, 139, 26, 89, 42, 80, 118, 6, 27, 56, 48, 149, 113, 118, 62, 50, 158}
	require.Equal(t, expected, actual)
}
