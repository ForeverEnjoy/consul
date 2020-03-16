package agent

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/hashicorp/consul/agent/consul"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

const (
	SerfLANKeyring = "serf/local.keyring"
	SerfWANKeyring = "serf/remote.keyring"
)

// initKeyring will create a keyring file at a given path.
func initKeyring(path, key string) error {
	var keys []string

	if keyBytes, err := decodeStringKey(key); err != nil {
		return fmt.Errorf("Invalid key: %s", err)
	} else if err := memberlist.ValidateKey(keyBytes); err != nil {
		return fmt.Errorf("Invalid key: %s", err)
	}

	// Just exit if the file already exists.
	if _, err := os.Stat(path); err == nil {
		return nil
	}

	keys = append(keys, key)
	keyringBytes, err := json.Marshal(keys)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}

	fh, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer fh.Close()

	if _, err := fh.Write(keyringBytes); err != nil {
		os.Remove(path)
		return err
	}

	return nil
}

// loadKeyringFile will load a gossip encryption keyring out of a file. The file
// must be in JSON format and contain a list of encryption key strings.
func loadKeyringFile(c *serf.Config) error {
	if c.KeyringFile == "" {
		return nil
	}

	if _, err := os.Stat(c.KeyringFile); err != nil {
		return err
	}

	keyringData, err := ioutil.ReadFile(c.KeyringFile)
	if err != nil {
		return err
	}

	keys := make([]string, 0)
	if err := json.Unmarshal(keyringData, &keys); err != nil {
		return err
	}

	return loadKeyring(c, keys)
}

// loadKeyring takes a list of base64-encoded strings and installs them in the
// given Serf's keyring.
func loadKeyring(c *serf.Config, keys []string) error {
	keysDecoded := make([][]byte, len(keys))
	for i, key := range keys {
		keyBytes, err := decodeStringKey(key)
		if err != nil {
			return err
		}
		keysDecoded[i] = keyBytes
	}

	if len(keysDecoded) == 0 {
		return fmt.Errorf("no keys present in keyring: %s", c.KeyringFile)
	}

	keyring, err := memberlist.NewKeyring(keysDecoded, keysDecoded[0])
	if err != nil {
		return err
	}

	c.MemberlistConfig.Keyring = keyring
	return nil
}

func decodeStringKey(key string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(key)
}

// KeyringOperation is used to abstract away the semantic similarities in
// performing various operations on the encryption keyring.
func (a *Agent) KeyringOperation(args *structs.KeyringRequest) (structs.KeyringResponses, error) {
	var reply structs.KeyringResponses

	if _, ok := a.delegate.(*consul.Server); !ok {
		return reply, fmt.Errorf("keyring operations must run against a server node")
	}
	if err := a.RPC("Internal.KeyringOperation", args, &reply); err != nil {
		return reply, err
	}

	return reply, nil
}

// ParseRelayFactor validates and converts the given relay factor to uint8
func ParseRelayFactor(n int) (uint8, error) {
	if n < 0 || n > 5 {
		return 0, fmt.Errorf("Relay factor must be in range: [0, 5]")
	}
	return uint8(n), nil
}

// ValidateLocalOnly validates the local-only flag, requiring that it only be
// set for list requests.
func ValidateLocalOnly(local bool, list bool) error {
	if local && !list {
		return fmt.Errorf("local-only can only be set for list requests")
	}
	return nil
}

// keyringIsMissingKey checks whether a key is part of a keyring. Returns true
// if it is not included.
func keyringIsMissingKey(keyring *memberlist.Keyring, key string) bool {
	k1, err := decodeStringKey(key)
	if err != nil {
		return true
	}
	for _, k2 := range keyring.GetKeys() {
		if bytes.Equal(k1, k2) {
			return false
		}
	}
	return true
}
