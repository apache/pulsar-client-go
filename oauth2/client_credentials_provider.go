// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package auth

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

const (
	KeyFileTypeServiceAccount = "sn_service_account"
)

type KeyFileProvider struct {
	KeyFile string
}

type KeyFile struct {
	Type         string `json:"type"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	ClientEmail  string `json:"client_email"`
}

func NewClientCredentialsProviderFromKeyFile(keyFile string) *KeyFileProvider {
	return &KeyFileProvider{
		KeyFile: keyFile,
	}
}

var _ ClientCredentialsProvider = &KeyFileProvider{}

func (k *KeyFileProvider) GetClientCredentials() (*KeyFile, error) {
	keyFile, err := ioutil.ReadFile(k.KeyFile)
	if err != nil {
		return nil, err
	}

	var v KeyFile
	err = json.Unmarshal(keyFile, &v)
	if err != nil {
		return nil, err
	}
	if v.Type != KeyFileTypeServiceAccount {
		return nil, fmt.Errorf("open %s: unsupported format", k.KeyFile)
	}

	return &v, nil
}
