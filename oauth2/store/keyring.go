// Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.

package store

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/99designs/keyring"
	"github.com/apache/pulsar-client-go/oauth2"
	"k8s.io/utils/clock"
)

type KeyringStore struct {
	kr    keyring.Keyring
	clock clock.Clock
	lock  sync.Mutex
}

// storedItem represents an item stored in the keyring
type storedItem struct {
	Audience string
	UserName string
	Grant    auth.AuthorizationGrant
}

// NewKeyringStore creates a store based on a keyring.
func NewKeyringStore(kr keyring.Keyring) (*KeyringStore, error) {
	return &KeyringStore{
		kr:    kr,
		clock: clock.RealClock{},
	}, nil
}

var _ Store = &KeyringStore{}

func (f *KeyringStore) SaveGrant(audience string, grant auth.AuthorizationGrant) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	var err error
	var userName string
	switch grant.Type {
	case auth.GrantTypeClientCredentials:
		if grant.ClientCredentials == nil {
			return ErrUnsupportedAuthData
		}
		userName = grant.ClientCredentials.ClientEmail
	case auth.GrantTypeDeviceCode:
		if grant.Token == nil {
			return ErrUnsupportedAuthData
		}
		userName, err = auth.ExtractUserName(*grant.Token)
		if err != nil {
			return err
		}
	default:
		return ErrUnsupportedAuthData
	}
	item := storedItem{
		Audience: audience,
		UserName: userName,
		Grant:    grant,
	}
	err = f.setItem(item)
	if err != nil {
		return err
	}
	return nil
}

func (f *KeyringStore) LoadGrant(audience string) (*auth.AuthorizationGrant, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	item, err := f.getItem(audience)
	if err != nil {
		if err == keyring.ErrKeyNotFound {
			return nil, ErrNoAuthenticationData
		}
		return nil, err
	}
	switch item.Grant.Type {
	case auth.GrantTypeClientCredentials:
		if item.Grant.ClientCredentials == nil {
			return nil, ErrUnsupportedAuthData
		}
	case auth.GrantTypeDeviceCode:
		if item.Grant.Token == nil {
			return nil, ErrUnsupportedAuthData
		}
	default:
		return nil, ErrUnsupportedAuthData
	}
	return &item.Grant, nil
}

func (f *KeyringStore) WhoAmI(audience string) (string, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	key := hashKeyringKey(audience)
	authItem, err := f.kr.GetMetadata(key)
	if err != nil {
		if err == keyring.ErrKeyNotFound {
			return "", ErrNoAuthenticationData
		}
		return "", fmt.Errorf("unable to get information from the keyring: %v", err)
	}
	return authItem.Label, nil
}

func (f *KeyringStore) Logout() error {
	f.lock.Lock()
	defer f.lock.Unlock()

	var err error
	keys, err := f.kr.Keys()
	if err != nil {
		return fmt.Errorf("unable to get information from the keyring: %v", err)
	}
	for _, key := range keys {
		err = f.kr.Remove(key)
	}
	if err != nil {
		return fmt.Errorf("unable to update the keyring: %v", err)
	}
	return nil
}

func (f *KeyringStore) getItem(audience string) (storedItem, error) {
	key := hashKeyringKey(audience)
	i, err := f.kr.Get(key)
	if err != nil {
		return storedItem{}, err
	}
	var grant auth.AuthorizationGrant
	err = json.Unmarshal(i.Data, &grant)
	if err != nil {
		// the grant appears to be invalid
		return storedItem{}, ErrUnsupportedAuthData
	}
	return storedItem{
		Audience: audience,
		UserName: i.Label,
		Grant:    grant,
	}, nil
}

func (f *KeyringStore) setItem(item storedItem) error {
	key := hashKeyringKey(item.Audience)
	data, err := json.Marshal(item.Grant)
	if err != nil {
		return err
	}
	i := keyring.Item{
		Key:                         key,
		Data:                        data,
		Label:                       item.UserName,
		Description:                 "authorization grant",
		KeychainNotTrustApplication: false,
		KeychainNotSynchronizable:   false,
	}
	err = f.kr.Set(i)
	if err != nil {
		return fmt.Errorf("unable to update the keyring: %v", err)
	}
	return nil
}

// hashKeyringKey creates a safe key based on the given string
func hashKeyringKey(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	bs := h.Sum(nil)
	return fmt.Sprintf("%x", bs)
}
