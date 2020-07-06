package store

import (
	auth "github.com/apache/pulsar-client-go/oauth2"
	"k8s.io/utils/clock"
	"sync"
)

type MemoryStore struct {
	clock  clock.Clock
	lock   sync.Mutex
	grants map[string]*auth.AuthorizationGrant
}

func NewMemoryStore() Store {
	return &MemoryStore{
		clock: clock.RealClock{},
	}
}

var _ Store = &MemoryStore{}

func (f *MemoryStore) SaveGrant(audience string, grant auth.AuthorizationGrant) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.grants[audience] = &grant
	return nil
}

func (f *MemoryStore) LoadGrant(audience string) (*auth.AuthorizationGrant, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	grant, ok := f.grants[audience]
	if !ok {
		return nil, ErrNoAuthenticationData
	}
	return grant, nil
}

func (f *MemoryStore) WhoAmI(audience string) (string, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	grant, ok := f.grants[audience]
	if !ok {
		return "", ErrNoAuthenticationData
	}
	switch grant.Type {
	case auth.GrantTypeClientCredentials:
		if grant.ClientCredentials == nil {
			return "", ErrUnsupportedAuthData
		}
		return grant.ClientCredentials.ClientEmail, nil
	case auth.GrantTypeDeviceCode:
		if grant.Token == nil {
			return "", ErrUnsupportedAuthData
		}
		return auth.ExtractUserName(*grant.Token)
	default:
		return "", ErrUnsupportedAuthData
	}
}

func (f *MemoryStore) Logout() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.grants = map[string]*auth.AuthorizationGrant{}
	return nil
}
