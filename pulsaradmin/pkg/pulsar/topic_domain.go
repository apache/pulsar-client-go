package pulsar

import "github.com/pkg/errors"

type TopicDomain string

const (
	persistent     TopicDomain = "persistent"
	non_persistent TopicDomain = "non-persistent"
)

func ParseTopicDomain(domain string) (TopicDomain, error) {
	switch domain {
	case "persistent":
		return persistent, nil
	case "non-persistent":
		return non_persistent, nil
	default:
		return "", errors.Errorf("The domain only can be specified as 'persistent' or " +
			"'non-persistent'. Input domain is '%s'.", domain)
	}
}

func (t TopicDomain) String() string {
	return string(t)
}
