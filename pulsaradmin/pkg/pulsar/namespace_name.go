package pulsar

import (
	"fmt"
	"github.com/pkg/errors"
	"regexp"
	"strings"
)

type NameSpaceName struct {
	tenant    string
	nameSpace string
}

func GetNameSpaceName(tenant, namespace string) (*NameSpaceName, error) {
	return GetNamespaceName(fmt.Sprintf("%s/%s", tenant,namespace))
}

func GetNamespaceName(completeName string) (*NameSpaceName, error) {
	var n NameSpaceName

	if completeName == "" {
		return nil, errors.New("The namespace complete name is empty.")
	}

	parts := strings.Split(completeName, "/")
	if len(parts) == 2 {
		n.tenant = parts[0]
		n.nameSpace = parts[1]
		err := validateNamespaceName(n.tenant, n.nameSpace)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.Errorf("The complete name of namespace is invalid. complete name : [%s]", completeName)
	}

	return &n, nil
}

func (n *NameSpaceName) String() string {
	return fmt.Sprintf("%s/%s", n.tenant, n.nameSpace)
}

func validateNamespaceName(tenant, namespace string) error {
	if tenant == "" || namespace == "" {
		return errors.Errorf("Invalid tenant or namespace. [%s/%s]", tenant, namespace)
	}

	ok := checkName(tenant)
	if !ok {
		return errors.Errorf("Tenant name include unsupported special chars. tenant : [%s]", tenant)
	}

	ok = checkName(namespace)
	if !ok {
		return errors.Errorf("Namespace name include unsupported special chars. namespace : [%s]", namespace)
	}

	return nil
}

// allowed characters for property, namespace, cluster and topic
// names are alphanumeric (a-zA-Z0-9) and these special chars -=:.
// and % is allowed as part of valid URL encoding
const PATTEN = "^[-=:.\\w]*$"

func checkName(name string) bool {
	patten, err := regexp.Compile(PATTEN)
	if err != nil {
		return false
	}

	return patten.MatchString(name)
}
