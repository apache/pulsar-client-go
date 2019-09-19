package pulsar

import "github.com/pkg/errors"

type AuthAction string

const (
	produce       AuthAction = "produce"
	consume       AuthAction = "consume"
	functionsAuth AuthAction = "functions"
)

func ParseAuthAction(action string) (AuthAction, error) {
	switch action {
	case "produce":
		return produce, nil
	case "consume":
		return consume, nil
	case "functions":
		return functionsAuth, nil
	default:
		return "", errors.Errorf("The auth action  only can be specified as 'produce', "+
			"'consume', or 'functions'. Invalid auth action '%s'", action)
	}
}

func (a AuthAction) String() string {
	return string(a)
}
