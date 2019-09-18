package pulsar

type ApiVersion int

const (
	V1 ApiVersion = iota
	V2
	V3
)

const DefaultApiVersion = "v2"

func (v ApiVersion) String() string {
	switch v {
	case V1:
		return ""
	case V2:
		return "v2"
	case V3:
		return "v3"
	}

	return DefaultApiVersion
}
