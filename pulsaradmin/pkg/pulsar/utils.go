package pulsar

import (
	"fmt"
)

func makeHttpPath(apiVersion string, componentPath string) string {
	return fmt.Sprintf("/admin/%s%s", apiVersion, componentPath)
}
