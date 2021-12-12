package util

import (
	"fmt"
)

func BundleLabels(bundleName string) map[string]string {
	return map[string]string{"kuberpak.io/bundle-name": bundleName}
}

func MetadataConfigMapName(bundleName string) string {
	return fmt.Sprintf("bundle-metadata-%s", bundleName)
}
