package registry

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"strings"

	operatorsv1alpha1 "github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/operator-registry/pkg/registry"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
)

type bundleParser struct {
	log *logrus.Entry
}

func NewBundleParser(log *logrus.Entry) *bundleParser {
	if log == nil {
		logr := logrus.New()
		logr.SetOutput(ioutil.Discard)
		log = logrus.NewEntry(logr)
	}
	return &bundleParser{
		log: log,
	}
}

// Parse parses the given FS into a Bundle.
func (b *bundleParser) Parse(root fs.FS) (*registry.Bundle, error) {
	if root == nil {
		return nil, fmt.Errorf("filesystem is nil")
	}

	bundle := &registry.Bundle{}
	manifests, err := fs.Sub(root, "manifests")
	if err != nil {
		return nil, fmt.Errorf("error opening manifests directory: %s", err)
	}
	if err := b.addManifests(manifests, bundle); err != nil {
		return nil, err
	}

	metadata, err := fs.Sub(root, "metadata")
	if err != nil {
		return nil, fmt.Errorf("error opening metadata directory: %s", err)
	}
	if err := b.addMetadata(metadata, bundle); err != nil {
		return nil, err
	}

	derived, err := b.derivedProperties(bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to derive properties: %s", err)
	}

	bundle.Properties = propertySet(append(bundle.Properties, derived...))

	return bundle, nil
}

// addManifests adds the result of parsing the manifests directory to a bundle.
func (b *bundleParser) addManifests(manifests fs.FS, bundle *registry.Bundle) error {
	files, err := fs.ReadDir(manifests, ".")
	if err != nil {
		return err
	}

	var csvFound bool
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		name := f.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}

		obj := &unstructured.Unstructured{}
		if err = decodeFileFS(manifests, name, obj); err != nil {
			b.log.Warnf("failed to decode: %s", err)
			continue
		}

		// Only include the first CSV we find in the
		if obj.GetKind() == operatorsv1alpha1.ClusterServiceVersionKind {
			if csvFound {
				continue
			}
			csvFound = true
		}

		if obj.Object != nil {
			bundle.Add(obj)
		}
	}

	if bundle.Size() == 0 {
		return fmt.Errorf("no bundle objects found")
	}

	csv, err := bundle.ClusterServiceVersion()
	if err != nil {
		return err
	}
	if csv == nil {
		return fmt.Errorf("no csv in bundle")
	}

	bundle.Name = csv.GetName()
	if err := bundle.AllProvidedAPIsInBundle(); err != nil {
		return fmt.Errorf("error checking provided apis in bundle %s: %s", bundle.Name, err)
	}

	return nil
}

// addManifests adds the result of parsing the metadata directory to a bundle.
func (b *bundleParser) addMetadata(metadata fs.FS, bundle *registry.Bundle) error {
	files, err := fs.ReadDir(metadata, ".")
	if err != nil {
		return err
	}

	var (
		af *registry.AnnotationsFile
		df *registry.DependenciesFile
		pf *registry.PropertiesFile
	)
	for _, f := range files {
		name := f.Name()
		if af == nil {
			decoded := registry.AnnotationsFile{}
			if err = decodeFileFS(metadata, name, &decoded); err == nil {
				if decoded != (registry.AnnotationsFile{}) {
					af = &decoded
				}
			}
		}
		if df == nil {
			decoded := registry.DependenciesFile{}
			if err = decodeFileFS(metadata, name, &decoded); err == nil {
				if len(decoded.Dependencies) > 0 {
					df = &decoded
				}
			}
		}
		if pf == nil {
			decoded := registry.PropertiesFile{}
			if err = decodeFileFS(metadata, name, &decoded); err == nil {
				if len(decoded.Properties) > 0 {
					pf = &decoded
				}
			}
		}
	}

	if af != nil {
		bundle.Annotations = &af.Annotations
		bundle.Package = af.Annotations.PackageName
		bundle.Channels = af.GetChannels()
	} else {
		return fmt.Errorf("Could not find annotations file")
	}

	if df != nil {
		bundle.Dependencies = append(bundle.Dependencies, df.GetDependencies()...)
	} else {
		b.log.Info("Could not find optional dependencies file")
	}

	if pf != nil {
		bundle.Properties = append(bundle.Properties, pf.Properties...)
	} else {
		b.log.Info("Could not find optional properties file")
	}

	return nil
}

func (b *bundleParser) derivedProperties(bundle *registry.Bundle) ([]registry.Property, error) {
	// Add properties from CSV annotations
	csv, err := bundle.ClusterServiceVersion()
	if err != nil {
		return nil, fmt.Errorf("error getting csv: %s", err)
	}
	if csv == nil {
		return nil, fmt.Errorf("bundle missing csv")
	}

	var derived []registry.Property
	if len(csv.GetAnnotations()) > 0 {
		properties, ok := csv.GetAnnotations()[registry.PropertyKey]
		if ok {
			if err := json.Unmarshal([]byte(properties), &derived); err != nil {
				b.log.Warnf("failed to unmarshal csv annotation properties: %s", err)
			}
		}
	}

	if bundle.Annotations != nil && bundle.Annotations.PackageName != "" {
		pkg := bundle.Annotations.PackageName
		version, err := bundle.Version()
		if err != nil {
			return nil, err
		}

		value, err := json.Marshal(registry.PackageProperty{
			PackageName: pkg,
			Version:     version,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to marshal package property: %s", err)
		}

		// Annotations file takes precedent over CSV annotations
		derived = append([]registry.Property{{Type: registry.PackageType, Value: value}}, derived...)
	}

	providedAPIs, err := bundle.ProvidedAPIs()
	if err != nil {
		return nil, fmt.Errorf("error getting provided apis: %s", err)
	}

	for api := range providedAPIs {
		value, err := json.Marshal(registry.GVKProperty{
			Group:   api.Group,
			Kind:    api.Kind,
			Version: api.Version,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to marshal gvk property: %s", err)
		}
		derived = append(derived, registry.Property{Type: registry.GVKType, Value: value})
	}

	return propertySet(derived), nil
}

// propertySet returns the deduplicated set of a property list.
func propertySet(properties []registry.Property) []registry.Property {
	var (
		set     []registry.Property
		visited = map[string]struct{}{}
	)
	for _, p := range properties {
		if _, ok := visited[p.String()]; ok {
			continue
		}
		visited[p.String()] = struct{}{}
		set = append(set, p)
	}

	return set
}

func decodeFileFS(root fs.FS, path string, into interface{}) error {
	fileReader, err := root.Open(path)
	if err != nil {
		return fmt.Errorf("unable to read file %s: %s", path, err)
	}
	defer fileReader.Close()

	decoder := yaml.NewYAMLOrJSONDecoder(fileReader, 30)

	return decoder.Decode(into)
}
