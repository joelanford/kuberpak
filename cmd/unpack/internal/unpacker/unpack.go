package unpacker

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/operator-framework/operator-registry/pkg/registry"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/yaml"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
	"github.com/joelanford/kuberpak/internal/util"
)

func UnpackCommand() *cobra.Command {
	var (
		unpacker  Unpacker
		bundleDir string
	)

	cmd := &cobra.Command{
		Use:  "unpack",
		Args: cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, _ []string) error {
			log := zap.New().WithValues("bundle", unpacker.BundleName)
			cl, err := getClient()
			if err != nil {
				log.Error(err, "could not get client")
				os.Exit(1)
			}
			unpacker.Client = cl
			unpacker.Bundle = os.DirFS(bundleDir)
			if err := unpacker.Run(cmd.Context()); err != nil {
				log.Error(err, "unpack failed")
				os.Exit(1)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&unpacker.PodNamespace, "pod-namespace", "", "namespace of pod with bundle image container")
	cmd.Flags().StringVar(&unpacker.PodName, "pod-name", "", "name of pod with bundle image container")
	cmd.Flags().StringVar(&unpacker.BundleName, "bundle-name", "", "the name of the bundle object that is being unpacked")
	cmd.Flags().StringVar(&bundleDir, "bundle-dir", "", "directory in which the bundle can be found")
	return cmd
}

func getClient() (client.Client, error) {
	sch := scheme.Scheme
	if err := olmv1alpha1.AddToScheme(sch); err != nil {
		return nil, err
	}
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: sch})
}

type Unpacker struct {
	// Used to manage config maps and read a pod to get image digest
	Client       client.Client
	PodNamespace string
	PodName      string

	// A filesystem containing the bundle manifests and metadata
	Bundle fs.FS

	// Used to apply metadata to the generated configmaps
	PackageName string
	BundleName  string
}

func (u *Unpacker) Run(ctx context.Context) error {
	bundle := &olmv1alpha1.Bundle{}
	bundleKey := types.NamespacedName{Name: u.BundleName}
	if err := u.Client.Get(ctx, bundleKey, bundle); err != nil {
		return err
	}
	bundle.SetGroupVersionKind(olmv1alpha1.GroupVersion.WithKind("Bundle"))

	resolvedImage, err := u.getImageDigest(ctx)
	if err != nil {
		return err
	}

	annotations, err := u.getAnnotations()
	if err != nil {
		return err
	}

	objects, err := u.getObjects()
	if err != nil {
		return err
	}

	var objectRefs []corev1.ObjectReference
	for _, obj := range objects {
		apiVersion, kind := obj.GetObjectKind().GroupVersionKind().ToAPIVersionAndKind()
		objectRefs = append(objectRefs, corev1.ObjectReference{
			Kind:       kind,
			Namespace:  obj.GetNamespace(),
			Name:       obj.GetName(),
			APIVersion: apiVersion,
		})
	}

	desiredConfigMaps, err := u.getDesiredConfigMaps(bundle, resolvedImage, annotations, objects)
	if err != nil {
		return err
	}

	data, err := json.Marshal(desiredConfigMaps)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func (u *Unpacker) getImageDigest(ctx context.Context) (string, error) {
	podKey := types.NamespacedName{Namespace: u.PodNamespace, Name: u.PodName}
	pod := &corev1.Pod{}
	if err := u.Client.Get(ctx, podKey, pod); err != nil {
		return "", err
	}
	for _, ps := range pod.Status.InitContainerStatuses {
		if ps.Name == "copy-bundle" && ps.ImageID != "" {
			return ps.ImageID, nil
		}
	}
	return "", fmt.Errorf("bundle image digest not found")
}

func (u *Unpacker) getAnnotations() (*registry.Annotations, error) {
	fileData, err := fs.ReadFile(u.Bundle, filepath.Join("metadata", "annotations.yaml"))
	if err != nil {
		return nil, err
	}
	annotationsFile := registry.AnnotationsFile{}
	if err := yaml.Unmarshal(fileData, &annotationsFile); err != nil {
		return nil, err
	}
	return &annotationsFile.Annotations, nil
}

func (u *Unpacker) getObjects() ([]unstructured.Unstructured, error) {
	var objects []unstructured.Unstructured
	const manifestsDir = "manifests"

	entries, err := fs.ReadDir(u.Bundle, manifestsDir)
	if err != nil {
		return nil, fmt.Errorf("read manifests: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fileData, err := fs.ReadFile(u.Bundle, filepath.Join(manifestsDir, e.Name()))
		if err != nil {
			return nil, err
		}
		dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(fileData), 1024)
		for {
			obj := unstructured.Unstructured{}
			err := dec.Decode(&obj)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}
			objects = append(objects, obj)
		}
	}
	return objects, nil
}

func (u *Unpacker) getDesiredConfigMaps(bundle *olmv1alpha1.Bundle, resolvedImage string, annotations *registry.Annotations, objects []unstructured.Unstructured) (*corev1.ConfigMapList, error) {
	var desiredConfigMaps []corev1.ConfigMap

	var (
		pkgName       = annotations.PackageName
		bundleName    string
		bundleVersion string
	)

	for _, obj := range objects {
		if obj.GetObjectKind().GroupVersionKind().Kind == "ClusterServiceVersion" {
			if v, found, err := unstructured.NestedString(obj.Object, "spec", "version"); found && err == nil {
				bundleVersion = v
			}
			if v, found, err := unstructured.NestedString(obj.Object, "metadata", "name"); found && err == nil {
				bundleName = v
			}
		}
	}
	immutable := true
	controllerRef := metav1.NewControllerRef(bundle, bundle.GroupVersionKind())
	for _, obj := range objects {
		objData, err := yaml.Marshal(obj)
		if err != nil {
			return nil, err
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(objData))
		objCompressed := &bytes.Buffer{}
		gzipper := gzip.NewWriter(objCompressed)
		if _, err := gzipper.Write(objData); err != nil {
			return nil, fmt.Errorf("gzip object data: %v", err)
		}
		if err := gzipper.Close(); err != nil {
			return nil, fmt.Errorf("close gzip writer: %v", err)
		}
		apiVersion, kind := obj.GetObjectKind().GroupVersionKind().ToAPIVersionAndKind()
		cm := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("bundle-object-%s-%s", u.BundleName, hash[0:8]),
				Namespace:       u.PodNamespace,
				Labels:          util.BundleLabels(bundle.Name),
				OwnerReferences: []metav1.OwnerReference{*controllerRef},
			},
			Immutable: &immutable,
			Data: map[string]string{
				"package-name":      pkgName,
				"bundle-image":      resolvedImage,
				"bundle-name":       bundleName,
				"bundle-version":    bundleVersion,
				"object-sha256":     hash,
				"object-kind":       kind,
				"object-apiversion": apiVersion,
				"object-name":       obj.GetName(),
				"object-namespace":  obj.GetNamespace(),
			},
			BinaryData: map[string][]byte{
				"object": objCompressed.Bytes(),
			},
		}
		desiredConfigMaps = append(desiredConfigMaps, cm)
	}
	objectConfigMaps := []string{}
	for _, dcm := range desiredConfigMaps {
		objectConfigMaps = append(objectConfigMaps, dcm.Name)
	}
	ocmJson, err := json.Marshal(objectConfigMaps)
	if err != nil {
		return nil, fmt.Errorf("marshal object configmap names as json: %v", err)
	}
	metadataCm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("bundle-metadata-%s", u.BundleName),
			Namespace:       u.PodNamespace,
			Labels:          util.BundleLabels(bundle.Name),
			OwnerReferences: []metav1.OwnerReference{*controllerRef},
		},
		Immutable: &immutable,
		Data: map[string]string{
			"objects":        string(ocmJson),
			"package-name":   pkgName,
			"bundle-image":   resolvedImage,
			"bundle-name":    bundleName,
			"bundle-version": bundleVersion,
		},
	}
	cmList := &corev1.ConfigMapList{
		Items: append(desiredConfigMaps, metadataCm),
	}
	return cmList, nil
}
