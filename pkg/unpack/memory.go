package unpack

import (
	"context"
	"fmt"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/authn/k8schain"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/nlepage/go-tarfs"
	"github.com/operator-framework/operator-registry/pkg/registry"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
	registryfork "github.com/joelanford/kuberpak/internal/operator-registry-fork/pkg/registry"
)

type memory struct {
	bundle olmv1alpha1.Bundle
}

func NewMemory(bundle *olmv1alpha1.Bundle) Unpacker {
	return &memory{*bundle}
}

func (m *memory) getDescriptor(ctx context.Context) (*remote.Descriptor, error) {
	ref, err := name.ParseReference(m.bundle.Spec.Image)
	if err != nil {
		return nil, fmt.Errorf("parse image reference: %v", err)
	}

	chain, err := m.getK8sChain(ctx)
	if err != nil {
		return nil, fmt.Errorf("get image pull authenticator: %v", err)
	}

	desc, err := remote.Get(ref, remote.WithAuthFromKeychain(chain))
	if err != nil {
		return nil, fmt.Errorf("get image metadata: %v", err)
	}
	return desc, nil
}

func (m *memory) GetDigest(ctx context.Context) (string, error) {
	desc, err := m.getDescriptor(ctx)
	if err != nil {
		return "", err
	}
	return desc.Digest.String(), nil
}

func (m *memory) Unpack(ctx context.Context) (*registry.Bundle, error) {
	desc, err := m.getDescriptor(ctx)
	if err != nil {
		return nil, err
	}

	img, err := desc.Image()
	if err != nil {
		return nil, fmt.Errorf("get image contents: %v", err)
	}

	bundleFs, err := tarfs.New(mutate.Extract(img))
	if err != nil {
		return nil, fmt.Errorf("read image tar: %v", err)
	}

	rBundle, err := registryfork.NewBundleParser(nil).Parse(bundleFs)
	if err != nil {
		return nil, fmt.Errorf("parse bundle: %v", err)
	}
	return rBundle, nil
}

func (m memory) getK8sChain(ctx context.Context) (authn.Keychain, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("get kubernetes config: %v", err)
	}
	cl, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("get kubernetes client: %v", err)
	}

	var pullSecrets []string
	for _, ps := range m.bundle.Spec.ImagePullSecrets {
		pullSecrets = append(pullSecrets, ps.Name)
	}

	return k8schain.New(ctx, cl, k8schain.Options{
		Namespace:          m.bundle.Namespace,
		ServiceAccountName: m.bundle.Spec.ServiceAccount,
		ImagePullSecrets:   pullSecrets,
	})
}
