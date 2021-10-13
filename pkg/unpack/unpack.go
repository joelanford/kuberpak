package unpack

import (
	"context"

	"github.com/operator-framework/operator-registry/pkg/registry"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
)

type NewUnpackerFunc func(*olmv1alpha1.Bundle) Unpacker

type Unpacker interface {
	GetDigest(context.Context) (string, error)
	Unpack(context.Context) (*registry.Bundle, error)
}
