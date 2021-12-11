package unpack

import (
	"context"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
)

type NewUnpackerFunc func(*olmv1alpha1.Bundle) Unpacker

type Unpacker interface {
	Unpack(context.Context) (*olmv1alpha1.BundleStatus, error)
}
