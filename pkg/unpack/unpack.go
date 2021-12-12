package unpack

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
)

type NewUnpackerFunc func(*olmv1alpha1.Bundle) Unpacker

type Unpacker interface {
	Unpack(context.Context) (*corev1.ConfigMapList, error)
}
