/*
Copyright 2020 The Operator-SDK Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package updater

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
)

func New(client client.Client) Updater {
	return Updater{
		client: client,
	}
}

type Updater struct {
	client            client.Client
	updateStatusFuncs []UpdateStatusFunc
}

type UpdateStatusFunc func(bundle *olmv1alpha1.BundleStatus) bool

func (u *Updater) UpdateStatus(fs ...UpdateStatusFunc) {
	u.updateStatusFuncs = append(u.updateStatusFuncs, fs...)
}

func (u *Updater) Apply(ctx context.Context, b *olmv1alpha1.Bundle) error {
	backoff := retry.DefaultRetry

	return retry.RetryOnConflict(backoff, func() error {
		needsStatusUpdate := false
		for _, f := range u.updateStatusFuncs {
			needsStatusUpdate = f(&b.Status) || needsStatusUpdate
		}
		if needsStatusUpdate {
			return u.client.Status().Update(ctx, b)
		}
		return nil
	})
}

func EnsureCondition(condition metav1.Condition) UpdateStatusFunc {
	return func(status *olmv1alpha1.BundleStatus) bool {
		existing := meta.FindStatusCondition(status.Conditions, condition.Type)
		meta.SetStatusCondition(&status.Conditions, condition)
		return existing == nil || *existing != condition
	}
}

func EnsureObservedGeneration(observedGeneration int64) UpdateStatusFunc {
	return func(status *olmv1alpha1.BundleStatus) bool {
		if status.ObservedGeneration == observedGeneration {
			return false
		}
		status.ObservedGeneration = observedGeneration
		return true
	}
}

func UnsetBundleContents() UpdateStatusFunc {
	return SetBundleContents(nil)
}

func SetBundleContents(contents *olmv1alpha1.BundleContents) UpdateStatusFunc {
	return func(status *olmv1alpha1.BundleStatus) bool {
		if status.Contents == contents {
			return false
		}
		status.Contents = contents
		return true
	}
}
