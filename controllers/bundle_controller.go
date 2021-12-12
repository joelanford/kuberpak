/*
Copyright 2021.

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

package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
	"github.com/joelanford/kuberpak/internal/updater"
	"github.com/joelanford/kuberpak/internal/util"
	"github.com/joelanford/kuberpak/pkg/unpack"
)

// BundleReconciler reconciles a Bundle object
type BundleReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	GetUnpacker unpack.NewUnpackerFunc
}

//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundles,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundles/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundles/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=pods;secrets,verbs=get;list;watch;create;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Bundle object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.9.2/pkg/reconcile
func (r *BundleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, reconcileErr error) {
	l := log.FromContext(ctx)
	l.Info("starting reconciliation")
	defer l.Info("ending reconciliation")
	bundle := &olmv1alpha1.Bundle{}
	if err := r.Get(ctx, req.NamespacedName, bundle); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	u := updater.New(r.Client)
	defer func() {
		l.Info("applying status changes")
		if err := u.Apply(ctx, bundle); err != nil {
			l.Error(err, "failed to update status")
		}
	}()
	u.UpdateStatus(
		updater.EnsureObservedGeneration(bundle.Generation),
	)

	unpacker := r.GetUnpacker(bundle)
	u.UpdateStatus(
		updater.UnsetBundleInfo(),
	)
	if err := u.Apply(ctx, bundle); err != nil {
		return ctrl.Result{}, err
	}

	l.Info("unpacking bundle")
	desiredConfigMaps, err := unpacker.Unpack(ctx)
	if err != nil {
		u.UpdateStatus(
			updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeUnpacked,
				Status:  metav1.ConditionFalse,
				Reason:  olmv1alpha1.ReasonUnpackFailed,
				Message: err.Error(),
			}),
		)
		return ctrl.Result{}, fmt.Errorf("unpack bundle: %v", err)
	}

	actualConfigMaps := &corev1.ConfigMapList{}
	if err := r.List(ctx, actualConfigMaps, client.MatchingLabels(util.BundleLabels(bundle.Name)), client.InNamespace("kuberpak-system")); err != nil {
		u.UpdateStatus(
			updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeUnpacked,
				Status:  metav1.ConditionFalse,
				Reason:  olmv1alpha1.ReasonUnpackFailed,
				Message: err.Error(),
			}),
		)
		return ctrl.Result{}, err
	}

	if err := r.ensureDesiredConfigMaps(ctx, actualConfigMaps.Items, desiredConfigMaps.Items); err != nil {
		u.UpdateStatus(
			updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeUnpacked,
				Status:  metav1.ConditionFalse,
				Reason:  olmv1alpha1.ReasonUnpackFailed,
				Message: err.Error(),
			}),
		)
		return ctrl.Result{}, err
	}

	status, err := statusFromDesiredConfigMaps(bundle, desiredConfigMaps.Items)
	if err != nil {
		u.UpdateStatus(
			updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeUnpacked,
				Status:  metav1.ConditionFalse,
				Reason:  olmv1alpha1.ReasonUnpackFailed,
				Message: err.Error(),
			}),
		)
		return ctrl.Result{}, err
	}

	u.UpdateStatus(
		updater.SetBundleInfo(status.Info),
		updater.EnsureBundleDigest(status.Digest),
		updater.EnsureCondition(metav1.Condition{
			Type:   olmv1alpha1.TypeUnpacked,
			Status: metav1.ConditionTrue,
			Reason: olmv1alpha1.ReasonUnpackSuccessful,
		}),
	)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BundleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&olmv1alpha1.Bundle{}, builder.WithPredicates(
			bundleProvisionerFilter("kuberpak.io/registry+v1"),
			ignoreStatusChanges(),
		)).
		Owns(&corev1.ConfigMap{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 128}).
		Complete(r)
}

func bundleProvisionerFilter(provisionerClassName string) predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		b := obj.(*olmv1alpha1.Bundle)
		return b.Spec.ProvisionerClassName == provisionerClassName
	})
}

func ignoreStatusChanges() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(evt event.UpdateEvent) bool {
			a := evt.ObjectOld.(*olmv1alpha1.Bundle)
			b := evt.ObjectNew.(*olmv1alpha1.Bundle)
			a.Status = olmv1alpha1.BundleStatus{}
			b.Status = olmv1alpha1.BundleStatus{}
			return reflect.DeepEqual(a, b)
		},
	}
}

func (r *BundleReconciler) ensureDesiredConfigMaps(ctx context.Context, actual, desired []corev1.ConfigMap) error {
	l := log.FromContext(ctx)

	actualCms := map[types.NamespacedName]corev1.ConfigMap{}
	for _, cm := range actual {
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		actualCms[key] = cm
	}

	for _, cm := range desired {
		cm := cm
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		if acm, ok := actualCms[key]; ok {
			if configMapsEqual(acm, cm) {
				l.V(2).Info("found desired configmap, skipping", "name", acm.Name)
				delete(actualCms, key)
				continue
			}
		}
		acm := &corev1.ConfigMap{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(&cm), acm); err == nil {
			l.V(2).Info("configmap needs update, deleting", "name", acm.Name)
			if err := r.Delete(ctx, acm); client.IgnoreNotFound(err) != nil {
				return fmt.Errorf("delete configmap: %v", err)
			}
		}
		l.V(2).Info("creating desired configmap", "name", cm.Name)
		if err := r.Create(ctx, &cm); err != nil {
			return fmt.Errorf("create configmap: %v", err)
		}
	}
	for _, acm := range actualCms {
		l.V(2).Info("deleting undesired configmap", "name", acm.Name)
		if err := r.Delete(ctx, &acm); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete configmap: %v", err)
		}
	}
	return nil
}

func configMapsEqual(a, b corev1.ConfigMap) bool {
	return stringMapsEqual(a.Labels, b.Labels) &&
		stringMapsEqual(a.Annotations, b.Annotations) &&
		ownerRefsEqual(a.OwnerReferences, b.OwnerReferences) &&
		stringMapsEqual(a.Data, b.Data) &&
		bytesMapsEqual(a.BinaryData, b.BinaryData)
}

func stringMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for ka, va := range a {
		vb, ok := b[ka]
		if !ok || va != vb {
			return false
		}
	}
	return true
}

func bytesMapsEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for ka, va := range a {
		vb, ok := b[ka]
		if !ok || !bytes.Equal(va, vb) {
			return false
		}
	}
	return true
}

func ownerRefsEqual(a, b []metav1.OwnerReference) bool {
	if len(a) != len(b) {
		return false
	}
	for i, ora := range a {
		orb := b[i]
		if !reflect.DeepEqual(ora, orb) {
			return false
		}
	}
	return true
}

func statusFromDesiredConfigMaps(bundle *olmv1alpha1.Bundle, desiredConfigMaps []corev1.ConfigMap) (*olmv1alpha1.BundleStatus, error) {
	configMaps := map[string]corev1.ConfigMap{}
	for _, cm := range desiredConfigMaps {
		configMaps[cm.Name] = cm
	}

	metadataConfigMapName := util.MetadataConfigMapName(bundle.Name)
	metadataCm, ok := configMaps[metadataConfigMapName]
	if !ok {
		return nil, fmt.Errorf("cannot find metadata config map %q in list of desired config maps", metadataConfigMapName)
	}

	bundleStatus := &olmv1alpha1.BundleStatus{
		Info: &olmv1alpha1.BundleInfo{
			Package: metadataCm.Data["package-name"],
			Name:    metadataCm.Data["bundle-name"],
			Version: metadataCm.Data["bundle-version"],
		},
		Digest: metadataCm.Data["bundle-image"],
	}

	cmNames := []string{}
	if err := json.Unmarshal([]byte(metadataCm.Data["objects"]), &cmNames); err != nil {
		return nil, fmt.Errorf("unmarshal object refs from metadata configmap: %v", err)
	}

	sort.Strings(cmNames)
	for _, cmName := range cmNames {
		cm, ok := configMaps[cmName]
		if !ok {
			return nil, fmt.Errorf("cannot find object config map %q in list of desired config maps", cmName)
		}
		bundleStatus.Info.Objects = append(bundleStatus.Info.Objects, olmv1alpha1.BundleObject{
			APIVersion:   cm.Data["object-apiversion"],
			Kind:         cm.Data["object-kind"],
			Name:         cm.Data["object-name"],
			Namespace:    cm.Data["object-namespace"],
			ConfigMapRef: corev1.LocalObjectReference{Name: cmName},
		})
	}

	return bundleStatus, nil
}
