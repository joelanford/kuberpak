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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
	"github.com/joelanford/kuberpak/internal/updater"
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
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;delete

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

	//actualConfigMaps := &corev1.ConfigMapList{}
	//if err := r.List(ctx, actualConfigMaps, &client.ListOptions{LabelSelector: labels.SelectorFromSet(labels.Set{
	//	"kuberpak.io/bundle-name": bundle.Name,
	//})}); err != nil {
	//	u.UpdateStatus(
	//		updater.EnsureCondition(metav1.Condition{
	//			Type:    olmv1alpha1.TypeUnpacked,
	//			Status:  metav1.ConditionFalse,
	//			Reason:  olmv1alpha1.ReasonListConfigMapsFailed,
	//			Message: err.Error(),
	//		}),
	//		updater.UnsetBundleInfo(),
	//	)
	//	return ctrl.Result{}, fmt.Errorf("list configmaps: %v", err)
	//}
	//expectedConfigMaps := []corev1.ConfigMap{}
	//defer func() {
	//	if err := r.ensureExpectedConfigMaps(ctx, actualConfigMaps.Items, expectedConfigMaps); err != nil {
	//		u.UpdateStatus(updater.EnsureCondition(metav1.Condition{
	//			Type:    olmv1alpha1.TypeUnpacked,
	//			Status:  metav1.ConditionFalse,
	//			Reason:  olmv1alpha1.ReasonReconcileConfigMapsFailed,
	//			Message: err.Error(),
	//		}))
	//		if reconcileErr == nil {
	//			reconcileErr = err
	//		}
	//	}
	//}()

	unpacker := r.GetUnpacker(bundle)
	//l.Info("getting digest")
	//digest, err := unpacker.GetDigest(ctx)
	//if err != nil {
	//	u.UpdateStatus(
	//		updater.EnsureCondition(metav1.Condition{
	//			Type:    olmv1alpha1.TypeUnpacked,
	//			Status:  metav1.ConditionFalse,
	//			Reason:  olmv1alpha1.ReasonGetDigestFailed,
	//			Message: err.Error(),
	//		}),
	//		updater.RemoveConditions(olmv1alpha1.TypeFailedClusterRead, olmv1alpha1.TypeFailedClusterWrite),
	//		updater.UnsetBundleInfo(),
	//		updater.EnsureBundleDigest(""),
	//	)
	//	return ctrl.Result{}, fmt.Errorf("get digest: %v", err)
	//}
	//u.UpdateStatus(updater.EnsureBundleDigest(digest))
	//
	//ref, err := name.ParseReference(bundle.Spec.Image)
	//if err != nil {
	//	u.UpdateStatus(
	//		updater.EnsureCondition(metav1.Condition{
	//			Type:    olmv1alpha1.TypeUnpacked,
	//			Status:  metav1.ConditionFalse,
	//			Reason:  olmv1alpha1.ReasonInvalidImageReference,
	//			Message: err.Error(),
	//		}),
	//		updater.RemoveConditions(olmv1alpha1.TypeFailedClusterRead, olmv1alpha1.TypeFailedClusterWrite),
	//		updater.UnsetBundleInfo(),
	//	)
	//	return ctrl.Result{}, fmt.Errorf("parse image reference: %v", err)
	//}
	//resolvedImage := fmt.Sprintf("%s@%s", ref.Context(), digest)

	l.Info("updating bundle contents")
	u.UpdateStatus(
		updater.UnsetBundleInfo(),
	)

	l.Info("unpacking bundle")
	if err := unpacker.Unpack(ctx); err != nil {
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

	//version, err := rBundle.Version()
	//if err != nil {
	//	u.UpdateStatus(
	//		updater.EnsureCondition(metav1.Condition{
	//			Type:    olmv1alpha1.TypeUnpacked,
	//			Status:  metav1.ConditionFalse,
	//			Reason:  olmv1alpha1.ReasonInvalidBundle,
	//			Message: err.Error(),
	//		}),
	//	)
	//	return ctrl.Result{}, fmt.Errorf("extract version from bundle: %v", err)
	//}
	u.UpdateStatus(updater.EnsureCondition(metav1.Condition{
		Type:   olmv1alpha1.TypeUnpacked,
		Status: metav1.ConditionTrue,
		Reason: olmv1alpha1.ReasonUnpackSuccessful,
	}))

	//objs := []corev1.ObjectReference{}
	//for _, obj := range rBundle.Objects {
	//	objData, err := yaml.Marshal(obj)
	//	if err != nil {
	//		return ctrl.Result{}, fmt.Errorf("marshal object as YAML: %v", err)
	//	}
	//	hash := fmt.Sprintf("%x", sha256.Sum256(append([]byte(rBundle.Name), objData...)))
	//	objCompressed := &bytes.Buffer{}
	//	gzipper := gzip.NewWriter(objCompressed)
	//	if _, err := gzipper.Write(objData); err != nil {
	//		u.UpdateStatus(updater.EnsureCondition(metav1.Condition{
	//			Type:    olmv1alpha1.TypeFailedClusterWrite,
	//			Status:  metav1.ConditionTrue,
	//			Reason:  olmv1alpha1.ReasonReconcileConfigMapsFailed,
	//			Message: err.Error(),
	//		}))
	//		return ctrl.Result{}, fmt.Errorf("gzip object data for configmap: %v", err)
	//	}
	//	if err := gzipper.Close(); err != nil {
	//		u.UpdateStatus(updater.EnsureCondition(metav1.Condition{
	//			Type:    olmv1alpha1.TypeFailedClusterWrite,
	//			Status:  metav1.ConditionTrue,
	//			Reason:  olmv1alpha1.ReasonReconcileConfigMapsFailed,
	//			Message: err.Error(),
	//		}))
	//		return ctrl.Result{}, fmt.Errorf("close gzip writer for configmap: %v", err)
	//	}
	//	immutable := true
	//	cm := corev1.ConfigMap{
	//		ObjectMeta: metav1.ObjectMeta{
	//			Name:      fmt.Sprintf("bundle-object-%s-%s", rBundle.Package, hash[0:8]),
	//			Namespace: "kuberpak-system",
	//			Labels: map[string]string{
	//				"kuberpak.io/bundle-name": bundle.Name,
	//			},
	//		},
	//		Immutable: &immutable,
	//		Data: map[string]string{
	//			"bundle-image":  resolvedImage,
	//			"object-sha256": hash,
	//		},
	//		BinaryData: map[string][]byte{
	//			"object": objCompressed.Bytes(),
	//		},
	//	}
	//	if err := controllerutil.SetControllerReference(bundle, &cm, r.Scheme); err != nil {
	//		u.UpdateStatus(updater.EnsureCondition(metav1.Condition{
	//			Type:    olmv1alpha1.TypeUnpacked,
	//			Status:  metav1.ConditionFalse,
	//			Reason:  olmv1alpha1.ReasonReconcileConfigMapsFailed,
	//			Message: err.Error(),
	//		}))
	//		return ctrl.Result{}, fmt.Errorf("set owner reference on configmap: %v", err)
	//	}
	//	expectedConfigMaps = append(expectedConfigMaps, cm)
	//	objs = append(objs, corev1.ObjectReference{
	//		APIVersion: obj.GetAPIVersion(),
	//		Kind:       obj.GetKind(),
	//		Namespace:  obj.GetNamespace(),
	//		Name:       obj.GetName(),
	//	})
	//}
	//
	//u.UpdateStatus(
	//	updater.EnsureCondition(metav1.Condition{
	//		Type:   olmv1alpha1.TypeFailedClusterWrite,
	//		Status: metav1.ConditionFalse,
	//		Reason: olmv1alpha1.ReasonClusterWriteSuccessful,
	//	}),
	//)

	return ctrl.Result{}, nil
}

//
//func (r *BundleReconciler) ensureExpectedConfigMaps(ctx context.Context, actual, expected []corev1.ConfigMap) error {
//	existingCms := map[types.NamespacedName]corev1.ConfigMap{}
//	for _, cm := range actual {
//		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
//		existingCms[key] = cm
//	}
//
//	for _, cm := range expected {
//		cm := cm
//		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
//		if ecm, ok := existingCms[key]; ok {
//			if ecm.Data["kuberpak.io_object-sha256"] == cm.Data["kuberpak.io_object-sha256"] {
//				delete(existingCms, key)
//				continue
//			}
//			if err := r.Delete(ctx, &ecm); client.IgnoreNotFound(err) != nil {
//				return fmt.Errorf("delete configmap: %v", err)
//			}
//		}
//		if err := r.Client.Create(ctx, &cm); err != nil {
//			return fmt.Errorf("create configmap: %v", err)
//		}
//	}
//	for _, ecm := range existingCms {
//		if err := r.Delete(ctx, &ecm); client.IgnoreNotFound(err) != nil {
//			return fmt.Errorf("delete configmap: %v", err)
//		}
//	}
//	return nil
//}

func bundleProvisionerFilter(provisionerClassName string) predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		b := obj.(*olmv1alpha1.Bundle)
		return b.Spec.ProvisionerClassName == provisionerClassName
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *BundleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&olmv1alpha1.Bundle{}, builder.WithPredicates(bundleProvisionerFilter("kuberpak.io/registry+v1"))).
		Owns(&corev1.ConfigMap{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 128}).
		Complete(r)
}
