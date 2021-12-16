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
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"

	"github.com/go-logr/logr"
	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/storage/driver"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/yaml"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
)

// BundleInstanceReconciler reconciles a BundleInstance object
type BundleInstanceReconciler struct {
	client.Client
	ActionConfigGetter helmclient.ActionConfigGetter
	ActionClientGetter helmclient.ActionClientGetter
	Scheme             *runtime.Scheme
}

//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundleinstances,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundleinstances/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundleinstances/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the BundleInstance object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.9.2/pkg/reconcile
func (r *BundleInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	bi := &olmv1alpha1.BundleInstance{}
	if err := r.Get(ctx, req.NamespacedName, bi); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	b := &olmv1alpha1.Bundle{}
	if err := r.Get(ctx, types.NamespacedName{Name: bi.Spec.BundleName}, b); err != nil {
		meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
			Type:    "Installed",
			Status:  metav1.ConditionFalse,
			Reason:  "BundleLookupFailed",
			Message: err.Error(),
		})
		return ctrl.Result{}, err
	}
	if b.Status.Phase != olmv1alpha1.PhaseUnpacked {
		return ctrl.Result{}, nil
	}

	objectConfigMaps := &corev1.ConfigMapList{}
	listOpts := []client.ListOption{client.InNamespace("kuberpak-system"), client.MatchingLabels{"kuberpak.io/bundle-name": b.Name}}
	if err := r.List(ctx, objectConfigMaps, listOpts...); err != nil {
		meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
			Type:    "Installed",
			Status:  metav1.ConditionFalse,
			Reason:  "BundleLookupFailed",
			Message: err.Error(),
		})
		return ctrl.Result{}, err
	}

	installNamespace := bi.Annotations["kuberpak.io/install-namespace"]

	sort.Slice(objectConfigMaps.Items, func(i, j int) bool {
		if objectConfigMaps.Items[i].APIVersion != objectConfigMaps.Items[j].APIVersion {
			return objectConfigMaps.Items[i].APIVersion < objectConfigMaps.Items[j].APIVersion
		}
		if objectConfigMaps.Items[i].Kind != objectConfigMaps.Items[j].Kind {
			return objectConfigMaps.Items[i].Kind < objectConfigMaps.Items[j].Kind
		}
		if objectConfigMaps.Items[i].Namespace != objectConfigMaps.Items[j].Namespace {
			return objectConfigMaps.Items[i].Namespace < objectConfigMaps.Items[j].Namespace
		}
		return objectConfigMaps.Items[i].Name < objectConfigMaps.Items[j].Name
	})

	expectedObjects := []*unstructured.Unstructured{}
	for _, cm := range objectConfigMaps.Items {
		r, err := gzip.NewReader(bytes.NewReader(cm.BinaryData["object"]))
		if err != nil {
			return ctrl.Result{}, err
		}
		objData, err := ioutil.ReadAll(r)
		if err != nil {
			return ctrl.Result{}, err
		}
		obj := &unstructured.Unstructured{}
		if err := yaml.Unmarshal(objData, obj); err != nil {
			return ctrl.Result{}, err
		}
		labels := obj.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["kuberpak.io/bundle-instance-name"] = bi.Name
		obj.SetLabels(labels)
		if obj.GetObjectKind().GroupVersionKind().Kind == "ClusterServiceVersion" {
			obj.SetNamespace(installNamespace)
		}
		expectedObjects = append(expectedObjects, obj)
	}
	chrt := &chart.Chart{Metadata: &chart.Metadata{Name: b.Name}}
	for i, obj := range expectedObjects {
		gvk := obj.GetObjectKind().GroupVersionKind()
		l.Info("found object",
			"apiVersion", gvk.GroupVersion().String(),
			"kind", gvk.Kind,
			"name", obj.GetName(),
			"namespace", obj.GetNamespace(),
		)
		objData, err := yaml.Marshal(obj)
		if err != nil {
			return ctrl.Result{}, err
		}
		chrt.Templates = append(chrt.Templates, &chart.File{
			Name: fmt.Sprintf("manifest-%d.yaml", i),
			Data: objData,
		})
	}

	bi.Namespace = installNamespace
	cl, err := r.ActionClientGetter.ActionClientFor(bi)
	bi.Namespace = ""
	if err != nil {
		return ctrl.Result{}, err
	}

	if _, err := cl.Get(bi.Name); errors.Is(err, driver.ErrReleaseNotFound) {
		if _, err := cl.Install(bi.Name, installNamespace, chrt, nil); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	} else {
		if _, err := cl.Upgrade(bi.Name, installNamespace, chrt, nil); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func bundleInstanceProvisionerFilter(provisionerClassName string) predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		b := obj.(*olmv1alpha1.BundleInstance)
		return b.Spec.ProvisionerClassName == provisionerClassName
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *BundleInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.ActionConfigGetter = helmclient.NewActionConfigGetter(mgr.GetConfig(), mgr.GetRESTMapper(), mgr.GetLogger())
	r.ActionClientGetter = helmclient.NewActionClientGetter(r.ActionConfigGetter)
	return ctrl.NewControllerManagedBy(mgr).
		For(&olmv1alpha1.BundleInstance{}, builder.WithPredicates(bundleInstanceProvisionerFilter("kuberpak.io/registry+v1"))).
		Watches(&source.Kind{Type: &olmv1alpha1.Bundle{}}, handler.EnqueueRequestsFromMapFunc(mapBundleToBundleInstanceHandler(mgr.GetClient(), mgr.GetLogger()))).
		Complete(r)
}

func mapBundleToBundleInstanceHandler(cl client.Client, log logr.Logger) handler.MapFunc {
	return func(object client.Object) []reconcile.Request {
		b := object.(*olmv1alpha1.Bundle)
		bundleInstances := &olmv1alpha1.BundleInstanceList{}
		var requests []reconcile.Request
		if err := cl.List(context.Background(), bundleInstances); err != nil {
			log.WithName("mapBundleToBundleInstanceHandler").Error(err, "list bundles")
			return requests
		}
		for _, bi := range bundleInstances.Items {
			bi := bi
			if bi.Spec.BundleName == b.Name {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&bi)})
			}
		}
		return requests
	}
}
