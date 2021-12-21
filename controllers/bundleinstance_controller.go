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
	"io"
	"io/ioutil"
	"sort"

	"github.com/go-logr/logr"
	helmclient "github.com/operator-framework/helm-operator-plugins/pkg/client"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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
	"github.com/joelanford/kuberpak/internal/util"
)

// BundleInstanceReconciler reconciles a BundleInstance object
type BundleInstanceReconciler struct {
	client.Client
	ActionConfigGetter helmclient.ActionConfigGetter
	ActionClientGetter helmclient.ActionClientGetter
	PodNamespace       string
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
	l.Info("starting reconciliation")
	defer l.Info("ending reconciliation")

	bi := &olmv1alpha1.BundleInstance{}
	if err := r.Get(ctx, req.NamespacedName, bi); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	defer func() {
		bi := bi.DeepCopy()
		bi.ObjectMeta.ManagedFields = nil
		if err := r.Status().Patch(ctx, bi, client.Apply, client.FieldOwner("kuberpak.io/registry+v1")); err != nil {
			l.Error(err, "failed to patch status")
		}
	}()

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
	installNamespace := bi.Annotations["kuberpak.io/install-namespace"]
	desiredCRDs, desiredObjects, err := r.getDesiredObjects(ctx, bi, installNamespace)
	if err != nil {
		var bnuErr *errBundleNotUnpacked
		if errors.As(err, &bnuErr) {
			reason := fmt.Sprintf("BundleUnpack%s", b.Status.Phase)
			if b.Status.Phase == olmv1alpha1.PhaseUnpacking {
				reason = "BundleUnpackRunning"
			}
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:   "Installed",
				Status: metav1.ConditionFalse,
				Reason: reason,
			})
			return ctrl.Result{}, nil
		} else {
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:    "Installed",
				Status:  metav1.ConditionFalse,
				Reason:  "BundleLookupFailed",
				Message: err.Error(),
			})
			return ctrl.Result{}, err
		}
	}
	_ = desiredObjects

	createOrUpdateAll := func(ctx context.Context, cl client.Client, crds []apiextensionsv1.CustomResourceDefinition) error {
		for _, crd := range crds {
			if _, err := util.CreateOrUpdateCRD(ctx, cl, &crd); err != nil {
				return err
			}
		}
		return nil
	}

	for _, cl := range []client.Client{client.NewDryRunClient(r.Client), r.Client} {
		if err := createOrUpdateAll(ctx, cl, desiredCRDs); err != nil {
			meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
				Type:    "Installed",
				Status:  metav1.ConditionFalse,
				Reason:  "CRDInstallFailed",
				Message: err.Error(),
			})
			return ctrl.Result{}, err
		}
	}

	//currentManifest, err := r.getCurrentManifest(ctx, bi)
	//if err != nil {
	//	meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
	//		Type:    "Installed",
	//		Status:  metav1.ConditionUnknown,
	//		Reason:  "CurrentManifestLookupFailed",
	//		Message: err.Error(),
	//	})
	//	return ctrl.Result{}, err
	//}
	//
	//if err := func() error {
	//	switch currentManifest {
	//	case "": // install
	//		// doInstall
	//		// create manifest
	//	case desiredManifest: //reconcile
	//	default: // update
	//	}
	//	return nil
	//}(); err != nil {
	//
	//}
	//
	//chrt := &chart.Chart{Metadata: &chart.Metadata{Name: b.Name}}
	//for i, obj := range desiredObjects {
	//	gvk := obj.GetObjectKind().GroupVersionKind()
	//	l.Info("found object",
	//		"apiVersion", gvk.GroupVersion().String(),
	//		"kind", gvk.Kind,
	//		"name", obj.GetName(),
	//		"namespace", obj.GetNamespace(),
	//	)
	//	objData, err := yaml.Marshal(obj.Object)
	//	if err != nil {
	//		return ctrl.Result{}, err
	//	}
	//	chrt.Templates = append(chrt.Templates, &chart.File{
	//		Name: fmt.Sprintf("manifest-%d.yaml", i),
	//		Data: objData,
	//	})
	//}
	//
	//bi.Namespace = installNamespace
	//cl, err := r.ActionClientGetter.ActionClientFor(bi)
	//bi.Namespace = ""
	//if err != nil {
	//	return ctrl.Result{}, err
	//}
	//
	//skipInstallCRDs := func(install *action.Install) error {
	//	install.SkipCRDs = true
	//	return nil
	//}
	//skipUpgradeCRDs := func(upgrade *action.Upgrade) error {
	//	upgrade.SkipCRDs = true
	//	return nil
	//}
	//
	//if _, err := cl.Get(bi.Name); errors.Is(err, driver.ErrReleaseNotFound) {
	//	if _, err := cl.Install(bi.Name, installNamespace, chrt, nil, skipInstallCRDs); err != nil {
	//		return ctrl.Result{}, err
	//	}
	//} else if err != nil {
	//	return ctrl.Result{}, err
	//} else {
	//	if _, err := cl.Upgrade(bi.Name, installNamespace, chrt, nil, skipUpgradeCRDs); err != nil {
	//		return ctrl.Result{}, err
	//	}
	//}
	meta.SetStatusCondition(&bi.Status.Conditions, metav1.Condition{
		Type:   "Installed",
		Status: metav1.ConditionTrue,
		Reason: "InstallationSucceeded",
	})
	return ctrl.Result{}, nil
}

type errBundleNotUnpacked struct {
	currentPhase string
}

func (err errBundleNotUnpacked) Error() string {
	const baseError = "bundle is not yet unpacked"
	if err.currentPhase == "" {
		return baseError
	}
	return fmt.Sprintf("%s, current phase=%s", baseError, err.currentPhase)
}

func (r *BundleInstanceReconciler) getDesiredObjects(ctx context.Context, bi *olmv1alpha1.BundleInstance, installNamespace string) ([]apiextensionsv1.CustomResourceDefinition, []unstructured.Unstructured, error) {
	b := &olmv1alpha1.Bundle{}
	if err := r.Get(ctx, types.NamespacedName{Name: bi.Spec.BundleName}, b); err != nil {
		return nil, nil, fmt.Errorf("get bundle %q: %v", bi.Spec.BundleName, err)
	}
	if b.Status.Phase != olmv1alpha1.PhaseUnpacked {
		return nil, nil, &errBundleNotUnpacked{currentPhase: b.Status.Phase}
	}

	objectConfigMaps := &corev1.ConfigMapList{}
	listOpts := []client.ListOption{client.InNamespace("kuberpak-system"), client.MatchingLabels{
		"kuberpak.io/bundle-name":    b.Name,
		"kuberpak.io/configmap-type": "object",
	}}
	if err := r.List(ctx, objectConfigMaps, listOpts...); err != nil {
		return nil, nil, fmt.Errorf("list bundle object config maps: %v", err)
	}

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

	desiredCRDs := []apiextensionsv1.CustomResourceDefinition{}
	desiredObjects := []unstructured.Unstructured{}
	for _, cm := range objectConfigMaps.Items {
		r, err := gzip.NewReader(bytes.NewReader(cm.BinaryData["object"]))
		if err != nil {
			return nil, nil, fmt.Errorf("create gzip reader for bundle object data: %v", err)
		}
		objData, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, nil, fmt.Errorf("read gzip data for bundle object: %v", err)
		}
		obj := &unstructured.Unstructured{}
		if err := yaml.Unmarshal(objData, obj); err != nil {
			return nil, nil, fmt.Errorf("unmarshal bundle object: %v", err)
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

		if obj.GetObjectKind().GroupVersionKind().Kind == "CustomResourceDefinition" {
			crd := &apiextensionsv1.CustomResourceDefinition{}
			if err := yaml.Unmarshal(objData, crd); err != nil {
				return nil, nil, fmt.Errorf("unmarshal CRD: %v", err)
			}
			desiredCRDs = append(desiredCRDs, *crd)
		} else {
			desiredObjects = append(desiredObjects, *obj)
		}
	}
	return desiredCRDs, desiredObjects, nil
}

func (r *BundleInstanceReconciler) getReleaseObjects(ctx context.Context, bi *olmv1alpha1.BundleInstance) (string, error) {
	currentManifestConfigMap := &corev1.ConfigMap{}
	currentManifestConfigMapKey := types.NamespacedName{Namespace: r.PodNamespace, Name: fmt.Sprintf("bundle-instance-manifest-%s", bi.Name)}
	err := r.Get(ctx, currentManifestConfigMapKey, currentManifestConfigMap)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	rd, err := gzip.NewReader(bytes.NewReader(currentManifestConfigMap.BinaryData["manifest"]))
	if err != nil {
		return "", err
	}
	currentManifestData, err := io.ReadAll(rd)
	if err != nil {
		return "", err
	}
	return string(currentManifestData), nil
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
