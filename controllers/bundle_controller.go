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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
	"io"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
	"github.com/joelanford/kuberpak/internal/updater"
	"github.com/joelanford/kuberpak/internal/util"
)

// BundleReconciler reconciles a Bundle object
type BundleReconciler struct {
	client.Client
	KubeClient kubernetes.Interface
	Scheme     *runtime.Scheme

	PodNamespace string
	UnpackImage  string
}

//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundles,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundles/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=olm.operatorframework.io,resources=bundles/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=pods;secrets;configmaps,verbs=get;list;watch;create;delete

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
		updater.SetPhase(olmv1alpha1.PhasePending),
		updater.EnsureCondition(metav1.Condition{
			Type:   olmv1alpha1.TypeUnpacked,
			Status: metav1.ConditionFalse,
			Reason: olmv1alpha1.ReasonUnpackPending,
		}),
	)

	pod := &corev1.Pod{}
	if op, err := r.ensureUnpackPod(ctx, bundle, pod); err != nil {
		u.UpdateStatus(
			updater.SetBundleInfo(nil),
			updater.EnsureBundleDigest(""),
			updater.SetPhase(olmv1alpha1.PhaseFailing),
			updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeUnpacked,
				Status:  metav1.ConditionFalse,
				Reason:  olmv1alpha1.ReasonUnpackFailed,
				Message: err.Error(),
			}),
		)
		return ctrl.Result{}, fmt.Errorf("ensure unpack pod: %v", err)
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		return ctrl.Result{}, nil
	}

	switch phase := pod.Status.Phase; phase {
	case corev1.PodPending:
		var messages []string
		for _, cStatus := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
			if cStatus.State.Waiting != nil && cStatus.State.Waiting.Reason == "ErrImagePull" {
				messages = append(messages, cStatus.State.Waiting.Message)
			}
			if cStatus.State.Waiting != nil && cStatus.State.Waiting.Reason == "ImagePullBackoff" {
				messages = append(messages, cStatus.State.Waiting.Message)
			}
		}
		u.UpdateStatus(
			updater.SetBundleInfo(nil),
			updater.EnsureBundleDigest(""),
			updater.SetPhase(olmv1alpha1.PhasePending),
			updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeUnpacked,
				Status:  metav1.ConditionFalse,
				Reason:  olmv1alpha1.ReasonUnpackPending,
				Message: strings.Join(messages, "; "),
			}),
		)
		return ctrl.Result{}, nil
	case corev1.PodRunning:
		u.UpdateStatus(
			updater.SetBundleInfo(nil),
			updater.EnsureBundleDigest(""),
			updater.SetPhase(olmv1alpha1.PhaseUnpacking),
			updater.EnsureCondition(metav1.Condition{
				Type:   olmv1alpha1.TypeUnpacked,
				Status: metav1.ConditionFalse,
				Reason: olmv1alpha1.ReasonUnpacking,
			}),
		)
		return ctrl.Result{}, nil
	case corev1.PodFailed:
		u.UpdateStatus(
			updater.SetBundleInfo(nil),
			updater.EnsureBundleDigest(""),
			updater.SetPhase(olmv1alpha1.PhaseFailing),
		)
		logs, err := r.getPodLogs(ctx, bundle)
		if err != nil {
			u.UpdateStatus(
				updater.EnsureCondition(metav1.Condition{
					Type:    olmv1alpha1.TypeUnpacked,
					Status:  metav1.ConditionFalse,
					Reason:  olmv1alpha1.ReasonUnpackFailed,
					Message: fmt.Sprintf("unpack failed: failed to retrieve failed pod logs: %v", err),
				}),
			)
		}
		logStr := string(logs)
		u.UpdateStatus(
			updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeUnpacked,
				Status:  metav1.ConditionFalse,
				Reason:  olmv1alpha1.ReasonUnpackFailed,
				Message: logStr,
			}),
		)
		_ = r.Delete(ctx, pod)
		return ctrl.Result{}, fmt.Errorf("unpack failed: %v", logStr)
	case corev1.PodSucceeded:
		status, err := r.handleCompletedPod(ctx, bundle)
		if err != nil {
			u.UpdateStatus(
				updater.SetPhase(olmv1alpha1.PhaseFailing),
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
			updater.SetPhase(olmv1alpha1.PhaseUnpacked),
			updater.EnsureCondition(metav1.Condition{
				Type:   olmv1alpha1.TypeUnpacked,
				Status: metav1.ConditionTrue,
				Reason: olmv1alpha1.ReasonUnpackSuccessful,
			}),
		)

		return ctrl.Result{}, nil
	default:
		return ctrl.Result{}, fmt.Errorf("unexpected pod phase: %v", phase)
	}
}

func (r *BundleReconciler) getPodLogs(ctx context.Context, bundle *olmv1alpha1.Bundle) ([]byte, error) {
	logReader, err := r.KubeClient.CoreV1().Pods(r.PodNamespace).GetLogs(util.PodName(bundle.Name), &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		return nil, err
	}
	defer logReader.Close()
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, logReader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *BundleReconciler) handleCompletedPod(ctx context.Context, bundle *olmv1alpha1.Bundle) (*olmv1alpha1.BundleStatus, error) {
	desiredConfigMapsData, err := r.getPodLogs(ctx, bundle)
	if err != nil {
		return nil, err
	}
	desiredConfigMaps := &corev1.ConfigMapList{}
	if err := json.Unmarshal(desiredConfigMapsData, desiredConfigMaps); err != nil {
		return nil, err
	}

	actualConfigMaps := &corev1.ConfigMapList{}
	if err := r.List(ctx, actualConfigMaps, client.MatchingLabels(util.BundleLabels(bundle.Name)), client.InNamespace(r.PodNamespace)); err != nil {
		return nil, err
	}

	if err := r.ensureDesiredConfigMaps(ctx, actualConfigMaps.Items, desiredConfigMaps.Items); err != nil {
		return nil, err
	}

	return statusFromDesiredConfigMaps(bundle, desiredConfigMaps.Items)
}

// SetupWithManager sets up the controller with the Manager.
func (r *BundleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&olmv1alpha1.Bundle{}, builder.WithPredicates(
			bundleProvisionerFilter("kuberpak.io/registry+v1"),
		)).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.ConfigMap{}).
		Watches(&source.Kind{Type: &corev1.Secret{}}, remoteSecretHandler(mgr.GetClient(), mgr.GetLogger())).
		WithOptions(controller.Options{MaxConcurrentReconciles: 128}).
		Complete(r)
}

func bundleProvisionerFilter(provisionerClassName string) predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		b := obj.(*olmv1alpha1.Bundle)
		return b.Spec.ProvisionerClassName == provisionerClassName
	})
}

func remoteSecretHandler(cl client.Client, log logr.Logger) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		secret := object.(*corev1.Secret)
		bundles := &olmv1alpha1.BundleList{}
		var requests []reconcile.Request
		if err := cl.List(context.Background(), bundles); err != nil {
			log.WithName("remoteSecretHandler").Error(err, "list bundles")
			return requests
		}
		for _, b := range bundles.Items {
			b := b
			for _, ips := range b.Spec.ImagePullSecrets {
				if ips.Namespace == secret.Namespace && ips.Name == secret.Name {
					requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&b)})
				}
			}
		}
		return requests
	})
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

func (r *BundleReconciler) ensureImagePullSecrets(ctx context.Context, bundle *olmv1alpha1.Bundle) ([]corev1.Secret, error) {
	actualPullSecretsList := &corev1.SecretList{}
	if err := r.List(ctx, actualPullSecretsList, client.MatchingLabels(util.BundleLabels(bundle.Name)), client.InNamespace(r.PodNamespace)); err != nil {
		return nil, err
	}
	actualPullSecrets := map[types.NamespacedName]corev1.Secret{}
	for _, pullSecret := range actualPullSecretsList.Items {
		key := types.NamespacedName{Namespace: pullSecret.Namespace, Name: pullSecret.Name}
		actualPullSecrets[key] = pullSecret
	}

	var bundlePullSecrets []corev1.Secret
	controllerRef := metav1.NewControllerRef(bundle, bundle.GroupVersionKind())
	for _, remoteKey := range bundle.Spec.ImagePullSecrets {
		localName := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%s", remoteKey.Namespace, remoteKey.Name))))
		localSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: localName, Namespace: r.PodNamespace}}

		ips := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName(remoteKey), ips); err != nil {
			if apierrors.IsNotFound(err) {
				// Ignore missing image pull secrets. This aligns with what happens with pods
				// if a non-existent image pull localSecret is specified?
				continue
			}
			return nil, fmt.Errorf("get image pull localSecret: %v", err)
		}
		if _, err := r.createOrRecreate(ctx, localSecret, func() error {
			localSecret.SetLabels(util.BundleLabels(bundle.Name))
			localSecret.SetOwnerReferences([]metav1.OwnerReference{*controllerRef})
			immutable := true
			localSecret.Immutable = &immutable
			localSecret.Type = ips.Type
			localSecret.Data = ips.Data
			localSecret.Annotations = ips.Annotations
			localSecret.StringData = ips.StringData
			return nil
		}); err != nil {
			return nil, err
		}
		delete(actualPullSecrets, types.NamespacedName{Namespace: r.PodNamespace, Name: localName})
		bundlePullSecrets = append(bundlePullSecrets, *localSecret)
	}

	for _, aps := range actualPullSecrets {
		aps := aps
		if err := r.Delete(ctx, &aps); client.IgnoreNotFound(err) != nil {
			return nil, fmt.Errorf("delete undesired image pull secret: %v", err)
		}
	}
	return bundlePullSecrets, nil
}

func (r *BundleReconciler) ensureUnpackPod(ctx context.Context, bundle *olmv1alpha1.Bundle, pod *corev1.Pod) (controllerutil.OperationResult, error) {
	imagePullSecrets, err := r.ensureImagePullSecrets(ctx, bundle)
	if err != nil {
		return controllerutil.OperationResultNone, err
	}
	var imagePullSecretRefs []corev1.LocalObjectReference
	for _, ips := range imagePullSecrets {
		imagePullSecretRefs = append(imagePullSecretRefs, corev1.LocalObjectReference{Name: ips.Name})
	}
	sort.Slice(imagePullSecretRefs, func(i, j int) bool {
		return imagePullSecretRefs[i].Name < imagePullSecretRefs[j].Name
	})

	controllerRef := metav1.NewControllerRef(bundle, bundle.GroupVersionKind())
	pod.SetName(util.PodName(bundle.Name))
	pod.SetNamespace(r.PodNamespace)
	return r.createOrRecreate(ctx, pod, func() error {
		pod.SetLabels(util.BundleLabels(bundle.Name))
		pod.SetOwnerReferences([]metav1.OwnerReference{*controllerRef})
		pod.Spec.ImagePullSecrets = imagePullSecretRefs
		pod.Spec.ServiceAccountName = "kuberpak-bundle-unpacker"
		pod.Spec.Volumes = getVolumes(pod.Spec.Volumes, []corev1.Volume{
			{Name: "util", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "bundle", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		})
		pod.Spec.RestartPolicy = corev1.RestartPolicyNever
		if len(pod.Spec.InitContainers) != 2 {
			pod.Spec.InitContainers = make([]corev1.Container, 2)
		}
		pod.Spec.InitContainers[0].Name = "install-cpb"
		pod.Spec.InitContainers[0].Image = "quay.io/operator-framework/olm:latest"
		pod.Spec.InitContainers[0].Command = []string{"/bin/cp", "-Rv", "/bin/cpb", "/util/cpb"}
		pod.Spec.InitContainers[0].VolumeMounts = getVolumeMounts(pod.Spec.InitContainers[0].VolumeMounts, []corev1.VolumeMount{
			{Name: "util", MountPath: "/util"},
		})

		pod.Spec.InitContainers[1].Name = "copy-bundle"
		pod.Spec.InitContainers[1].Image = bundle.Spec.Image
		pod.Spec.InitContainers[1].ImagePullPolicy = corev1.PullAlways
		pod.Spec.InitContainers[1].Command = []string{"/util/cpb", "/bundle"}
		pod.Spec.InitContainers[1].VolumeMounts = getVolumeMounts(pod.Spec.InitContainers[1].VolumeMounts, []corev1.VolumeMount{
			{Name: "util", MountPath: "/util"},
			{Name: "bundle", MountPath: "/bundle"},
		})

		if len(pod.Spec.Containers) != 1 {
			pod.Spec.Containers = make([]corev1.Container, 1)
		}
		pod.Spec.Containers[0].Name = "unpack-bundle"
		pod.Spec.Containers[0].Image = r.UnpackImage
		pod.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways
		pod.Spec.Containers[0].Args = []string{
			fmt.Sprintf("--bundle-name=%s", bundle.Name),
			"--pod-name=$(POD_NAME)",
			"--pod-namespace=$(POD_NAMESPACE)",
			"--bundle-dir=/bundle",
		}
		pod.Spec.Containers[0].Env = []corev1.EnvVar{
			{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "metadata.name",
					},
				},
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "metadata.namespace",
					},
				},
			},
		}
		pod.Spec.Containers[0].VolumeMounts = getVolumeMounts(pod.Spec.Containers[0].VolumeMounts, []corev1.VolumeMount{
			{Name: "bundle", MountPath: "/bundle"},
		})
		return nil
	})
}

func getVolumes(existing []corev1.Volume, desired []corev1.Volume) []corev1.Volume {
	out := desired
	for _, e := range existing {
		if strings.HasPrefix(e.Name, "kube-api-access-") {
			out = append(out, e)
		}
	}
	return out
}

func getVolumeMounts(existing []corev1.VolumeMount, desired []corev1.VolumeMount) []corev1.VolumeMount {
	out := desired
	for _, e := range existing {
		if e.MountPath == "/var/run/secrets/kubernetes.io/serviceaccount" {
			out = append(out, e)
		}
	}
	return out
}

func (r *BundleReconciler) createOrRecreate(ctx context.Context, obj client.Object, f controllerutil.MutateFn) (controllerutil.OperationResult, error) {
	key := client.ObjectKeyFromObject(obj)
	if err := r.Get(ctx, key, obj); err != nil {
		if !apierrors.IsNotFound(err) {
			return controllerutil.OperationResultNone, err
		}
		if err := mutate(f, key, obj); err != nil {
			return controllerutil.OperationResultNone, err
		}
		if err := r.Create(ctx, obj); err != nil {
			return controllerutil.OperationResultNone, err
		}
		return controllerutil.OperationResultCreated, nil
	}

	existing := obj.DeepCopyObject() //nolint
	if err := mutate(f, key, obj); err != nil {
		return controllerutil.OperationResultNone, err
	}

	if equality.Semantic.DeepEqual(existing, obj) {
		return controllerutil.OperationResultNone, nil
	}

	if err := wait.PollImmediateUntil(time.Millisecond*5, func() (done bool, err error) {
		if err := r.Delete(ctx, obj); err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	}, ctx.Done()); err != nil {
		return controllerutil.OperationResultNone, err
	}

	obj.SetUID("")
	obj.SetResourceVersion("")
	obj.SetGeneration(0)
	if err := r.Create(ctx, obj); err != nil {
		return controllerutil.OperationResultNone, err
	}
	return controllerutil.OperationResultUpdated, nil
}

// mutate wraps a MutateFn and applies validation to its result.
func mutate(f controllerutil.MutateFn, key client.ObjectKey, obj client.Object) error {
	if err := f(); err != nil {
		return err
	}
	if newKey := client.ObjectKeyFromObject(obj); key != newKey {
		return fmt.Errorf("MutateFn cannot mutate object name and/or object namespace")
	}
	return nil
}
