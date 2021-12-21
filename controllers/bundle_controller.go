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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing/fstest"

	"github.com/go-logr/logr"
	"github.com/operator-framework/operator-registry/pkg/registry"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	apimachyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/yaml"

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
//+kubebuilder:rbac:groups=core,resources=pods/log,verbs=get

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
		if err := u.Apply(ctx, bundle); err != nil {
			l.Error(err, "failed to update status")
		}
	}()
	u.UpdateStatus(
		updater.EnsureObservedGeneration(bundle.Generation),
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
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated || pod.DeletionTimestamp != nil {
		u.UpdateStatus(
			updater.SetBundleInfo(nil),
			updater.EnsureBundleDigest(""),
			updater.SetPhase(olmv1alpha1.PhasePending),
			updater.EnsureCondition(metav1.Condition{
				Type:   olmv1alpha1.TypeUnpacked,
				Status: metav1.ConditionFalse,
				Reason: olmv1alpha1.ReasonUnpackPending,
			}),
		)
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
			err = fmt.Errorf("unpack failed: failed to retrieve failed pod logs: %v", err)
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
		err := fmt.Errorf("unexpected pod phase: %v", phase)
		u.UpdateStatus(
			updater.SetPhase(olmv1alpha1.PhaseFailing),
			updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeUnpacked,
				Status:  metav1.ConditionFalse,
				Reason:  olmv1alpha1.ReasonUnpackFailed,
				Message: err.Error(),
			}),
		)
		_ = r.Delete(ctx, pod)
		return ctrl.Result{}, err
	}
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
	automountServiceAccountToken := false
	pod.SetName(util.PodName(bundle.Name))
	pod.SetNamespace(r.PodNamespace)
	return util.CreateOrRecreate(ctx, r.Client, pod, func() error {
		pod.SetLabels(util.BundleLabels(bundle.Name))
		pod.SetOwnerReferences([]metav1.OwnerReference{*controllerRef})
		pod.Spec.AutomountServiceAccountToken = &automountServiceAccountToken
		pod.Spec.ImagePullSecrets = imagePullSecretRefs
		pod.Spec.Volumes = []corev1.Volume{
			{Name: "util", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "bundle", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		}
		pod.Spec.RestartPolicy = corev1.RestartPolicyNever
		if len(pod.Spec.InitContainers) != 2 {
			pod.Spec.InitContainers = make([]corev1.Container, 2)
		}
		pod.Spec.InitContainers[0].Name = "install-cpb"
		pod.Spec.InitContainers[0].Image = "quay.io/operator-framework/olm:latest"
		pod.Spec.InitContainers[0].Command = []string{"/bin/cp", "-Rv", "/bin/cpb", "/util/cpb"}
		pod.Spec.InitContainers[0].VolumeMounts = []corev1.VolumeMount{{Name: "util", MountPath: "/util"}}

		pod.Spec.InitContainers[1].Name = "copy-bundle"
		pod.Spec.InitContainers[1].Image = bundle.Spec.Image
		pod.Spec.InitContainers[1].ImagePullPolicy = corev1.PullAlways
		pod.Spec.InitContainers[1].Command = []string{"/util/cpb", "/bundle"}
		pod.Spec.InitContainers[1].VolumeMounts = []corev1.VolumeMount{
			{Name: "util", MountPath: "/util"},
			{Name: "bundle", MountPath: "/bundle"},
		}

		if len(pod.Spec.Containers) != 1 {
			pod.Spec.Containers = make([]corev1.Container, 1)
		}
		pod.Spec.Containers[0].Name = "unpack-bundle"
		pod.Spec.Containers[0].Image = r.UnpackImage
		pod.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways
		pod.Spec.Containers[0].Args = []string{"--bundle-dir=/bundle"}
		pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "bundle", MountPath: "/bundle"}}
		return nil
	})
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
		if _, err := util.CreateOrRecreate(ctx, r.Client, localSecret, func() error {
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

func (r *BundleReconciler) handleCompletedPod(ctx context.Context, bundle *olmv1alpha1.Bundle) (*olmv1alpha1.BundleStatus, error) {
	bundleFS, err := r.getBundleContents(ctx, bundle)
	if err != nil {
		return nil, fmt.Errorf("get bundle contents: %v", err)
	}

	bundleImageDigest, err := r.getBundleImageDigest(ctx, bundle)
	if err != nil {
		return nil, fmt.Errorf("get bundle image digest: %v", err)
	}

	annotations, err := getAnnotations(bundleFS)
	if err != nil {
		return nil, fmt.Errorf("get bundle annotations: %v", err)
	}

	objects, err := getObjects(bundleFS)
	if err != nil {
		return nil, fmt.Errorf("get objects from bundle manifests: %v", err)
	}

	desiredConfigMaps, err := r.getDesiredConfigMaps(bundle, bundleImageDigest, annotations, objects)
	if err != nil {
		return nil, fmt.Errorf("get desired configmaps: %v", err)
	}

	actualConfigMaps := &corev1.ConfigMapList{}
	if err := r.List(ctx, actualConfigMaps, client.MatchingLabels(util.BundleLabels(bundle.Name)), client.InNamespace(r.PodNamespace)); err != nil {
		return nil, fmt.Errorf("list actual configmaps: %v", err)
	}

	if err := r.ensureDesiredConfigMaps(ctx, actualConfigMaps.Items, desiredConfigMaps.Items); err != nil {
		return nil, fmt.Errorf("ensure desired configmaps: %v", err)
	}

	st, err := statusFromDesiredConfigMaps(bundle, desiredConfigMaps.Items)
	if err != nil {
		return nil, fmt.Errorf("derive status update from desired configmaps: %v", err)
	}
	return st, nil
}

func (r *BundleReconciler) getBundleContents(ctx context.Context, bundle *olmv1alpha1.Bundle) (fs.FS, error) {
	bundleContentsData, err := r.getPodLogs(ctx, bundle)
	if err != nil {
		return nil, fmt.Errorf("get bundle contents: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(bundleContentsData))
	bundleContents := map[string][]byte{}
	if err := decoder.Decode(&bundleContents); err != nil {
		return nil, err
	}
	bundleFS := fstest.MapFS{}
	for name, data := range bundleContents {
		bundleFS[name] = &fstest.MapFile{Data: data}
	}
	return bundleFS, nil
}

func (r *BundleReconciler) getPodLogs(ctx context.Context, bundle *olmv1alpha1.Bundle) ([]byte, error) {
	logReader, err := r.KubeClient.CoreV1().Pods(r.PodNamespace).GetLogs(util.PodName(bundle.Name), &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("get pod logs: %v", err)
	}
	defer logReader.Close()
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, logReader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *BundleReconciler) getBundleImageDigest(ctx context.Context, bundle *olmv1alpha1.Bundle) (string, error) {
	podKey := types.NamespacedName{Namespace: r.PodNamespace, Name: util.PodName(bundle.Name)}
	pod := &corev1.Pod{}
	if err := r.Get(ctx, podKey, pod); err != nil {
		return "", err
	}
	for _, ps := range pod.Status.InitContainerStatuses {
		if ps.Name == "copy-bundle" && ps.ImageID != "" {
			return ps.ImageID, nil
		}
	}
	return "", fmt.Errorf("bundle image digest not found")
}

func getAnnotations(bundleFS fs.FS) (*registry.Annotations, error) {
	fileData, err := fs.ReadFile(bundleFS, filepath.Join("metadata", "annotations.yaml"))
	if err != nil {
		return nil, err
	}
	annotationsFile := registry.AnnotationsFile{}
	if err := yaml.Unmarshal(fileData, &annotationsFile); err != nil {
		return nil, err
	}
	return &annotationsFile.Annotations, nil
}

func getObjects(bundleFS fs.FS) ([]unstructured.Unstructured, error) {
	var objects []unstructured.Unstructured
	const manifestsDir = "manifests"

	entries, err := fs.ReadDir(bundleFS, manifestsDir)
	if err != nil {
		return nil, fmt.Errorf("read manifests: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fileData, err := fs.ReadFile(bundleFS, filepath.Join(manifestsDir, e.Name()))
		if err != nil {
			return nil, err
		}

		dec := apimachyaml.NewYAMLOrJSONDecoder(bytes.NewReader(fileData), 1024)
		for {
			obj := unstructured.Unstructured{}
			err := dec.Decode(&obj)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}
			objects = append(objects, obj)
		}
	}
	return objects, nil
}

func (r *BundleReconciler) getDesiredConfigMaps(bundle *olmv1alpha1.Bundle, resolvedImage string, annotations *registry.Annotations, objects []unstructured.Unstructured) (*corev1.ConfigMapList, error) {
	var desiredConfigMaps []corev1.ConfigMap

	var (
		pkgName       = annotations.PackageName
		csvName       string
		bundleVersion string
	)

	for _, obj := range objects {
		if obj.GetObjectKind().GroupVersionKind().Kind == "ClusterServiceVersion" {
			if v, found, err := unstructured.NestedString(obj.Object, "spec", "version"); found && err == nil {
				bundleVersion = v
			}
			if v, found, err := unstructured.NestedString(obj.Object, "metadata", "name"); found && err == nil {
				csvName = v
			}
		}
	}
	immutable := true
	controllerRef := metav1.NewControllerRef(bundle, bundle.GroupVersionKind())
	for _, obj := range objects {
		objData, err := yaml.Marshal(obj.Object)
		if err != nil {
			return nil, err
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(objData))
		objCompressed := &bytes.Buffer{}
		gzipper := gzip.NewWriter(objCompressed)
		if _, err := gzipper.Write(objData); err != nil {
			return nil, fmt.Errorf("gzip object data: %v", err)
		}
		if err := gzipper.Close(); err != nil {
			return nil, fmt.Errorf("close gzip writer: %v", err)
		}
		gvk := obj.GetObjectKind().GroupVersionKind()
		labels := mergeMaps(util.BundleLabels(bundle.Name), map[string]string{
			"kuberpak.io/configmap-type":   "object",
			"kuberpak.io/package-name":     pkgName,
			"kuberpak.io/csv-name":         csvName,
			"kuberpak.io/bundle-version":   bundleVersion,
			"kuberpak.io/object-group":     gvk.Group,
			"kuberpak.io/object-version":   gvk.Version,
			"kuberpak.io/object-kind":      gvk.Kind,
			"kuberpak.io/object-name":      obj.GetName(),
			"kuberpak.io/object-namespace": obj.GetNamespace(),
		})
		cm := corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("bundle-object-%s-%s", bundle.Name, hash[0:8]),
				Namespace:       r.PodNamespace,
				Labels:          labels,
				OwnerReferences: []metav1.OwnerReference{*controllerRef},
			},
			Immutable: &immutable,
			Data: map[string]string{
				"bundle-image":  resolvedImage,
				"object-sha256": hash,
			},
			BinaryData: map[string][]byte{
				"object": objCompressed.Bytes(),
			},
		}
		desiredConfigMaps = append(desiredConfigMaps, cm)
	}
	objectConfigMaps := []string{}
	for _, dcm := range desiredConfigMaps {
		objectConfigMaps = append(objectConfigMaps, dcm.Name)
	}
	ocmJson, err := json.Marshal(objectConfigMaps)
	if err != nil {
		return nil, fmt.Errorf("marshal object configmap names as json: %v", err)
	}
	labels := mergeMaps(util.BundleLabels(bundle.Name), map[string]string{
		"kuberpak.io/configmap-type": "metadata",
		"kuberpak.io/package-name":   pkgName,
		"kuberpak.io/csv-name":       csvName,
		"kuberpak.io/bundle-version": bundleVersion,
	})
	metadataCm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("bundle-metadata-%s", bundle.Name),
			Namespace:       r.PodNamespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*controllerRef},
		},
		Immutable: &immutable,
		Data: map[string]string{
			"objects":      string(ocmJson),
			"bundle-image": resolvedImage,
		},
	}
	cmList := &corev1.ConfigMapList{
		Items: append(desiredConfigMaps, metadataCm),
	}
	return cmList, nil
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
			if util.ConfigMapsEqual(acm, cm) {
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
			Package: metadataCm.Labels["kuberpak.io/package-name"],
			Name:    metadataCm.Labels["kuberpak.io/csv-name"],
			Version: metadataCm.Labels["kuberpak.io/bundle-version"],
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
		gvk := schema.GroupVersionKind{
			Group:   cm.Labels["kuberpak.io/object-group"],
			Version: cm.Labels["kuberpak.io/object-version"],
			Kind:    cm.Labels["kuberpak.io/object-kind"],
		}

		bundleStatus.Info.Objects = append(bundleStatus.Info.Objects, olmv1alpha1.BundleObject{
			APIVersion:   gvk.GroupVersion().String(),
			Kind:         gvk.Kind,
			Name:         cm.Labels["kuberpak.io/object-name"],
			Namespace:    cm.Labels["kuberpak.io/object-namespace"],
			ConfigMapRef: corev1.LocalObjectReference{Name: cmName},
		})
	}

	return bundleStatus, nil
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
		Watches(&source.Kind{Type: &corev1.Secret{}}, mapSecretToBundleHandler(mgr.GetClient(), mgr.GetLogger())).
		Complete(r)
}

func bundleProvisionerFilter(provisionerClassName string) predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		b := obj.(*olmv1alpha1.Bundle)
		return b.Spec.ProvisionerClassName == provisionerClassName
	})
}

func mapSecretToBundleHandler(cl client.Client, log logr.Logger) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(object client.Object) []reconcile.Request {
		secret := object.(*corev1.Secret)
		bundles := &olmv1alpha1.BundleList{}
		var requests []reconcile.Request
		if err := cl.List(context.Background(), bundles); err != nil {
			log.WithName("mapSecretToBundleHandler").Error(err, "list bundles")
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

func mergeMaps(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}
