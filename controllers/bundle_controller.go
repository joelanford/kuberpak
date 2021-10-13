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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

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

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Bundle object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.9.2/pkg/reconcile
func (r *BundleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	l.Info("starting reconciliation")
	defer l.Info("ending reconciliation")
	bundle := &olmv1alpha1.Bundle{}
	if err := r.Get(ctx, req.NamespacedName, bundle); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	u := updater.New(r.Client)
	u.UpdateStatus(
		updater.EnsureObservedGeneration(bundle.Generation),
	)
	defer func() {
		l.Info("applying status changes")
		if err := u.Apply(ctx, bundle); err != nil {
			l.Error(err, "failed to update status")
		}
	}()

	unpacker := r.GetUnpacker(bundle)
	l.Info("getting digest")
	digest, err := unpacker.GetDigest(ctx)
	if err != nil {
		u.UpdateStatus(updater.EnsureCondition(metav1.Condition{
			Type:    olmv1alpha1.TypeFailedUnpack,
			Status:  metav1.ConditionTrue,
			Reason:  olmv1alpha1.ReasonGetDigestFailure,
			Message: err.Error(),
		}))
		return ctrl.Result{}, fmt.Errorf("get digest: %v", err)
	}

	if bundle.Status.Contents == nil || bundle.Status.Contents.Digest != digest {
		l.Info("updating bundle contents")

		u.UpdateStatus(updater.UnsetBundleContents())

		l.Info("unpacking bundle")
		rBundle, err := unpacker.Unpack(ctx)
		if err != nil {
			u.UpdateStatus(updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeFailedUnpack,
				Status:  metav1.ConditionTrue,
				Reason:  olmv1alpha1.ReasonImagePullFailure,
				Message: err.Error(),
			}))
			return ctrl.Result{}, fmt.Errorf("unpack bundle: %v", err)
		}
		version, err := rBundle.Version()
		if err != nil {
			u.UpdateStatus(updater.EnsureCondition(metav1.Condition{
				Type:    olmv1alpha1.TypeFailedUnpack,
				Status:  metav1.ConditionTrue,
				Reason:  olmv1alpha1.ReasonInvalidBundle,
				Message: err.Error(),
			}))
			return ctrl.Result{}, fmt.Errorf("extract version from bundle: %v", err)
		}

		objs := []corev1.ObjectReference{}
		for _, obj := range rBundle.Objects {
			objs = append(objs, corev1.ObjectReference{
				APIVersion: obj.GetAPIVersion(),
				Kind:       obj.GetKind(),
				Namespace:  obj.GetNamespace(),
				Name:       obj.GetName(),
			})
		}

		u.UpdateStatus(
			updater.SetBundleContents(&olmv1alpha1.BundleContents{
				Package: rBundle.Package,
				Name:    rBundle.Name,
				Version: version,
				Objects: objs,
			}),
			updater.EnsureCondition(metav1.Condition{
				Type:    "FailedUnpack",
				Status:  metav1.ConditionFalse,
				Reason:  "UnpackSuccessful",
				Message: "successfully unpacked the bundle image",
			}),
		)
	}
	return ctrl.Result{}, nil
}

func (r *BundleReconciler) createUnpackJob(ctx context.Context, bundle *olmv1alpha1.Bundle) error {
	cfgmap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace,
			Name:      bundle.Name,
		},
	}
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace,
			Name:      bundle.Name,
		},
		ImagePullSecrets: bundle.Spec.ImagePullSecrets,
	}
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace,
			Name:      bundle.Name,
		},
		Rules: []rbacv1.PolicyRule{
			{
				Verbs:         []string{"get", "create", "update"},
				Resources:     []string{"configmaps"},
				APIGroups:     []string{corev1.GroupName},
				ResourceNames: []string{bundle.Name},
			},
		},
	}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace,
			Name:      bundle.Name,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      sa.Name,
				Namespace: sa.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     role.Name,
		},
	}

	ttlSecondsAfterFinished := int32(0)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace,
			Name:      bundle.Name,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: "OnFailure",
					Volumes: []corev1.Volume{
						{
							Name: "util",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: "bundle",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:    "copy-cpb",
							Image:   "quay.io/operator-framework/olm:latest",
							Command: []string{"/bin/cp", "-Rv", "/bin/cpb", "/util/cpb"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "util",
									MountPath: "/util",
								},
							},
						},
						{
							Name:    "run-cpb",
							Image:   bundle.Spec.Image,
							Command: []string{"/util/cpb", "/bundle"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "util",
									MountPath: "/util",
								},
								{
									Name:      "bundle",
									MountPath: "/bundle",
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  "bundle-extract",
							Image: "quay.io/operator-framework/opm:latest",
							Command: []string{"/bin/opm", "alpha", "bundle", "extract",
								"--gzip",
								"--manifestsdir", "/bundle/",
								"--namespace", bundle.Namespace,
								"--configmapname", bundle.Name},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "bundle",
									MountPath: "/bundle",
								},
							},
						},
					},
					ServiceAccountName: sa.Name,
					ImagePullSecrets:   bundle.Spec.ImagePullSecrets,
				},
			},
		},
	}
	for _, obj := range []client.Object{cfgmap, job} {
		obj := obj
		if err := controllerutil.SetControllerReference(bundle, obj, r.Scheme); err != nil {
			return fmt.Errorf("set owner reference on unpack-related object: %v", err)
		}
		if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete unpack-related object: %v", err)
		}
		if err := r.Create(ctx, obj); err != nil {
			return fmt.Errorf("create unpack-related object: %v", err)
		}
	}
	for _, obj := range []client.Object{sa, role, rb} {
		obj := obj
		if err := controllerutil.SetOwnerReference(job, obj, r.Scheme); err != nil {
			return fmt.Errorf("set owner reference on unpack-related rbac object: %v", err)
		}
		if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete unpack-related rbac object: %v", err)
		}
		if err := r.Create(ctx, obj); err != nil {
			return fmt.Errorf("create unpack-related rbac object: %v", err)
		}
	}
	return nil

	//
	//registry.NewBundle()
	//
	//pr, pw := io.Pipe()
	//var group errgroup.Group
	//group.Go(func() error {
	//	if err := crane.Export(img, pw); err != nil {
	//		return fmt.Errorf("write image to tar: %v", err)
	//	}
	//	return nil
	//})
	//tfs, err := tarfs.New(pr)
	//if err != nil {
	//	return nil, fmt.Errorf("read tar into in-memory fs: %v", err)
	//}
	//if err := group.Wait(); err != nil {
	//	return nil, err
	//}
	//rbundle, err := registryfork.NewBundleParser(nil).Parse(tfs)
	//if err != nil {
	//	return nil, fmt.Errorf("parse bundle: %v", err)
	//}
	//
	//version, err := rbundle.Version()
	//if err != nil {
	//	return nil, fmt.Errorf("get bundle version: %v", err)
	//}
	//
	//refs := []corev1.ObjectReference{}
	//for _, u := range rbundle.Objects {
	//	namespace := u.GetNamespace()
	//	if u.GetKind() == "ClusterServiceVersion" {
	//		namespace = bundle.Namespace
	//	}
	//	refs = append(refs, corev1.ObjectReference{
	//		Kind:       u.GetKind(),
	//		Namespace:  namespace,
	//		Name:       u.GetName(),
	//		APIVersion: u.GetAPIVersion(),
	//	})
	//}
	//
	//return &olmv1alpha1.BundleContents{
	//	Package: rbundle.Package,
	//	Name:    rbundle.Name,
	//	Version: version,
	//	Objects: refs,
	//}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BundleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&olmv1alpha1.Bundle{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}
