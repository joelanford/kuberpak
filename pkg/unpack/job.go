package unpack

import (
	"context"
	"fmt"
	"strings"

	"github.com/operator-framework/operator-registry/pkg/configmap"
	"github.com/operator-framework/operator-registry/pkg/registry"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
)

var _ Unpacker = &Job{}

type Job struct {
	bundle olmv1alpha1.Bundle

	client.Client
	Scheme     *runtime.Scheme
	KubeClient kubernetes.Interface
}

func (j Job) GetDigest(ctx context.Context) (string, error) {
	panic("not implemented")
}

func (j Job) Unpack(ctx context.Context) (*registry.Bundle, error) {
	cfgMap := j.configMap(j.bundle)
	job := j.job(j.bundle)
	sa := j.serviceAccount(j.bundle)
	role := j.role(j.bundle)
	roleBinding := j.roleBinding(j.bundle)
	for _, obj := range []client.Object{cfgMap, job} {
		obj := obj
		if err := controllerutil.SetControllerReference(&j.bundle, obj, j.Scheme); err != nil {
			return nil, fmt.Errorf("set controller reference on %s: %v", prettyName(obj), err)
		}
		if err := j.Create(ctx, obj); err != nil {
			return nil, fmt.Errorf("create %s: %v", prettyName(obj), err)
		}
	}
	for _, obj := range []client.Object{sa, role, roleBinding} {
		obj := obj
		if err := controllerutil.SetOwnerReference(job, obj, j.Scheme); err != nil {
			return nil, fmt.Errorf("set owner reference on %s: %v", prettyName(obj), err)
		}
		if err := j.Create(ctx, obj); err != nil {
			return nil, fmt.Errorf("create %s: %v", prettyName(obj), err)
		}
	}

	jobWatch, err := j.KubeClient.BatchV1().Jobs(j.bundle.Namespace).Watch(ctx, metav1.ListOptions{LabelSelector: ""})
	if err != nil {
		return nil, fmt.Errorf("watch %s: %v", prettyName(job), err)
	}
loop:
	for {
		select {
		case evt := <-jobWatch.ResultChan():
			switch evt.Type {
			case watch.Added, watch.Modified:
				job := evt.Object.(*batchv1.Job)
				if job.Status.Failed == 1 {
					// TODO: get job logs
					return nil, fmt.Errorf("unpack job failed")
				}
				if job.Status.Succeeded == 1 {
					break loop
				}
			case watch.Deleted:
				return nil, fmt.Errorf("%s was deleted prior to completion", prettyName(job))
			case watch.Error:
				return nil, fmt.Errorf("%s", evt.Object)
			}
		case <-ctx.Done():
			// TODO: get pod events/conditions
			return nil, ctx.Err()
		}
	}

	if err := j.Get(ctx, client.ObjectKeyFromObject(cfgMap), cfgMap); err != nil {
		return nil, fmt.Errorf("get %s: %v", prettyName(cfgMap), err)
	}

	annotations := registry.Annotations{
		PackageName:        cfgMap.Annotations["operators.operatorframework.io.bundle.package.v1"],
		Channels:           cfgMap.Annotations["operators.operatorframework.io.bundle.channels.v1"],
		DefaultChannelName: cfgMap.Annotations["operators.operatorframework.io.bundle.channel.default.v1"],
	}

	bl := configmap.NewBundleLoader()
	apiBundle, err := bl.Load(cfgMap)
	if err != nil {
		return nil, fmt.Errorf("load %s as bundle: %v", prettyName(cfgMap), err)
	}

	unstObjs := []*unstructured.Unstructured{}
	refs := []corev1.ObjectReference{}
	for _, o := range apiBundle.Object {
		dec := yaml.NewYAMLOrJSONDecoder(strings.NewReader(o), 10)
		unst := &unstructured.Unstructured{}
		if err := dec.Decode(unst); err != nil {
			return nil, fmt.Errorf("decode bundle object: %v", err)
		}
		unstObjs = append(unstObjs, unst)
		refs = append(refs, corev1.ObjectReference{
			APIVersion: unst.GetAPIVersion(),
			Kind:       unst.GetKind(),
			Namespace:  unst.GetNamespace(),
			Name:       unst.GetName(),
		})
	}

	for _, obj := range []client.Object{job, cfgMap} {
		if err := j.Delete(ctx, obj); err != nil {
			return nil, fmt.Errorf("delete %s: %v", prettyName(obj), err)
		}
	}

	rBundle := registry.NewBundle(apiBundle.CsvName, &annotations, unstObjs...)
	return rBundle, nil
}

func prettyName(obj client.Object) string {
	if obj.GetNamespace() == "" {
		return fmt.Sprintf("%s (%s)", obj.GetName(), obj.GetObjectKind().GroupVersionKind())
	}
	return fmt.Sprintf("%s/%s (%s)", obj.GetNamespace(), obj.GetName(), obj.GetObjectKind().GroupVersionKind())
}

func (j Job) configMap(bundle olmv1alpha1.Bundle) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: batchv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace,
			Name:      bundle.Name,
		},
	}
}

func (j Job) serviceAccount(bundle olmv1alpha1.Bundle) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ServiceAccount",
			APIVersion: corev1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace,
			Name:      bundle.Name,
		},
		ImagePullSecrets: bundle.Spec.ImagePullSecrets,
	}
}

func (j Job) role(bundle olmv1alpha1.Bundle) *rbacv1.Role {
	return &rbacv1.Role{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Role",
			APIVersion: rbacv1.SchemeGroupVersion.String(),
		},
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
}
func (j Job) roleBinding(bundle olmv1alpha1.Bundle) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{
			Kind:       "RoleBinding",
			APIVersion: rbacv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace,
			Name:      bundle.Name,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      bundle.Name,
				Namespace: bundle.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     bundle.Name,
		},
	}
}

func (j Job) job(bundle olmv1alpha1.Bundle) *batchv1.Job {
	ttlSecondsAfterFinished := int32(0)
	return &batchv1.Job{
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
					ServiceAccountName: bundle.Name,
				},
			},
		},
	}
}
