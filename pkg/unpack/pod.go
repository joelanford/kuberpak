package unpack

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/nlepage/go-tarfs"
	"github.com/operator-framework/operator-registry/pkg/registry"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
	registryfork "github.com/joelanford/kuberpak/internal/operator-registry-fork/pkg/registry"
)

type Pod struct {
	client.Client
	Scheme     *runtime.Scheme
	KubeClient kubernetes.Interface

	bundle *olmv1alpha1.Bundle

	m       sync.RWMutex
	rBundle *registry.Bundle
	digest  string
}

type ErrPodFailed struct {
	Conditions []corev1.PodCondition
	Logs       map[string]string
}

func (err ErrPodFailed) Error() string {
	return fmt.Sprintf("pod failed with conditions: %#v, logs: %#v", err.Conditions, err.Logs)
}

func NewPodFunc(mgr ctrl.Manager) (NewUnpackerFunc, error) {
	k8s, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return nil, err
	}
	return func(bundle *olmv1alpha1.Bundle) Unpacker {
		return &Pod{
			bundle:     bundle,
			Client:     mgr.GetClient(),
			Scheme:     mgr.GetScheme(),
			KubeClient: k8s,
		}
	}, nil
}

func (p *Pod) GetDigest(ctx context.Context) (string, error) {
	p.m.RLock()
	synced := p.digest != ""
	digest := p.digest
	p.m.RUnlock()

	if synced {
		return digest, nil
	}
	p.m.Lock()
	defer p.m.Unlock()
	var err error
	p.rBundle, p.digest, err = p.unpack(ctx)
	return p.digest, err
}

func (p *Pod) Unpack(ctx context.Context) (*registry.Bundle, error) {
	p.m.RLock()
	synced := p.rBundle != nil
	rBundle := *p.rBundle
	p.m.RUnlock()

	if synced {
		return &rBundle, nil
	}
	p.m.Lock()
	defer p.m.Unlock()
	var err error
	p.rBundle, p.digest, err = p.unpack(ctx)
	return p.rBundle, err
}

func (p *Pod) unpack(ctx context.Context) (*registry.Bundle, string, error) {
	l := log.FromContext(ctx)
	pod := p.pod(p.bundle)
	sa := p.serviceAccount(p.bundle)
	role := p.role(p.bundle)
	roleBinding := p.roleBinding(p.bundle)

	defer func() {
		for _, obj := range []client.Object{pod, sa, role, roleBinding} {
			if err := p.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
				l.Error(err, "delete object")
				return
			}
			if err := wait.PollImmediateUntilWithContext(ctx, time.Millisecond*50, func(ctx context.Context) (bool, error) {
				err := p.Get(ctx, client.ObjectKeyFromObject(obj), obj)
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				if err != nil {
					return false, err
				}
				return false, nil
			}); err != nil {
				l.Error(err, "wait for deletion of temp objects")
				return
			}
		}
	}()

	for _, obj := range []client.Object{sa, role, roleBinding} {
		obj := obj
		if err := p.Create(ctx, obj); err != nil {
			return nil, "", fmt.Errorf("create %s: %v", prettyName(obj), err)
		}
	}
	if err := controllerutil.SetControllerReference(p.bundle, pod, p.Scheme); err != nil {
		return nil, "", fmt.Errorf("set controller reference on %s: %v", prettyName(pod), err)
	}
	if err := p.Create(ctx, pod); err != nil {
		return nil, "", fmt.Errorf("create %s: %v", prettyName(pod), err)
	}

	for _, obj := range []client.Object{sa, role, roleBinding} {
		obj := obj
		if err := controllerutil.SetControllerReference(pod, obj, p.Scheme); err != nil {
			return nil, "", fmt.Errorf("set controller reference on %s: %v", prettyName(obj), err)
		}
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := p.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
				return err
			}
			return p.Update(ctx, obj)
		}); err != nil {
			return nil, "", fmt.Errorf("update %s: %v", prettyName(obj), err)
		}
	}

	if err := wait.PollImmediateUntilWithContext(ctx, time.Millisecond*50, func(ctx context.Context) (bool, error) {
		if err := p.Get(ctx, client.ObjectKeyFromObject(p.bundle), pod); err != nil {
			return false, err
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return true, nil
		case corev1.PodFailed:
			logs, err := p.getLogs(ctx)
			if err != nil {
				return false, fmt.Errorf("get pod logs: %v", err)
			}
			return false, ErrPodFailed{Logs: logs, Conditions: pod.Status.Conditions}
		default:
			return false, nil
		}
	}); err != nil {
		return nil, "", fmt.Errorf("waiting for pod to complete: %v", err)
	}

	cfgMap := &corev1.ConfigMap{}
	if err := p.Get(ctx, client.ObjectKeyFromObject(p.bundle), cfgMap); err != nil {
		return nil, "", fmt.Errorf("get configmap %s: %v", prettyName(cfgMap), err)
	}

	tgz, ok := cfgMap.BinaryData["bundle"]
	if !ok {
		return nil, "", fmt.Errorf("could not find bundle binaryData in config map")
	}
	gzipReader, err := gzip.NewReader(bytes.NewBuffer(tgz))
	if err != nil {
		return nil, "", fmt.Errorf("create gzip reader for bundle.tar.gz: %v", err)
	}
	tfs, err := tarfs.New(gzipReader)
	if err != nil {
		return nil, "", fmt.Errorf("create tar fs from bundle.tar.gz: %v", err)
	}

	bp := registryfork.NewBundleParser(nil)
	rBundle, err := bp.Parse(tfs)
	if err != nil {
		return nil, "", fmt.Errorf("parse bundle: %v", err)
	}
	digest := strings.SplitN(pod.Status.ContainerStatuses[0].ImageID, "@", 2)[1]
	return rBundle, digest, nil
}

func (p *Pod) configMap(bundle *olmv1alpha1.Bundle) *corev1.ConfigMap {
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

func (p *Pod) serviceAccount(bundle *olmv1alpha1.Bundle) *corev1.ServiceAccount {
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

func (p *Pod) role(bundle *olmv1alpha1.Bundle) *rbacv1.Role {
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
				Verbs:         []string{"create"},
				Resources:     []string{"configmaps"},
				APIGroups:     []string{corev1.GroupName},
				ResourceNames: []string{bundle.Name},
			},
		},
	}
}
func (p *Pod) roleBinding(bundle *olmv1alpha1.Bundle) *rbacv1.RoleBinding {
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
func (p *Pod) getLogs(ctx context.Context) (map[string]string, error) {
	logs := map[string]string{}
	for _, container := range []string{"copy-kubectl", "copy-busybox", "bundle-extract"} {
		stream, err := p.KubeClient.CoreV1().Pods(p.bundle.Namespace).GetLogs(p.bundle.Name, &corev1.PodLogOptions{Container: container}).Stream(ctx)
		if err != nil {
			return nil, fmt.Errorf("get logs from container %q: %v", container, err)
		}
		defer stream.Close()
		buf := &bytes.Buffer{}
		if _, err := io.Copy(buf, stream); err != nil {
			return nil, fmt.Errorf("read logs from container %q: %v", container, err)
		}
		logs[container] = buf.String()
	}
	return logs, nil
}

func (p *Pod) pod(bundle *olmv1alpha1.Bundle) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace,
			Name:      bundle.Name,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: "Never",
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
					Name:    "copy-kubectl",
					Image:   "bitnami/kubectl:latest",
					Command: []string{"/bin/cp", "-Rv", "/opt/bitnami/kubectl/bin/kubectl", "/util/kubectl"},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "util",
							MountPath: "/util",
						},
					},
				},
				{
					Name:    "copy-busybox",
					Image:   "busybox:stable-musl",
					Command: []string{"/bin/sh", "-c", "/bin/cp -Rv /bin/* /util/"},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "util",
							MountPath: "/util",
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "bundle-extract",
					Image: p.bundle.Spec.Image,
					Env: []corev1.EnvVar{
						{Name: "PATH", Value: "/util"},
					},
					Command: []string{"sh", "-c",
						fmt.Sprintf("find / -xdev && tar zcf /util/bundle.tar.gz /manifests && kubectl create configmap %s --namespace %s --from-file=bundle=/util/bundle.tar.gz", p.bundle.Name, p.bundle.Namespace)},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "util",
							MountPath: "/util",
						},
					},
				},
			},
			ServiceAccountName: bundle.Name,
		},
	}
}
