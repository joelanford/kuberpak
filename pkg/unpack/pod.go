package unpack

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	olmv1alpha1 "github.com/joelanford/kuberpak/api/v1alpha1"
)

type PodProvider struct {
	Client      client.Client
	KubeClient  kubernetes.Interface
	Namespace   string
	UnpackImage string
}

func (p *PodProvider) NewPod(bundle *olmv1alpha1.Bundle) Unpacker {
	return &podUnpacker{
		bundle:      bundle,
		Client:      p.Client,
		KubeClient:  p.KubeClient,
		Namespace:   p.Namespace,
		UnpackImage: p.UnpackImage,
	}
}

type podUnpacker struct {
	bundle *olmv1alpha1.Bundle

	client.Client
	KubeClient  kubernetes.Interface
	Namespace   string
	UnpackImage string
}

func (p podUnpacker) Unpack(ctx context.Context) (*olmv1alpha1.BundleStatus, error) {
	pod := p.pod(p.bundle)
	existingPod := &corev1.Pod{}
	if err := p.Get(ctx, client.ObjectKeyFromObject(pod), existingPod); err == nil {
		if deleteErr := p.ensurePodDeletion(ctx, pod); deleteErr != nil {
			return nil, deleteErr
		}
	}

	if err := p.Create(ctx, pod); err != nil {
		return nil, err
	}
	pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))

	err := p.waitForPodCompletion(ctx, pod)
	if deleteErr := p.ensurePodDeletion(ctx, pod); deleteErr != nil {
		return nil, deleteErr
	}
	if err != nil {
		return nil, err
	}

	cm := &corev1.ConfigMap{}
	cmKey := types.NamespacedName{Namespace: p.Namespace, Name: fmt.Sprintf("bundle-metadata-%s", p.bundle.Name)}
	if err := p.Get(ctx, cmKey, cm); err != nil {
		return nil, fmt.Errorf("get bundle metadata configmap: %v", err)
	}

	bundleStatus := &olmv1alpha1.BundleStatus{
		Info: &olmv1alpha1.BundleInfo{
			Package: cm.Data["package-name"],
			Name:    cm.Data["bundle-name"],
			Version: cm.Data["bundle-version"],
		},
		Digest: cm.Data["bundle-image"],
	}

	cmNames := []string{}
	if err := json.Unmarshal([]byte(cm.Data["objects"]), &cmNames); err != nil {
		return nil, fmt.Errorf("unmarshal object refs from metadata configmap: %v", err)
	}

	sort.Strings(cmNames)
	for _, cmName := range cmNames {
		cm := &corev1.ConfigMap{}
		cmKey := types.NamespacedName{Namespace: p.Namespace, Name: cmName}
		if err := p.Get(ctx, cmKey, cm); err != nil {
			return nil, fmt.Errorf("get object configmap: %v", err)
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

func (p *podUnpacker) ensurePodDeletion(ctx context.Context, pod *corev1.Pod) error {
	var g errgroup.Group
	watchStarted := make(chan struct{})
	g.Go(func() error { return p.waitForPodDeletion(ctx, pod, watchStarted) })
	<-watchStarted
	if deleteErr := p.KubeClient.CoreV1().Pods(p.Namespace).Delete(ctx, pod.GetName(), metav1.DeleteOptions{}); client.IgnoreNotFound(deleteErr) != nil {
		return deleteErr
	}
	return g.Wait()
}

func getLabels(bundle *olmv1alpha1.Bundle) map[string]string {
	return map[string]string{
		"kuberpak.io/bundle-name": bundle.Name,
	}
}

func deriveName(b *olmv1alpha1.Bundle) string {
	return fmt.Sprintf("kuberpak-unpack-bundle-%s", b.Name)
}

func (p podUnpacker) pod(bundle *olmv1alpha1.Bundle) *corev1.Pod {
	controllerRef := metav1.NewControllerRef(bundle, bundle.GroupVersionKind())
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            deriveName(bundle),
			Namespace:       p.Namespace,
			Labels:          getLabels(bundle),
			OwnerReferences: []metav1.OwnerReference{*controllerRef},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "kuberpak-bundle-unpacker",
			Volumes: []corev1.Volume{
				{Name: "util", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "bundle", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
			RestartPolicy: corev1.RestartPolicyNever,
			InitContainers: []corev1.Container{
				{
					Name:    "install-cpb",
					Image:   "quay.io/operator-framework/olm:latest",
					Command: []string{"/bin/cp", "-Rv", "/bin/cpb", "/util/cpb"},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "util", MountPath: "/util"},
					},
				},
				{
					Name:            "copy-bundle",
					Image:           bundle.Spec.Image,
					ImagePullPolicy: corev1.PullAlways,
					Command:         []string{"/util/cpb", "/bundle"},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "util", MountPath: "/util"},
						{Name: "bundle", MountPath: "/bundle"},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:            "unpack-bundle",
					Image:           p.UnpackImage,
					ImagePullPolicy: corev1.PullAlways,
					Args: []string{
						fmt.Sprintf("--bundle-name=%s", p.bundle.Name),
						"--pod-name=$(POD_NAME)",
						"--namespace=$(POD_NAMESPACE)",
						"--bundle-dir=/bundle",
					},
					Env: []corev1.EnvVar{
						{
							Name: "POD_NAME",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "metadata.name",
								},
							},
						},
						{
							Name: "POD_NAMESPACE",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "metadata.namespace",
								},
							},
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "bundle",
							MountPath: "/bundle",
						},
					},
				},
			},
		},
	}
}

func (p *podUnpacker) waitForPodCompletion(ctx context.Context, pod *corev1.Pod) error {
	fs := fields.SelectorFromSet(map[string]string{
		"metadata.name":      pod.Name,
		"metadata.namespace": pod.Namespace,
	}).String()
	podWatch, err := p.KubeClient.CoreV1().Pods(p.Namespace).Watch(ctx, metav1.ListOptions{Watch: true, FieldSelector: fs})
	if err != nil {
		return fmt.Errorf("watch pod %q: %v", pod.Name, err)
	}
	defer podWatch.Stop()
loop:
	for {
		select {
		case evt := <-podWatch.ResultChan():
			switch evt.Type {
			case watch.Added, watch.Modified:
				pod := evt.Object.(*corev1.Pod)
				switch pod.Status.Phase {
				case corev1.PodSucceeded:
					break loop
				case corev1.PodFailed:
					// TODO: get pod logs
					return fmt.Errorf("unpack pod failed")
				}
			case watch.Deleted:
				return fmt.Errorf("pod %q was deleted prior to completion", pod.Name)
			case watch.Error:
				return fmt.Errorf("%s", evt.Object)
			}
		case <-ctx.Done():
			// TODO: get pod events/conditions
			return ctx.Err()
		}
	}
	return nil
}

func (p *podUnpacker) waitForPodDeletion(ctx context.Context, pod *corev1.Pod, watchStarted chan<- struct{}) error {
	fs := fields.SelectorFromSet(map[string]string{
		"metadata.name":      pod.Name,
		"metadata.namespace": pod.Namespace,
	}).String()
	podWatch, err := p.KubeClient.CoreV1().Pods(p.Namespace).Watch(ctx, metav1.ListOptions{Watch: true, FieldSelector: fs})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("watch pod %q: %v", pod.Name, err)
	}
	close(watchStarted)
	defer podWatch.Stop()
	for {
		select {
		case evt := <-podWatch.ResultChan():
			switch evt.Type {
			case watch.Deleted:
				return nil
			case watch.Error:
				return fmt.Errorf("%s", evt.Object)
			}
		case <-ctx.Done():
			// TODO: get pod events/conditions
			return ctx.Err()
		}
	}
}
