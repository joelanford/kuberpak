package unpack

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
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

func (p podUnpacker) Unpack(ctx context.Context) error {
	//saAc := p.serviceAccount(p.bundle)
	//sa, err := p.KubeClient.CoreV1().ServiceAccounts(p.Namespace).Apply(ctx, saAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	//if err != nil {
	//	return err
	//}
	//
	//roleAc := p.role(p.bundle)
	//role, err := p.KubeClient.RbacV1().Roles(p.Namespace).Apply(ctx, roleAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	//if err != nil {
	//	return err
	//}
	//
	//roleBindingAc := p.roleBinding(p.bundle)
	//roleBinding, err := p.KubeClient.RbacV1().RoleBindings(p.Namespace).Apply(ctx, roleBindingAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	//if err != nil {
	//	return err
	//}
	//
	//clusterRoleAc := p.clusterRole(p.bundle)
	//clusterRole, err := p.KubeClient.RbacV1().ClusterRoles().Apply(ctx, clusterRoleAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	//if err != nil {
	//	return err
	//}
	//
	//clusterRoleBindingAc := p.clusterRoleBinding(p.bundle)
	//clusterRoleBinding, err := p.KubeClient.RbacV1().ClusterRoleBindings().Apply(ctx, clusterRoleBindingAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	//if err != nil {
	//	return err
	//}
	//_, _, _, _, _ = sa, role, roleBinding, clusterRole, clusterRoleBinding

	pod := p.pod(p.bundle)
	existingPod := &corev1.Pod{}
	if err := p.Get(ctx, client.ObjectKeyFromObject(pod), existingPod); err == nil {
		if deleteErr := p.ensurePodDeletion(ctx, pod); deleteErr != nil {
			return deleteErr
		}
	}

	if err := p.Create(ctx, pod); err != nil {
		return err
	}
	pod.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))

	err := p.waitForPodCompletion(ctx, pod)
	if deleteErr := p.ensurePodDeletion(ctx, pod); deleteErr != nil {
		return deleteErr
	}
	return err
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

func prettyName(obj client.Object) string {
	if obj.GetNamespace() == "" {
		return fmt.Sprintf("%s (%s)", obj.GetName(), obj.GetObjectKind().GroupVersionKind())
	}
	return fmt.Sprintf("%s/%s (%s)", obj.GetNamespace(), obj.GetName(), obj.GetObjectKind().GroupVersionKind())
}

func getLabels(bundle *olmv1alpha1.Bundle) map[string]string {
	return map[string]string{
		"kuberpak.io/bundle-name": bundle.Name,
	}
}

func getOwnerRef(obj client.Object) metav1.OwnerReference {
	apiVersion, kind := obj.GetObjectKind().GroupVersionKind().ToAPIVersionAndKind()
	controller := true
	blockOwnerDeletion := true
	return metav1.OwnerReference{
		APIVersion:         apiVersion,
		Kind:               kind,
		Name:               obj.GetName(),
		UID:                obj.GetUID(),
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}
}

func getOwnerReferenceApplyConfiguration(obj client.Object) *metav1ac.OwnerReferenceApplyConfiguration {
	apiVersion, kind := obj.GetObjectKind().GroupVersionKind().ToAPIVersionAndKind()
	return metav1ac.OwnerReference().
		WithAPIVersion(apiVersion).
		WithKind(kind).
		WithName(obj.GetName()).
		WithUID(obj.GetUID()).
		WithController(true).
		WithBlockOwnerDeletion(true)
}

func deriveName(b *olmv1alpha1.Bundle) string {
	return fmt.Sprintf("kuberpak-unpack-bundle-%s", b.Name)
}

func (p podUnpacker) serviceAccount(bundle *olmv1alpha1.Bundle) *corev1ac.ServiceAccountApplyConfiguration {
	//return &corev1.ServiceAccount{
	//	ObjectMeta:       metav1.ObjectMeta{Name: bundle.Name, Namespace: p.Namespace, Labels: getLabels(bundle), OwnerReferences: []metav1.OwnerReference{getOwnerReferenceApplyConfiguration()}},
	//	ImagePullSecrets: bundle.Spec.ImagePullSecrets,
	//}
	var pullSecrets []*corev1ac.LocalObjectReferenceApplyConfiguration
	for _, ps := range bundle.Spec.ImagePullSecrets {
		pullSecrets = append(pullSecrets, corev1ac.LocalObjectReference().WithName(ps.Name))
	}
	return corev1ac.ServiceAccount(deriveName(bundle), p.Namespace).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithImagePullSecrets(pullSecrets...)
}

func (p podUnpacker) clusterRole(bundle *olmv1alpha1.Bundle) *rbacv1ac.ClusterRoleApplyConfiguration {
	//return &rbacv1.ClusterRole{
	//	ObjectMeta: metav1.ObjectMeta{Name: bundle.Name, Labels: getLabels(bundle)},
	//	Rules: []rbacv1.PolicyRule{
	//		{Verbs: []string{"get"}, Resources: []string{"bundles"}, APIGroups: []string{olmv1alpha1.GroupVersion.Group}, ResourceNames: []string{bundle.Name}},
	//	},
	//}
	return rbacv1ac.ClusterRole(deriveName(bundle)).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithRules(
			rbacv1ac.PolicyRule().WithVerbs("get").WithResources("bundles").WithAPIGroups(olmv1alpha1.GroupVersion.Group).WithResourceNames(bundle.Name),
		)
}

func (p podUnpacker) clusterRoleBinding(bundle *olmv1alpha1.Bundle) *rbacv1ac.ClusterRoleBindingApplyConfiguration {
	//return &rbacv1.ClusterRoleBinding{
	//	ObjectMeta: metav1.ObjectMeta{Name: bundle.Name, Labels: getLabels(bundle)},
	//	Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: bundle.Name, Namespace: p.Namespace}},
	//	RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: bundle.Name},
	//}
	return rbacv1ac.ClusterRoleBinding(deriveName(bundle)).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithSubjects(rbacv1ac.Subject().WithKind(rbacv1.ServiceAccountKind).WithName(deriveName(bundle)).WithNamespace(p.Namespace)).
		WithRoleRef(rbacv1ac.RoleRef().WithAPIGroup(rbacv1.GroupName).WithKind("ClusterRole").WithName(deriveName(bundle)))
}

func (p podUnpacker) role(bundle *olmv1alpha1.Bundle) *rbacv1ac.RoleApplyConfiguration {
	//return &rbacv1.Role{
	//	ObjectMeta: metav1.ObjectMeta{Name: bundle.Name, Namespace: p.Namespace, Labels: getLabels(bundle)},
	//	Rules: []rbacv1.PolicyRule{
	//		{Verbs: []string{"list", "create", "delete"}, Resources: []string{"configmaps"}, APIGroups: []string{corev1.GroupName}},
	//		{Verbs: []string{"get"}, Resources: []string{"pods"}, APIGroups: []string{corev1.GroupName}},
	//	},
	//}
	return rbacv1ac.Role(deriveName(bundle), p.Namespace).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithRules(
			rbacv1ac.PolicyRule().WithVerbs("list", "create", "delete").WithResources("configmaps").WithAPIGroups(corev1.GroupName),
			rbacv1ac.PolicyRule().WithVerbs("get").WithResources("pods").WithAPIGroups(corev1.GroupName).WithResourceNames(deriveName(bundle)),
		)
}
func (p podUnpacker) roleBinding(bundle *olmv1alpha1.Bundle) *rbacv1ac.RoleBindingApplyConfiguration {
	//return &rbacv1.RoleBinding{
	//	ObjectMeta: metav1.ObjectMeta{Name: bundle.Name, Namespace: p.Namespace, Labels: getLabels(bundle)},
	//	Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: bundle.Name, Namespace: p.Namespace}},
	//	RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: bundle.Name},
	//}
	return rbacv1ac.RoleBinding(deriveName(bundle), p.Namespace).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithSubjects(rbacv1ac.Subject().WithKind(rbacv1.ServiceAccountKind).WithName(deriveName(bundle)).WithNamespace(p.Namespace)).
		WithRoleRef(rbacv1ac.RoleRef().WithAPIGroup(rbacv1.GroupName).WithKind("Role").WithName(deriveName(bundle)))
}

func (p podUnpacker) pod(bundle *olmv1alpha1.Bundle) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            deriveName(bundle),
			Namespace:       p.Namespace,
			Labels:          getLabels(bundle),
			OwnerReferences: []metav1.OwnerReference{getOwnerRef(bundle)},
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
					Name:    "copy-bundle",
					Image:   bundle.Spec.Image,
					Command: []string{"/util/cpb", "/bundle"},
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

	//return corev1ac.Pod(bundle.Name, bundle.Namespace).
	//	WithGenerateName(bundle.Name).
	//	WithOwnerReferences(getOwnerRefApplyConfiguration(&bundle)).
	//	WithLabels(getLabels(bundle)).
	//	WithSpec(corev1ac.PodSpec().
	//		WithTTLSecondsAfterFinished(0).
	//		WithTemplate(corev1ac.PodTemplateSpec().
	//			WithLabels(getLabels(bundle)).
	//			WithSpec(corev1ac.PodSpec().
	//				WithServiceAccountName(bundle.Name).
	//				WithVolumes(
	//					corev1ac.Volume().WithName("util").WithEmptyDir(corev1ac.EmptyDirVolumeSource()),
	//					corev1ac.Volume().WithName("bundle").WithEmptyDir(corev1ac.EmptyDirVolumeSource()),
	//				).
	//				WithRestartPolicy(corev1.RestartPolicyNever).
	//				WithInitContainers(
	//					corev1ac.Container().
	//						WithName("install-cpb").
	//						WithImage("quay.io/operator-framework/olm:latest").
	//						WithCommand("/bin/cp", "-Rv", "/bin/cpb", "/util/cpb").
	//						WithVolumeMounts(
	//							corev1ac.VolumeMount().WithName("util").WithMountPath("/util"),
	//						),
	//					corev1ac.Container().
	//						WithName("copy-bundle").
	//						WithImage(bundle.Spec.Image).
	//						WithCommand("/util/cpb", "/bundle").
	//						WithVolumeMounts(
	//							corev1ac.VolumeMount().WithName("util").WithMountPath("/util"),
	//							corev1ac.VolumeMount().WithName("bundle").WithMountPath("/bundle"),
	//						),
	//				).
	//				WithContainers(
	//					corev1ac.Container().
	//						WithName("unpack-bundle").
	//						WithImage("quay.io/joelanford/unpack-bundle:latest").
	//						WithArgs(
	//							fmt.Sprintf("--bundle-name=%s", p.bundle.Name),
	//							"--pod-name=$(POD_NAME)",
	//							"--namespace=$(POD_NAMESPACE)",
	//							"--manifests-dir=/bundle/manifests",
	//						).
	//						WithEnv(
	//							corev1ac.EnvVar().
	//								WithName("POD_NAME").
	//								WithValueFrom(corev1ac.EnvVarSource().WithFieldRef(
	//									corev1ac.ObjectFieldSelector().WithFieldPath("metadata.name")),
	//								),
	//							corev1ac.EnvVar().
	//								WithName("POD_NAMESPACE").
	//								WithValueFrom(corev1ac.EnvVarSource().WithFieldRef(
	//									corev1ac.ObjectFieldSelector().WithFieldPath("metadata.namespace")),
	//								),
	//						).
	//						WithVolumeMounts(
	//							corev1ac.VolumeMount().WithName("bundle").WithMountPath("/bundle"),
	//						),
	//				),
	//			),
	//		),
	//	)
}

func (p *podUnpacker) waitForPodCompletion(ctx context.Context, pod *corev1.Pod) error {
	fs := fields.SelectorFromSet(map[string]string{
		"metadata.name":      pod.Name,
		"metadata.namespace": pod.Namespace,
	}).String()
	podWatch, err := p.KubeClient.CoreV1().Pods(p.Namespace).Watch(ctx, metav1.ListOptions{Watch: true, FieldSelector: fs})
	if err != nil {
		return fmt.Errorf("watch %s: %v", prettyName(pod), err)
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
				return fmt.Errorf("%s was deleted prior to completion", prettyName(pod))
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
		return fmt.Errorf("watch %s: %v", prettyName(pod), err)
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
