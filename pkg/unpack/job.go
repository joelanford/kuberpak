package unpack

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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

type JobProvider struct {
	Client      client.Client
	KubeClient  kubernetes.Interface
	Namespace   string
	UnpackImage string
}

func (p *JobProvider) NewJob(bundle *olmv1alpha1.Bundle) Unpacker {
	return &jobUnpacker{
		bundle:      bundle,
		Client:      p.Client,
		KubeClient:  p.KubeClient,
		Namespace:   p.Namespace,
		UnpackImage: p.UnpackImage,
	}
}

type jobUnpacker struct {
	bundle *olmv1alpha1.Bundle

	client.Client
	KubeClient  kubernetes.Interface
	Namespace   string
	UnpackImage string
}

func (j jobUnpacker) Unpack(ctx context.Context) error {
	saAc := j.serviceAccount(j.bundle)
	sa, err := j.KubeClient.CoreV1().ServiceAccounts(j.Namespace).Apply(ctx, saAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	if err != nil {
		return err
	}

	roleAc := j.role(j.bundle)
	role, err := j.KubeClient.RbacV1().Roles(j.Namespace).Apply(ctx, roleAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	if err != nil {
		return err
	}

	roleBindingAc := j.roleBinding(j.bundle)
	roleBinding, err := j.KubeClient.RbacV1().RoleBindings(j.Namespace).Apply(ctx, roleBindingAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	if err != nil {
		return err
	}

	clusterRoleAc := j.clusterRole(j.bundle)
	clusterRole, err := j.KubeClient.RbacV1().ClusterRoles().Apply(ctx, clusterRoleAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	if err != nil {
		return err
	}

	clusterRoleBindingAc := j.clusterRoleBinding(j.bundle)
	clusterRoleBinding, err := j.KubeClient.RbacV1().ClusterRoleBindings().Apply(ctx, clusterRoleBindingAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	if err != nil {
		return err
	}

	job := j.job(j.bundle)
	if err := j.Create(ctx, job); err != nil {
		return err
	}
	job.SetGroupVersionKind(batchv1.SchemeGroupVersion.WithKind("Job"))
	//jobAc := j.job(j.bundle)
	//job, err := j.KubeClient.BatchV1().Jobs(j.Namespace).Apply(ctx, jobAc, metav1.ApplyOptions{FieldManager: "kuberpak", Force: true})
	//if err != nil {
	//	return err
	//}
	//job.SetGroupVersionKind(batchv1.SchemeGroupVersion.WithKind("Job"))

	_, _, _, _, _ = sa, role, roleBinding, clusterRole, clusterRoleBinding

	err = j.waitForJobCompletion(ctx, job)

	var g errgroup.Group
	watchStarted := make(chan struct{})
	g.Go(func() error { return j.waitForJobDeletion(ctx, job, watchStarted) })
	<-watchStarted
	if deleteErr := j.KubeClient.BatchV1().Jobs(j.Namespace).Delete(ctx, job.GetName(), metav1.DeleteOptions{}); client.IgnoreNotFound(err) != nil {
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

func (j jobUnpacker) serviceAccount(bundle *olmv1alpha1.Bundle) *corev1ac.ServiceAccountApplyConfiguration {
	//return &corev1.ServiceAccount{
	//	ObjectMeta:       metav1.ObjectMeta{Name: bundle.Name, Namespace: j.Namespace, Labels: getLabels(bundle), OwnerReferences: []metav1.OwnerReference{getOwnerReferenceApplyConfiguration()}},
	//	ImagePullSecrets: bundle.Spec.ImagePullSecrets,
	//}
	var pullSecrets []*corev1ac.LocalObjectReferenceApplyConfiguration
	for _, ps := range bundle.Spec.ImagePullSecrets {
		pullSecrets = append(pullSecrets, corev1ac.LocalObjectReference().WithName(ps.Name))
	}
	return corev1ac.ServiceAccount(bundle.Name, j.Namespace).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithImagePullSecrets(pullSecrets...)
}

func (j jobUnpacker) clusterRole(bundle *olmv1alpha1.Bundle) *rbacv1ac.ClusterRoleApplyConfiguration {
	//return &rbacv1.ClusterRole{
	//	ObjectMeta: metav1.ObjectMeta{Name: bundle.Name, Labels: getLabels(bundle)},
	//	Rules: []rbacv1.PolicyRule{
	//		{Verbs: []string{"get"}, Resources: []string{"bundles"}, APIGroups: []string{olmv1alpha1.GroupVersion.Group}, ResourceNames: []string{bundle.Name}},
	//	},
	//}
	return rbacv1ac.ClusterRole(bundle.Name).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithRules(
			rbacv1ac.PolicyRule().WithVerbs("get").WithResources("bundles").WithAPIGroups(olmv1alpha1.GroupVersion.Group).WithResourceNames(bundle.Name),
		)
}

func (j jobUnpacker) clusterRoleBinding(bundle *olmv1alpha1.Bundle) *rbacv1ac.ClusterRoleBindingApplyConfiguration {
	//return &rbacv1.ClusterRoleBinding{
	//	ObjectMeta: metav1.ObjectMeta{Name: bundle.Name, Labels: getLabels(bundle)},
	//	Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: bundle.Name, Namespace: j.Namespace}},
	//	RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: bundle.Name},
	//}
	return rbacv1ac.ClusterRoleBinding(bundle.Name).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithSubjects(rbacv1ac.Subject().WithKind(rbacv1.ServiceAccountKind).WithName(bundle.Name).WithNamespace(j.Namespace)).
		WithRoleRef(rbacv1ac.RoleRef().WithAPIGroup(rbacv1.GroupName).WithKind("ClusterRole").WithName(bundle.Name))
}

func (j jobUnpacker) role(bundle *olmv1alpha1.Bundle) *rbacv1ac.RoleApplyConfiguration {
	//return &rbacv1.Role{
	//	ObjectMeta: metav1.ObjectMeta{Name: bundle.Name, Namespace: j.Namespace, Labels: getLabels(bundle)},
	//	Rules: []rbacv1.PolicyRule{
	//		{Verbs: []string{"list", "create", "delete"}, Resources: []string{"configmaps"}, APIGroups: []string{corev1.GroupName}},
	//		{Verbs: []string{"get"}, Resources: []string{"pods"}, APIGroups: []string{corev1.GroupName}},
	//	},
	//}
	return rbacv1ac.Role(bundle.Name, j.Namespace).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithRules(
			rbacv1ac.PolicyRule().WithVerbs("list", "create", "delete").WithResources("configmaps").WithAPIGroups(corev1.GroupName),
			rbacv1ac.PolicyRule().WithVerbs("get").WithResources("pods").WithAPIGroups(corev1.GroupName),
		)
}
func (j jobUnpacker) roleBinding(bundle *olmv1alpha1.Bundle) *rbacv1ac.RoleBindingApplyConfiguration {
	//return &rbacv1.RoleBinding{
	//	ObjectMeta: metav1.ObjectMeta{Name: bundle.Name, Namespace: j.Namespace, Labels: getLabels(bundle)},
	//	Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: bundle.Name, Namespace: j.Namespace}},
	//	RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: bundle.Name},
	//}
	return rbacv1ac.RoleBinding(bundle.Name, j.Namespace).
		WithLabels(getLabels(bundle)).
		WithOwnerReferences(getOwnerReferenceApplyConfiguration(bundle)).
		WithSubjects(rbacv1ac.Subject().WithKind(rbacv1.ServiceAccountKind).WithName(bundle.Name).WithNamespace(j.Namespace)).
		WithRoleRef(rbacv1ac.RoleRef().WithAPIGroup(rbacv1.GroupName).WithKind("Role").WithName(bundle.Name))
}

func (j jobUnpacker) job(bundle *olmv1alpha1.Bundle) *batchv1.Job {
	completions := int32(1)
	activeDeadlineSeconds := int64(300)
	ttlSecondsAfterCompletion := int32(10)
	//suspend := true
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            bundle.Name,
			Namespace:       j.Namespace,
			Labels:          getLabels(bundle),
			OwnerReferences: []metav1.OwnerReference{getOwnerRef(bundle)},
		},
		Spec: batchv1.JobSpec{
			Completions:             &completions,
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			BackoffLimit:            nil,
			TTLSecondsAfterFinished: &ttlSecondsAfterCompletion,
			//Suspend: &suspend,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: getLabels(bundle),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: bundle.Name,
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
							Image:           j.UnpackImage,
							ImagePullPolicy: corev1.PullAlways,
							Args: []string{
								fmt.Sprintf("--bundle-name=%s", j.bundle.Name),
								"--pod-name=$(POD_NAME)",
								"--namespace=$(POD_NAMESPACE)",
								"--manifests-dir=/bundle/manifests",
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
			},
		},
	}
	//return batchv1ac.Job(bundle.Name, bundle.Namespace).
	//	WithGenerateName(bundle.Name).
	//	WithOwnerReferences(getOwnerRefApplyConfiguration(&bundle)).
	//	WithLabels(getLabels(bundle)).
	//	WithSpec(batchv1ac.JobSpec().
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
	//							fmt.Sprintf("--bundle-name=%s", j.bundle.Name),
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

func (j *jobUnpacker) waitForJobCompletion(ctx context.Context, job *batchv1.Job) error {
	fs := fields.SelectorFromSet(map[string]string{
		"metadata.name":      j.bundle.Name,
		"metadata.namespace": j.Namespace,
	}).String()
	jobWatch, err := j.KubeClient.BatchV1().Jobs(j.Namespace).Watch(ctx, metav1.ListOptions{Watch: true, FieldSelector: fs})
	if err != nil {
		return fmt.Errorf("watch %s: %v", prettyName(job), err)
	}
	defer jobWatch.Stop()
loop:
	for {
		select {
		case evt := <-jobWatch.ResultChan():
			switch evt.Type {
			case watch.Added, watch.Modified:
				job := evt.Object.(*batchv1.Job)
				if job.Status.Failed == 1 {
					// TODO: get job logs
					return fmt.Errorf("unpack job failed")
				}
				if job.Status.Succeeded == 1 {
					break loop
				}
			case watch.Deleted:
				return fmt.Errorf("%s was deleted prior to completion", prettyName(job))
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

func (j *jobUnpacker) waitForJobDeletion(ctx context.Context, job *batchv1.Job, watchStarted chan<- struct{}) error {
	fs := fields.SelectorFromSet(map[string]string{
		"metadata.name":      j.bundle.Name,
		"metadata.namespace": j.Namespace,
	}).String()
	jobWatch, err := j.KubeClient.BatchV1().Jobs(j.Namespace).Watch(ctx, metav1.ListOptions{Watch: true, FieldSelector: fs})
	if err != nil {
		return fmt.Errorf("watch %s: %v", prettyName(job), err)
	}
	close(watchStarted)
	defer jobWatch.Stop()
	for {
		select {
		case evt := <-jobWatch.ResultChan():
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
