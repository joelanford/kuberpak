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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BundleSpec defines the desired state of Bundle
type BundleSpec struct {
	// Image is the bunlde image that backs the content of this bundle.
	Image string `json:"image,omitempty"`

	// ImagePullSecrets is a list of pull secrets to have available to
	// pull the referenced image.
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecret,omitempty"`

	// ServiceAccount is the name of a service account to use for image pulling.
	ServiceAccount string `json:"serviceAccount,omitempty"`
}

// BundleStatus defines the observed state of Bundle
type BundleStatus struct {
	Contents *BundleContents `json:"contents,omitempty"`

	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

type BundleConditionType string

const (
	TypeFailedUnpack = "FailedUnpack"

	ReasonUnpackSuccessful = "UnpackSuccessful"
	ReasonImagePullFailure = "ImagePullFailure"
	ReasonGetDigestFailure = "GetDigestFailure"
	ReasonInvalidBundle    = "InvalidBundle"
)

type BundleContents struct {
	Package string                   `json:"package,omitempty"`
	Name    string                   `json:"name,omitempty"`
	Version string                   `json:"version,omitempty"`
	Digest  string                   `json:"imageDigest,omitempty"`
	Objects []corev1.ObjectReference `json:"objectRefs,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Bundle is the Schema for the bundles API
type Bundle struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BundleSpec   `json:"spec,omitempty"`
	Status BundleStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// BundleList contains a list of Bundle
type BundleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Bundle `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Bundle{}, &BundleList{})
}
