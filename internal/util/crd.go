package util

import (
	"context"
	"fmt"
	"reflect"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func CreateOrUpdateCRD(ctx context.Context, cl client.Client, crd *apiextensionsv1.CustomResourceDefinition) (controllerutil.OperationResult, error) {
	createCRD := crd.DeepCopy()
	createErr := cl.Create(ctx, createCRD)
	if createErr == nil {
		return controllerutil.OperationResultCreated, nil
	}
	if !apierrors.IsAlreadyExists(createErr) {
		return controllerutil.OperationResultNone, createErr
	}
	if updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		currentCRD := &apiextensionsv1.CustomResourceDefinition{}
		if err := cl.Get(ctx, client.ObjectKeyFromObject(crd), currentCRD); err != nil {
			return err
		}
		crd.SetResourceVersion(currentCRD.GetResourceVersion())

		if err := validateCRDCompatibility(ctx, cl, currentCRD, crd); err != nil {
			return fmt.Errorf("error validating existing CRs against new CRD's schema for %q: %w", crd.Name, err)
		}

		// check to see if stored versions changed and whether the upgrade could cause potential data loss
		safe, err := safeStorageVersionUpgrade(currentCRD, crd)
		if !safe {
			return fmt.Errorf("risk of data loss updating %q: %w", crd.Name, err)
		}
		if err != nil {
			return fmt.Errorf("checking CRD for potential data loss updating %q: %w", crd.Name, err)
		}

		// Update CRD to new version
		if err := cl.Update(ctx, crd); err != nil {
			return fmt.Errorf("error updating CRD %q: %w", crd.Name, err)
		}
		return nil
	}); updateErr != nil {
		return controllerutil.OperationResultNone, updateErr
	}
	return controllerutil.OperationResultUpdated, nil
}

func validateCRDCompatibility(ctx context.Context, cl client.Client, oldCRD *apiextensionsv1.CustomResourceDefinition, newCRD *apiextensionsv1.CustomResourceDefinition) error {
	// If validation schema is unchanged, return right away
	newestSchema := newCRD.Spec.Versions[len(newCRD.Spec.Versions)-1].Schema
	for i, oldVersion := range oldCRD.Spec.Versions {
		if !reflect.DeepEqual(oldVersion.Schema, newestSchema) {
			break
		}
		if i == len(oldCRD.Spec.Versions)-1 {
			// we are on the last iteration
			// schema has not changed between versions at this point.
			return nil
		}
	}

	convertedCRD := &apiextensions.CustomResourceDefinition{}
	if err := apiextensionsv1.Convert_v1_CustomResourceDefinition_To_apiextensions_CustomResourceDefinition(newCRD, convertedCRD, nil); err != nil {
		return err
	}
	for _, version := range oldCRD.Spec.Versions {
		if version.Served {
			listGVK := schema.GroupVersionKind{Group: oldCRD.Spec.Group, Version: version.Name, Kind: oldCRD.Spec.Names.ListKind}
			err := validateExistingCRs(ctx, cl, listGVK, convertedCRD)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func validateExistingCRs(ctx context.Context, dynamicClient client.Client, listGVK schema.GroupVersionKind, newCRD *apiextensions.CustomResourceDefinition) error {
	crList := &unstructured.UnstructuredList{}
	crList.SetGroupVersionKind(listGVK)
	if err := dynamicClient.List(ctx, crList); err != nil {
		return fmt.Errorf("error listing objects for GroupVersionKind %#v: %s", listGVK, err)
	}
	for _, cr := range crList.Items {
		validator, _, err := validation.NewSchemaValidator(newCRD.Spec.Validation)
		if err != nil {
			return fmt.Errorf("error creating validator for schema %#v: %s", newCRD.Spec.Validation, err)
		}
		err = validation.ValidateCustomResource(field.NewPath(""), cr.UnstructuredContent(), validator).ToAggregate()
		if err != nil {
			return fmt.Errorf("error validating custom resource against new schema for %s %s/%s: %v", newCRD.Spec.Names.Kind, cr.GetNamespace(), cr.GetName(), err)
		}
	}
	return nil
}

// safeStorageVersionUpgrade determines whether the new CRD spec includes all the storage versions of the existing on-cluster CRD.
// For each stored version in the status of the CRD on the cluster (there will be at least one) - each version must exist in the spec of the new CRD that is being installed.
// See https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definition-versioning/#upgrade-existing-objects-to-a-new-stored-version.
func safeStorageVersionUpgrade(existingCRD, newCRD *apiextensionsv1.CustomResourceDefinition) (bool, error) {
	existingStatusVersions, newSpecVersions := getStoredVersions(existingCRD, newCRD)
	if newSpecVersions == nil {
		return false, fmt.Errorf("could not find any versions in the new CRD")
	}
	if existingStatusVersions == nil {
		// every on-cluster CRD should have at least one stored version in its status
		// in the case where there are no existing stored versions, checking against new versions is not relevant
		return true, nil
	}

	for name := range existingStatusVersions {
		if _, ok := newSpecVersions[name]; !ok {
			// a storage version in the status of the old CRD is not present in the spec of the new CRD
			// potential data loss of CRs without a storage migration - throw error and block the CRD upgrade
			return false, fmt.Errorf("new CRD removes version %s that is listed as a stored version on the existing CRD", name)
		}
	}

	return true, nil
}

// getStoredVersions returns the storage versions listed in the status of the old on-cluster CRD
// and all the versions listed in the spec of the new CRD.
func getStoredVersions(oldCRD, newCRD *apiextensionsv1.CustomResourceDefinition) (sets.String, sets.String) {
	newSpecVersions := sets.NewString()
	for _, version := range newCRD.Spec.Versions {
		newSpecVersions.Insert(version.Name)
	}

	return sets.NewString(oldCRD.Status.StoredVersions...), newSpecVersions
}
