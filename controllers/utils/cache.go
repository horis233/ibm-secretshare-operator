//
// Copyright 2020 IBM Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package utils

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	cr_cache "sigs.k8s.io/controller-runtime/pkg/cache"
	cr_client "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// ErrUnsupported is returned for unsupported operations
var ErrUnsupported = errors.New("unsupported operation")

// ErrInternalError is returned for unexpected errors
var ErrInternalError = errors.New("internal error")

var secretSchema = corev1.SchemeGroupVersion.WithKind("Secret")
var secretListSchema = corev1.SchemeGroupVersion.WithKind("SecretList")
var cmSchema = corev1.SchemeGroupVersion.WithKind("ConfigMap")
var cmListSchema = corev1.SchemeGroupVersion.WithKind("ConfigMapList")

func NewCacheBuilder(namespace string, label string, globalGvks ...schema.GroupVersionKind) cr_cache.NewCacheFunc {
	return func(config *rest.Config, opts cr_cache.Options) (cr_cache.Cache, error) {
		// Setup filtered secret informer that will only store/return items matching the filter for listing purposes
		dynamicClient, err := dynamic.NewForConfig(config)

		clientSet, err := kubernetes.NewForConfig(config)
		if err != nil {
			klog.Error(err, "Failed to construct client")
			return nil, err
		}
		if err != nil {
			klog.Error(err, "Failed to dynamic client")
			return nil, err
		}
		var resync time.Duration
		if opts.Resync != nil {
			resync = *opts.Resync
		}

		informerMap, err := buildInformerMap(dynamicClient, opts, namespace, label, resync, globalGvks...)

		if err != nil {
			klog.Error(err, "Failed to build informer")
			return nil, err
		}

		fallback, err := cr_cache.New(config, opts)
		if err != nil {
			klog.Error(err, "Failed to init fallback cache")
			return nil, err
		}
		return customCache{clientSet: clientSet, dynamicClient: dynamicClient, informerMap: informerMap, fallback: fallback, Scheme: opts.Scheme, opts: opts}, nil
	}
}

func buildInformerMap(dynamicClient dynamic.Interface, opts cr_cache.Options, namespace string, label string, resync time.Duration, gvks ...schema.GroupVersionKind) (map[schema.GroupVersionKind]cache.SharedIndexInformer, error) {
	ctx := context.TODO()
	informerMap := make(map[schema.GroupVersionKind]cache.SharedIndexInformer)
	for _, gvk := range gvks {
		mapping, err := opts.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, err
		}
		listFunc := func(options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = label
			result, err := dynamicClient.Resource(mapping.Resource).Namespace(namespace).List(ctx, options)
			if err != nil {
				klog.Errorf("Failed to list %s error %s", gvk, err)
			}
			return result, err
		}
		watchFunc := func(options metav1.ListOptions) (watch.Interface, error) {
			options.Watch = true
			options.LabelSelector = label
			return dynamicClient.Resource(mapping.Resource).Namespace(namespace).Watch(ctx, options)
		}

		listerWatcher := &cache.ListWatch{ListFunc: listFunc, WatchFunc: watchFunc}
		informer := cache.NewSharedIndexInformer(listerWatcher, &corev1.Secret{}, resync, cache.Indexers{})
		informerMap[gvk] = informer
		gvkList := schema.GroupVersionKind{Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind + "List"}
		informerMap[gvkList] = informer
	}
	return informerMap, nil
}

type customCache struct {
	clientSet     *kubernetes.Clientset
	dynamicClient dynamic.Interface
	informerMap   map[schema.GroupVersionKind]cache.SharedIndexInformer
	fallback      cr_cache.Cache
	opts          cr_cache.Options
	Scheme        *runtime.Scheme
	config        *rest.Config
}

func (cc customCache) Get(ctx context.Context, key cr_client.ObjectKey, obj runtime.Object) error {

	gvk, err := apiutil.GVKForObject(obj, cc.Scheme)
	if err != nil {
		return err
	}

	if secret, ok := obj.(*corev1.Secret); ok {

		// Check store, then real client
		if err := cc.getSecretFromStore(key, secret); err != nil {
			fmt.Println("cache")
			fmt.Println(err)
			if err := cc.getSecretFromClient(ctx, key, secret); err != nil {
				fmt.Println("client")
				fmt.Println(err)
				return err
			}
		}
		secret.SetGroupVersionKind(gvk)
		return nil
	}
	if _, ok := obj.(*corev1.SecretList); ok {
		klog.Info("Get with SecretList object unsupported")
		return ErrUnsupported
	}
	if cm, ok := obj.(*corev1.ConfigMap); ok {
		// Check store, then real client
		if err := cc.getConfigMapFromStore(key, cm); err == nil {
			// Do nothing
		} else if err := cc.getConfigMapFromClient(ctx, key, cm); err != nil {
			return err
		}
		cm.SetGroupVersionKind(cmSchema)
		return nil
	}
	if _, ok := obj.(*corev1.ConfigMapList); ok {
		klog.Info("Get with ConfigMapList object unsupported")
		return ErrUnsupported
	}
	// Passthrough
	return cc.fallback.Get(ctx, key, obj)
}

func (cc customCache) getSecretFromStore(key cr_client.ObjectKey, obj *corev1.Secret) error {
	item, exists, err := cc.informerMap[secretSchema].GetStore().GetByKey(key.String())
	if err != nil {
		klog.Info("Failed to get item from cache", "error", err)
		return ErrInternalError
	}
	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{Group: secretSchema.Group, Resource: "secrets"}, key.String())
	}
	result, ok := item.(*corev1.Secret)
	if !ok {
		klog.Info("Failed to convert secret", "item", result)
		return ErrInternalError
	}

	result.DeepCopyInto(obj)
	return nil
}

func (cc customCache) getConfigMapFromStore(key cr_client.ObjectKey, obj *corev1.ConfigMap) error {
	item, exists, err := cc.informerMap[cmSchema].GetStore().GetByKey(key.String())
	if err != nil {
		klog.Info("Failed to get item from cache", "error", err)
		return ErrInternalError
	}
	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{Group: cmSchema.Group, Resource: "configmaps"}, key.String())
	}
	result, ok := item.(*corev1.ConfigMap)
	if !ok {
		klog.Info("Failed to convert configmap", "item", result)
		return ErrInternalError
	}
	result.DeepCopyInto(obj)
	return nil
}

func (cc customCache) getSecretFromClient(ctx context.Context, key cr_client.ObjectKey, obj *corev1.Secret) error {

	result, err := cc.clientSet.CoreV1().Secrets(key.Namespace).Get(ctx, key.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return err
	} else if err != nil {
		klog.Info("Failed to retrieve secret", "error", err)
		return err
	}

	result.DeepCopyInto(obj)
	return nil
}

func (cc customCache) getConfigMapFromClient(ctx context.Context, key cr_client.ObjectKey, obj *corev1.ConfigMap) error {

	result, err := cc.clientSet.CoreV1().ConfigMaps(key.Namespace).Get(ctx, key.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return err
	} else if err != nil {
		klog.Info("Failed to retrieve configmap", "error", err)
		return err
	}
	result.DeepCopyInto(obj)
	return nil
}

func (cc customCache) List(ctx context.Context, list runtime.Object, opts ...cr_client.ListOption) error {
	if _, ok := list.(*corev1.Secret); ok {
		klog.Info("List with Secret object unsupported")
		return ErrUnsupported
	}
	if secretList, ok := list.(*corev1.SecretList); ok {
		// Construct filter
		listOpts := cr_client.ListOptions{}
		for _, opt := range opts {
			opt.ApplyToList(&listOpts)
		}
		if listOpts.LabelSelector == nil || listOpts.LabelSelector.Empty() {
			klog.Info("Warning! Unfiltered List call. List only returns items watched by the filtered informer")
		}
		// Construct result
		result := cc.informerMap[secretSchema].GetStore().List()
		secretList.Items = make([]corev1.Secret, 0, len(result))
		// Filter items
		for _, item := range result {
			if secret, ok := item.(*corev1.Secret); ok && cc.secretMatchesOptions(secret, listOpts) {
				copy := secret.DeepCopy()
				copy.SetGroupVersionKind(secretSchema)
				secretList.Items = append(secretList.Items, *copy)
			}
		}
		secretList.SetGroupVersionKind(secretListSchema)
		klog.Info("Secret list filtered", "namespace", listOpts.Namespace, "filter", listOpts.LabelSelector, "all", len(result), "filtered", len(secretList.Items))
		return nil
	}
	if _, ok := list.(*corev1.Secret); ok {
		klog.Info("List with Secret object unsupported")
		return ErrUnsupported
	}

	if cmList, ok := list.(*corev1.ConfigMapList); ok {
		// Construct filter
		listOpts := cr_client.ListOptions{}
		for _, opt := range opts {
			opt.ApplyToList(&listOpts)
		}
		if listOpts.LabelSelector == nil || listOpts.LabelSelector.Empty() {
			klog.Info("Warning! Unfiltered List call. List only returns items watched by the filtered informer")
		}
		// Construct result
		result := cc.informerMap[cmSchema].GetStore().List()
		cmList.Items = make([]corev1.ConfigMap, 0, len(result))
		// Filter items
		for _, item := range result {
			if cm, ok := item.(*corev1.ConfigMap); ok && cc.configMapMatchesOptions(cm, listOpts) {
				copy := cm.DeepCopy()
				copy.SetGroupVersionKind(cmSchema)
				cmList.Items = append(cmList.Items, *copy)
			}
		}
		cmList.SetGroupVersionKind(cmListSchema)
		klog.Info("ConfigMap list filtered", "namespace", listOpts.Namespace, "filter", listOpts.LabelSelector, "all", len(result), "filtered", len(cmList.Items))
		return nil
	}
	// Passthrough
	return cc.fallback.List(ctx, list, opts...)
}

func (cc customCache) secretMatchesOptions(secret *corev1.Secret, opt cr_client.ListOptions) bool {
	if opt.Namespace != "" && secret.Namespace != opt.Namespace {
		return false
	}
	if opt.FieldSelector != nil && !opt.FieldSelector.Empty() {
		klog.Info("Field selector for SecretList not supported")
	}
	if opt.LabelSelector != nil && !opt.LabelSelector.Empty() {
		if !opt.LabelSelector.Matches(labels.Set(secret.Labels)) {
			return false
		}
	}
	return true
}

func (cc customCache) configMapMatchesOptions(cm *corev1.ConfigMap, opt cr_client.ListOptions) bool {
	if opt.Namespace != "" && cm.Namespace != opt.Namespace {
		return false
	}
	if opt.FieldSelector != nil && !opt.FieldSelector.Empty() {
		klog.Info("Field selector for ConfigMapList not supported")
	}
	if opt.LabelSelector != nil && !opt.LabelSelector.Empty() {
		if !opt.LabelSelector.Matches(labels.Set(cm.Labels)) {
			return false
		}
	}
	return true
}
func (cc customCache) GetInformer(ctx context.Context, obj runtime.Object) (cr_cache.Informer, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()

	if informer, ok := cc.informerMap[gvk]; ok {
		return informer, nil
	}
	// Passthrough
	return cc.fallback.GetInformer(ctx, obj)
}

func (cc customCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind) (cr_cache.Informer, error) {
	if informer, ok := cc.informerMap[gvk]; ok {
		return informer, nil
	}
	// Passthrough
	return cc.fallback.GetInformerForKind(ctx, gvk)
}

func (cc customCache) Start(stopCh <-chan struct{}) error {
	klog.Info("Start")
	for _, informer := range cc.informerMap {
		go informer.Run(stopCh)
	}
	return cc.fallback.Start(stopCh)
}

func (cc customCache) WaitForCacheSync(stop <-chan struct{}) bool {
	// Wait for secret informer to sync
	klog.Info("Waiting for secret and configmap informer to sync")
	waiting := true
	for waiting {
		select {
		case <-stop:
			waiting = false
		case <-time.After(time.Second):
			for _, informer := range cc.informerMap {
				waiting = !informer.HasSynced() && waiting
			}
		}
	}
	// Wait for fallback cache to sync
	klog.Info("Waiting for fallback informer to sync")
	return cc.fallback.WaitForCacheSync(stop)
}

func (cc customCache) IndexField(ctx context.Context, obj runtime.Object, field string, extractValue cr_client.IndexerFunc) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	if _, ok := cc.informerMap[gvk]; ok {
		klog.Infof("IndexField for %s not supported", gvk.String())
		return ErrUnsupported
	}

	return cc.fallback.IndexField(ctx, obj, field, extractValue)
}
