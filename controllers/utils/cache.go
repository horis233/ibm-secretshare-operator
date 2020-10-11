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
	"reflect"
	"strings"
	"time"

	"github.com/gobuffalo/flect"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// NewFilteredCacheBuilder create a customized cache with a filter for specified resources
func NewFilteredCacheBuilder(gvkLabelMap map[schema.GroupVersionKind]string) cache.NewCacheFunc {
	return func(config *rest.Config, opts cache.Options) (cache.Cache, error) {
		// Create a client for fetching resources
		clientSet, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, err
		}

		// Get the frequency that informers are resynced
		var resync time.Duration
		if opts.Resync != nil {
			resync = *opts.Resync
		}

		// Generate informermap to contain the gvks and their informers
		informerMap := buildInformerMap(clientSet, opts, gvkLabelMap, resync)

		// Create a default cache for the unspecified resources
		fallback, err := cache.New(config, opts)
		if err != nil {
			klog.Error(err, "Failed to init fallback cache")
			return nil, err
		}

		// Return the customized cache
		return filteredCache{clientSet: clientSet, informerMap: informerMap, labelSelectorMap: gvkLabelMap, fallback: fallback, Scheme: opts.Scheme}, nil
	}
}

//buildInformerMap create informerMap of the specified resource
func buildInformerMap(clientSet *kubernetes.Clientset, opts cache.Options, gvkLabelMap map[schema.GroupVersionKind]string, resync time.Duration) map[schema.GroupVersionKind]toolscache.SharedIndexInformer {
	// Initializer maps
	informerMap := make(map[schema.GroupVersionKind]toolscache.SharedIndexInformer)

	for gvk, label := range gvkLabelMap {
		// Get the plural type of the kind as resource
		plural := kindToResource(gvk.Kind)

		// Create ListerWatcher with the label by NewFilteredListWatchFromClient
		listerWatcher := toolscache.NewFilteredListWatchFromClient(clientSet.CoreV1().RESTClient(), plural, opts.Namespace, func(options *metav1.ListOptions) {
			options.LabelSelector = label
		})

		// Build typed runtime object for informer
		objType := &unstructured.Unstructured{}
		objType.GetObjectKind().SetGroupVersionKind(gvk)
		typed, err := opts.Scheme.New(gvk)
		if err != nil {
			klog.Error(err)
			continue
		}
		runtime.DefaultUnstructuredConverter.FromUnstructured(objType.UnstructuredContent(), typed)

		// Create new inforemer with the listerwatcher
		informer := toolscache.NewSharedIndexInformer(listerWatcher, typed, resync, toolscache.Indexers{})

		informerMap[gvk] = informer
		// Build list type for the GVK
		gvkList := schema.GroupVersionKind{Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind + "List"}
		informerMap[gvkList] = informer
	}
	return informerMap
}

// filteredCache is the customized cache by the specified label
type filteredCache struct {
	clientSet        *kubernetes.Clientset
	informerMap      map[schema.GroupVersionKind]toolscache.SharedIndexInformer
	labelSelectorMap map[schema.GroupVersionKind]string
	fallback         cache.Cache
	Scheme           *runtime.Scheme
}

// Get implements Reader
// If the resource is in the cache, Get function get fetch in from the informer
// Otherwise, resource will be get by the k8s client
func (c filteredCache) Get(ctx context.Context, key client.ObjectKey, obj runtime.Object) error {

	// Get the GVK of the runtime object
	gvk, err := apiutil.GVKForObject(obj, c.Scheme)
	if err != nil {
		klog.Error(err)
		return err
	}

	if informer, ok := c.informerMap[gvk]; ok {
		// Looking for object from the cache
		if err := c.getFromStore(informer, key, obj, gvk); err == nil {
			// If not found the object from cache, then fetch it from k8s apiserver
		} else if err := c.getFromClient(ctx, key, obj, gvk); err != nil {
			klog.Error(err)
			return err
		}
		return nil
	}

	// Passthrough
	return c.fallback.Get(ctx, key, obj)
}

// getFromStore get the resource from the cache
func (c filteredCache) getFromStore(informer toolscache.SharedIndexInformer, key client.ObjectKey, obj runtime.Object, gvk schema.GroupVersionKind) error {

	var keyString string
	if key.Namespace == "" {
		keyString = key.Name
	} else {
		keyString = key.Namespace + "/" + key.Name
	}

	item, exists, err := informer.GetStore().GetByKey(keyString)
	if err != nil {
		klog.Info("Failed to get item from cache", "error", err)
		return err
	}
	if !exists {
		return apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, key.String())
	}
	if _, isObj := item.(runtime.Object); !isObj {
		// This should never happen
		return fmt.Errorf("cache contained %T, which is not an Object", item)
	}

	// deep copy to avoid mutating cache
	item = item.(runtime.Object).DeepCopyObject()

	// Copy the value of the item in the cache to the returned value
	objVal := reflect.ValueOf(obj)
	itemVal := reflect.ValueOf(item)

	if !objVal.Type().AssignableTo(objVal.Type()) {
		return fmt.Errorf("cache had type %s, but %s was asked for", itemVal.Type(), objVal.Type())
	}
	reflect.Indirect(objVal).Set(reflect.Indirect(itemVal))
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	return nil
}

// getFromClient gets the resource from the k8s client
func (c filteredCache) getFromClient(ctx context.Context, key client.ObjectKey, obj runtime.Object, gvk schema.GroupVersionKind) error {

	resource := kindToResource(gvk.Kind)
	result, err := c.clientSet.CoreV1().RESTClient().
		Get().
		Namespace(key.Namespace).
		Name(key.Name).
		Resource(resource).
		VersionedParams(&metav1.GetOptions{}, metav1.ParameterCodec).
		Do(ctx).
		Get()

	if apierrors.IsNotFound(err) {
		return err
	} else if err != nil {
		klog.Info("Failed to retrieve resource list", "error", err)
		return err
	}

	// Copy the value of the item in the cache to the returned value
	objVal := reflect.ValueOf(obj)
	itemVal := reflect.ValueOf(result)

	if !objVal.Type().AssignableTo(objVal.Type()) {
		return fmt.Errorf("cache had type %s, but %s was asked for", itemVal.Type(), objVal.Type())
	}
	reflect.Indirect(objVal).Set(reflect.Indirect(itemVal))
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	return nil
}

// List lists items out of the indexer and writes them to list
func (c filteredCache) List(ctx context.Context, list runtime.Object, opts ...client.ListOption) error {
	gvk, err := apiutil.GVKForObject(list, c.Scheme)
	if err != nil {
		return err
	}
	if informer, ok := c.informerMap[gvk]; ok {
		// Construct filter
		var objList []interface{}

		listOpts := client.ListOptions{}
		listOpts.ApplyOptions(opts)

		if listOpts.LabelSelector == nil {
			return c.ListFromClient(ctx, list, gvk, opts...)
		}

		if listOpts.LabelSelector != nil && listOpts.LabelSelector.String() != c.labelSelectorMap[listToKind(gvk)] {
			return c.ListFromClient(ctx, list, gvk, opts...)
		}

		var labelSel labels.Selector

		if listOpts.LabelSelector != nil {
			labelSel = listOpts.LabelSelector
		}

		objList = informer.GetStore().List()

		runtimeObjList := make([]runtime.Object, 0, len(objList))
		for _, item := range objList {
			obj, isObj := item.(runtime.Object)
			if !isObj {
				return fmt.Errorf("cache contained %T, which is not an Object", obj)
			}
			meta, err := apimeta.Accessor(obj)
			if err != nil {
				return err
			}

			if listOpts.Namespace != "" && listOpts.Namespace != meta.GetNamespace() {
				continue
			}

			if labelSel != nil {
				lbls := labels.Set(meta.GetLabels())
				if !labelSel.Matches(lbls) {
					continue
				}
			}

			outObj := obj.DeepCopyObject()
			outObj.GetObjectKind().SetGroupVersionKind(listToKind(gvk))
			runtimeObjList = append(runtimeObjList, outObj)
		}
		return apimeta.SetList(list, runtimeObjList)
	}

	// Passthrough
	return c.fallback.List(ctx, list, opts...)
}

func (c filteredCache) ListFromClient(ctx context.Context, list runtime.Object, gvk schema.GroupVersionKind, opts ...client.ListOption) error {

	listOpts := client.ListOptions{}
	listOpts.ApplyOptions(opts)

	var labelSelector, fieldSelector string
	if listOpts.FieldSelector != nil {
		fieldSelector = listOpts.FieldSelector.String()
	}
	if listOpts.LabelSelector != nil {
		labelSelector = listOpts.LabelSelector.String()
	}

	resource := kindToResource(gvk.Kind[:len(gvk.Kind)-4])

	result, err := c.clientSet.CoreV1().RESTClient().
		Get().
		Namespace(listOpts.Namespace).
		Resource(resource).
		VersionedParams(&metav1.ListOptions{
			LabelSelector: labelSelector,
			FieldSelector: fieldSelector,
		}, metav1.ParameterCodec).
		Do(ctx).
		Get()

	if err != nil {
		klog.Info("Failed to retrieve resource list: ", err)
		return err
	}

	// Copy the value of the item in the cache to the returned value
	objVal := reflect.ValueOf(list)
	itemVal := reflect.ValueOf(result)

	if !objVal.Type().AssignableTo(objVal.Type()) {
		return fmt.Errorf("cache had type %s, but %s was asked for", itemVal.Type(), objVal.Type())
	}
	reflect.Indirect(objVal).Set(reflect.Indirect(itemVal))
	list.GetObjectKind().SetGroupVersionKind(gvk)

	return nil
}

func (c filteredCache) GetInformer(ctx context.Context, obj runtime.Object) (cache.Informer, error) {
	gvk, err := apiutil.GVKForObject(obj, c.Scheme)
	if err != nil {
		return nil, err
	}

	if informer, ok := c.informerMap[gvk]; ok {
		return informer, nil
	}
	// Passthrough
	return c.fallback.GetInformer(ctx, obj)
}

func (c filteredCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind) (cache.Informer, error) {
	if informer, ok := c.informerMap[gvk]; ok {
		return informer, nil
	}
	// Passthrough
	return c.fallback.GetInformerForKind(ctx, gvk)
}

func (c filteredCache) Start(stopCh <-chan struct{}) error {
	klog.Info("Start")
	for _, informer := range c.informerMap {
		go informer.Run(stopCh)
	}
	return c.fallback.Start(stopCh)
}

func (c filteredCache) WaitForCacheSync(stop <-chan struct{}) bool {
	// Wait for informer to sync
	klog.Info("Waiting for informer to sync")
	waiting := true
	for waiting {
		select {
		case <-stop:
			waiting = false
		case <-time.After(time.Second):
			for _, informer := range c.informerMap {
				waiting = !informer.HasSynced() && waiting
			}
		}
	}
	// Wait for fallback cache to sync
	klog.Info("Waiting for fallback informer to sync")
	return c.fallback.WaitForCacheSync(stop)
}

func (c filteredCache) IndexField(ctx context.Context, obj runtime.Object, field string, extractValue client.IndexerFunc) error {
	gvk, err := apiutil.GVKForObject(obj, c.Scheme)
	if err != nil {
		return err
	}

	if _, ok := c.informerMap[gvk]; ok {
		klog.Infof("IndexField for %s not supported", gvk.String())
		return errors.New("IndexField for " + gvk.String() + " not supported")
	}

	return c.fallback.IndexField(ctx, obj, field, extractValue)
}

func kindToResource(kind string) string {
	return strings.ToLower(flect.Pluralize(kind))
}

func listToKind(list schema.GroupVersionKind) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: list.Group, Version: list.Version, Kind: list.Kind[:len(list.Kind)-4]}
}
