/*
Copyright 2014 The Kubernetes Authors.

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

// Package factory can set up a scheduler. This code is here instead of
// plugin/cmd/scheduler for both testability and reuse.
package factory

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/api/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	appsinformers "k8s.io/kubernetes/pkg/client/informers/informers_generated/externalversions/apps/v1beta1"
	coreinformers "k8s.io/kubernetes/pkg/client/informers/informers_generated/externalversions/core/v1"
	extensionsinformers "k8s.io/kubernetes/pkg/client/informers/informers_generated/externalversions/extensions/v1beta1"
	appslisters "k8s.io/kubernetes/pkg/client/listers/apps/v1beta1"
	corelisters "k8s.io/kubernetes/pkg/client/listers/core/v1"
	extensionslisters "k8s.io/kubernetes/pkg/client/listers/extensions/v1beta1"
	"k8s.io/kubernetes/plugin/pkg/scheduler"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm/predicates"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
	"k8s.io/kubernetes/plugin/pkg/scheduler/api/validation"
	"k8s.io/kubernetes/plugin/pkg/scheduler/core"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
	"k8s.io/kubernetes/plugin/pkg/scheduler/tools"
	"k8s.io/kubernetes/plugin/pkg/scheduler/util"
)

const (
	initialGetBackoff = 100 * time.Millisecond
	maximalGetBackoff = time.Minute
)

// ConfigFactory is the default implementation of the scheduler.Configurator interface.
// TODO make this private if possible, so that only its interface is externally used.
type ConfigFactory struct {
	client   clientset.Interface
	groupMap map[string]*schedulerapi.SchedulingGroup
	// queue for groups that need scheduling
	groupQueue *cache.FIFO
	// a means to list all known scheduled pods.
	scheduledPodLister corelisters.PodLister
	// a means to list all known scheduled pods and pods assumed to have been scheduled.
	podLister algorithm.PodLister
	// a means to list all nodes
	nodeLister corelisters.NodeLister
	// a means to list all PersistentVolumes
	pVLister corelisters.PersistentVolumeLister
	// a means to list all PersistentVolumeClaims
	pVCLister corelisters.PersistentVolumeClaimLister
	// a means to list all services
	serviceLister corelisters.ServiceLister
	// a means to list all controllers
	controllerLister corelisters.ReplicationControllerLister
	// a means to list all replicasets
	replicaSetLister extensionslisters.ReplicaSetLister
	// a means to list all statefulsets
	statefulSetLister appslisters.StatefulSetLister

	// Close this to stop all reflectors
	StopEverything chan struct{}

	scheduledPodsHasSynced cache.InformerSynced

	schedulerCache schedulercache.Cache

	// SchedulerName of a scheduler is used to select which pods will be
	// processed by this scheduler, based on pods's "spec.SchedulerName".
	schedulerName string

	// RequiredDuringScheduling affinity is not symmetric, but there is an implicit PreferredDuringScheduling affinity rule
	// corresponding to every RequiredDuringScheduling affinity rule.
	// HardPodAffinitySymmetricWeight represents the weight of implicit PreferredDuringScheduling affinity rule, in the range 0-100.
	hardPodAffinitySymmetricWeight int

	// Equivalence class cache
	equivalencePodCache *core.EquivalenceCache
}

// NewConfigFactory initializes the default implementation of a Configurator To encourage eventual privatization of the struct type, we only
// return the interface.
func NewConfigFactory(
	schedulerName string,
	client clientset.Interface,
	nodeInformer coreinformers.NodeInformer,
	podInformer coreinformers.PodInformer,
	pvInformer coreinformers.PersistentVolumeInformer,
	pvcInformer coreinformers.PersistentVolumeClaimInformer,
	replicationControllerInformer coreinformers.ReplicationControllerInformer,
	replicaSetInformer extensionsinformers.ReplicaSetInformer,
	statefulSetInformer appsinformers.StatefulSetInformer,
	serviceInformer coreinformers.ServiceInformer,
	hardPodAffinitySymmetricWeight int,
) scheduler.Configurator {
	stopEverything := make(chan struct{})
	schedulerCache := schedulercache.New(30*time.Second, stopEverything)

	c := &ConfigFactory{
		client:                         client,
		podLister:                      schedulerCache,
		groupMap:                       make(map[string]*schedulerapi.SchedulingGroup),
		groupQueue:                     cache.NewFIFO(tools.KeyFunc),
		pVLister:                       pvInformer.Lister(),
		pVCLister:                      pvcInformer.Lister(),
		serviceLister:                  serviceInformer.Lister(),
		controllerLister:               replicationControllerInformer.Lister(),
		replicaSetLister:               replicaSetInformer.Lister(),
		statefulSetLister:              statefulSetInformer.Lister(),
		schedulerCache:                 schedulerCache,
		StopEverything:                 stopEverything,
		schedulerName:                  schedulerName,
		hardPodAffinitySymmetricWeight: hardPodAffinitySymmetricWeight,
	}

	c.scheduledPodsHasSynced = podInformer.Informer().HasSynced
	// scheduled pod cache
	podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return assignedNonTerminatedPod(t)
				default:
					runtime.HandleError(fmt.Errorf("unable to handle object in %T: %T", c, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    c.addPodToCache,
				UpdateFunc: c.updatePodInCache,
				DeleteFunc: c.deletePodFromCache,
			},
		},
	)
	// unscheduled pod queue
	podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return unassignedNonTerminatedPod(t)
				default:
					runtime.HandleError(fmt.Errorf("unable to handle object in %T: %T", c, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					pod, mini, targetGroup := c.GetSchedulingGroup(obj, true)
					if pod == nil || mini == nil || targetGroup == nil {
						glog.Warningf("Add: failed to get scheduling group.")
						return
					}
					c.AddPodToResourceObject(pod, mini, targetGroup)
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					pod, mini, targetGroup := c.GetSchedulingGroup(newObj, false)
					if pod == nil || mini == nil || targetGroup == nil {
						glog.Warningf("Update: failed to get scheduling group.")
						return
					}
					c.UpdatePodInResourceObject(pod, mini, targetGroup)
				},
				DeleteFunc: func(obj interface{}) {
					pod, mini, targetGroup := c.GetSchedulingGroup(obj, false)
					if pod == nil || mini == nil || targetGroup == nil {
						glog.Info("Delete: scheduling group is not exists.")
						return
					}

					c.DeletePodInResourceObject(pod, mini, targetGroup)
				},
			},
		},
	)
	// ScheduledPodLister is something we provide to plug-in functions that
	// they may need to call.
	c.scheduledPodLister = assignedPodLister{podInformer.Lister()}

	// Only nodes in the "Ready" condition with status == "True" are schedulable
	nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.addNodeToCache,
			UpdateFunc: c.updateNodeInCache,
			DeleteFunc: c.deleteNodeFromCache,
		},
		0,
	)
	c.nodeLister = nodeInformer.Lister()

	// TODO(harryz) need to fill all the handlers here and below for equivalence cache

	return c
}

func (c *ConfigFactory) GetSchedulingGroup(obj interface{}, newIfNotExists bool) (*v1.Pod, *schedulerapi.MiniGroup, *schedulerapi.SchedulingGroup) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		glog.Errorf("cannot convert to *v1.Pod: %v", obj)
		return nil, nil, nil
	}

	miniGroup := tools.GetSchedulingGroup(pod)
	targetGroup, ok := c.groupMap[miniGroup.Group]

	if !ok {
		if !newIfNotExists {
			return nil, nil, nil
		}
		targetGroup = tools.MiniGroupToGroup(miniGroup)
		c.groupMap[miniGroup.Group] = targetGroup
		glog.V(4).Infof("add group %s to group queue.", targetGroup.Group)
		if err := c.groupQueue.Add(targetGroup); err != nil {
			runtime.HandleError(fmt.Errorf("unable to queue %T: %v", obj, err))
		}
	}

	return pod, miniGroup, targetGroup
}

func (c *ConfigFactory) AddPodToResourceObject(pod *v1.Pod, miniGroup *schedulerapi.MiniGroup, group *schedulerapi.SchedulingGroup) {
	for _, ro := range group.Resources {
		if ro.Role == miniGroup.Role {
			_, ok := ro.PendingPods[pod.Name]
			ro.PendingPods[pod.Name] = pod
			if !ok {
				ro.PendingPodCount++
			}
			return
		}
	}
	resourceObject := &schedulerapi.ResourceObject{
		PendingPods:     make(map[string]*v1.Pod),
		PendingPodCount: 0,
		Role:            miniGroup.Role,
		Min:             miniGroup.MinReplicas,
		Max:             miniGroup.MaxReplicas,
		Priority:        miniGroup.Priority,
	}
	resourceObject.PendingPods[pod.Name] = pod
	resourceObject.PendingPodCount++

	group.Resources = append(group.Resources, resourceObject)
}

func (c *ConfigFactory) UpdatePodInResourceObject(pod *v1.Pod, miniGroup *schedulerapi.MiniGroup, group *schedulerapi.SchedulingGroup) {
	for _, ro := range group.Resources {
		if ro.Role == miniGroup.Role {
			_, ok := ro.PendingPods[pod.Name]
			if ok {
				ro.PendingPods[pod.Name] = pod
			}
			return
		}
	}
}

func (c *ConfigFactory) DeletePodInResourceObject(pod *v1.Pod, miniGroup *schedulerapi.MiniGroup, group *schedulerapi.SchedulingGroup) {
	zeroPodResourceObjectCount := 0
	for _, ro := range group.Resources {
		if ro.Role == miniGroup.Role {
			delete(ro.PendingPods, pod.Name)
			ro.PendingPodCount--
		}
		if ro.PendingPodCount == 0 {
			zeroPodResourceObjectCount++
		}
	}

	if zeroPodResourceObjectCount == group.ResourceCount {
		glog.Infof("All pods in group are deleted, forget group: %s", group.Group)
		group.Status.State = schedulerapi.Success
		delete(c.groupMap, group.Group)
	}
}

// GetNodeStore provides the cache to the nodes, mostly internal use, but may also be called by mock-tests.
func (c *ConfigFactory) GetNodeLister() corelisters.NodeLister {
	return c.nodeLister
}

func (c *ConfigFactory) GetHardPodAffinitySymmetricWeight() int {
	return c.hardPodAffinitySymmetricWeight
}

func (f *ConfigFactory) GetSchedulerName() string {
	return f.schedulerName
}

// GetClient provides a kubernetes client, mostly internal use, but may also be called by mock-tests.
func (f *ConfigFactory) GetClient() clientset.Interface {
	return f.client
}

// GetScheduledPodListerIndexer provides a pod lister, mostly internal use, but may also be called by mock-tests.
func (c *ConfigFactory) GetScheduledPodLister() corelisters.PodLister {
	return c.scheduledPodLister
}

// TODO(resouer) need to update all the handlers here and below for equivalence cache
func (c *ConfigFactory) addPodToCache(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		glog.Errorf("cannot convert to *v1.Pod: %v", obj)
		return
	}

	if err := c.schedulerCache.AddPod(pod); err != nil {
		glog.Errorf("scheduler cache AddPod failed: %v", err)
	}
}

func (c *ConfigFactory) updatePodInCache(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		glog.Errorf("cannot convert oldObj to *v1.Pod: %v", oldObj)
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		glog.Errorf("cannot convert newObj to *v1.Pod: %v", newObj)
		return
	}

	if err := c.schedulerCache.UpdatePod(oldPod, newPod); err != nil {
		glog.Errorf("scheduler cache UpdatePod failed: %v", err)
	}
}

func (c *ConfigFactory) deletePodFromCache(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			glog.Errorf("cannot convert to *v1.Pod: %v", t.Obj)
			return
		}
	default:
		glog.Errorf("cannot convert to *v1.Pod: %v", t)
		return
	}
	if err := c.schedulerCache.RemovePod(pod); err != nil {
		glog.Errorf("scheduler cache RemovePod failed: %v", err)
	}
}

func (c *ConfigFactory) addNodeToCache(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		glog.Errorf("cannot convert to *v1.Node: %v", obj)
		return
	}

	if err := c.schedulerCache.AddNode(node); err != nil {
		glog.Errorf("scheduler cache AddNode failed: %v", err)
	}
}

func (c *ConfigFactory) updateNodeInCache(oldObj, newObj interface{}) {
	oldNode, ok := oldObj.(*v1.Node)
	if !ok {
		glog.Errorf("cannot convert oldObj to *v1.Node: %v", oldObj)
		return
	}
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		glog.Errorf("cannot convert newObj to *v1.Node: %v", newObj)
		return
	}

	if err := c.schedulerCache.UpdateNode(oldNode, newNode); err != nil {
		glog.Errorf("scheduler cache UpdateNode failed: %v", err)
	}
}

func (c *ConfigFactory) deleteNodeFromCache(obj interface{}) {
	var node *v1.Node
	switch t := obj.(type) {
	case *v1.Node:
		node = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		node, ok = t.Obj.(*v1.Node)
		if !ok {
			glog.Errorf("cannot convert to *v1.Node: %v", t.Obj)
			return
		}
	default:
		glog.Errorf("cannot convert to *v1.Node: %v", t)
		return
	}
	if err := c.schedulerCache.RemoveNode(node); err != nil {
		glog.Errorf("scheduler cache RemoveNode failed: %v", err)
	}
}

// Create creates a scheduler with the default algorithm provider.
func (f *ConfigFactory) Create() (*scheduler.Config, error) {
	return f.CreateFromProvider(DefaultProvider)
}

// Creates a scheduler from the name of a registered algorithm provider.
func (f *ConfigFactory) CreateFromProvider(providerName string) (*scheduler.Config, error) {
	glog.V(2).Infof("Creating scheduler from algorithm provider '%v'", providerName)
	provider, err := GetAlgorithmProvider(providerName)
	if err != nil {
		return nil, err
	}

	return f.CreateFromKeys(provider.FitPredicateKeys, provider.PriorityFunctionKeys, []algorithm.SchedulerExtender{})
}

// Creates a scheduler from the configuration file
func (f *ConfigFactory) CreateFromConfig(policy schedulerapi.Policy) (*scheduler.Config, error) {
	glog.V(2).Infof("Creating scheduler from configuration: %v", policy)

	// validate the policy configuration
	if err := validation.ValidatePolicy(policy); err != nil {
		return nil, err
	}

	predicateKeys := sets.NewString()
	for _, predicate := range policy.Predicates {
		glog.V(2).Infof("Registering predicate: %s", predicate.Name)
		predicateKeys.Insert(RegisterCustomFitPredicate(predicate))
	}

	priorityKeys := sets.NewString()
	for _, priority := range policy.Priorities {
		glog.V(2).Infof("Registering priority: %s", priority.Name)
		priorityKeys.Insert(RegisterCustomPriorityFunction(priority))
	}

	extenders := make([]algorithm.SchedulerExtender, 0)
	if len(policy.ExtenderConfigs) != 0 {
		for ii := range policy.ExtenderConfigs {
			glog.V(2).Infof("Creating extender with config %+v", policy.ExtenderConfigs[ii])
			if extender, err := core.NewHTTPExtender(&policy.ExtenderConfigs[ii]); err != nil {
				return nil, err
			} else {
				extenders = append(extenders, extender)
			}
		}
	}
	// Providing HardPodAffinitySymmetricWeight in the policy config is the new and preferred way of providing the value.
	// Give it higher precedence than scheduler CLI configuration when it is provided.
	if policy.HardPodAffinitySymmetricWeight != 0 {
		f.hardPodAffinitySymmetricWeight = policy.HardPodAffinitySymmetricWeight
	}
	return f.CreateFromKeys(predicateKeys, priorityKeys, extenders)
}

// getBinder returns an extender that supports bind or a default binder.
func (f *ConfigFactory) getBinder(extenders []algorithm.SchedulerExtender) scheduler.Binder {
	for i := range extenders {
		if extenders[i].IsBinder() {
			return extenders[i]
		}
	}
	return &binder{f.client}
}

// Creates a scheduler from a set of registered fit predicate keys and priority keys.
func (f *ConfigFactory) CreateFromKeys(predicateKeys, priorityKeys sets.String, extenders []algorithm.SchedulerExtender) (*scheduler.Config, error) {
	glog.V(2).Infof("Creating scheduler with fit predicates '%v' and priority functions '%v", predicateKeys, priorityKeys)

	if f.GetHardPodAffinitySymmetricWeight() < 1 || f.GetHardPodAffinitySymmetricWeight() > 100 {
		return nil, fmt.Errorf("invalid hardPodAffinitySymmetricWeight: %d, must be in the range 1-100", f.GetHardPodAffinitySymmetricWeight())
	}

	predicateFuncs, err := f.GetPredicates(predicateKeys)
	if err != nil {
		return nil, err
	}

	priorityConfigs, err := f.GetPriorityFunctionConfigs(priorityKeys)
	if err != nil {
		return nil, err
	}

	priorityMetaProducer, err := f.GetPriorityMetadataProducer()
	if err != nil {
		return nil, err
	}

	predicateMetaProducer, err := f.GetPredicateMetadataProducer()
	if err != nil {
		return nil, err
	}

	// TODO(resouer) use equivalence cache instead of nil here when #36238 get merged
	algo := core.NewGenericScheduler(f.schedulerCache, nil, predicateFuncs, predicateMetaProducer, priorityConfigs, priorityMetaProducer, extenders)
	//podBackoff := util.CreateDefaultPodBackoff()
	return &scheduler.Config{
		SchedulerCache: f.schedulerCache,
		// The scheduler only needs to consider schedulable nodes.
		NodeLister:          &nodePredicateLister{f.nodeLister},
		Algorithm:           algo,
		Binder:              f.getBinder(extenders),
		PodConditionUpdater: &podConditionUpdater{f.client},
		ConfigMapTool:       &configMapTool{f.client},
		WaitForCacheSync: func() bool {
			return cache.WaitForCacheSync(f.StopEverything, f.scheduledPodsHasSynced)
		},
		NextSchedulingGroup: func() *schedulerapi.SchedulingGroup {
			return f.getNextSchedulingGroup()
		},
		PushBackSchedulingGroup: func(group *schedulerapi.SchedulingGroup) {
			f.pushbackSchedulingGroup(group)
			time.Sleep(2 * time.Second)
		},
		ForgetSchedulingGroup: func(group string) {
			delete(f.groupMap, group)
		},
		//Error:          f.MakeDefaultErrorFunc(podBackoff, f.podQueue),
		StopEverything: f.StopEverything,
	}, nil
}

type nodePredicateLister struct {
	corelisters.NodeLister
}

func (n *nodePredicateLister) List() ([]*v1.Node, error) {
	return n.ListWithPredicate(getNodeConditionPredicate())
}

func (f *ConfigFactory) GetPriorityFunctionConfigs(priorityKeys sets.String) ([]algorithm.PriorityConfig, error) {
	pluginArgs, err := f.getPluginArgs()
	if err != nil {
		return nil, err
	}

	return getPriorityFunctionConfigs(priorityKeys, *pluginArgs)
}

func (f *ConfigFactory) GetPriorityMetadataProducer() (algorithm.MetadataProducer, error) {
	pluginArgs, err := f.getPluginArgs()
	if err != nil {
		return nil, err
	}

	return getPriorityMetadataProducer(*pluginArgs)
}

func (f *ConfigFactory) GetPredicateMetadataProducer() (algorithm.MetadataProducer, error) {
	pluginArgs, err := f.getPluginArgs()
	if err != nil {
		return nil, err
	}
	return getPredicateMetadataProducer(*pluginArgs)
}

func (f *ConfigFactory) GetPredicates(predicateKeys sets.String) (map[string]algorithm.FitPredicate, error) {
	pluginArgs, err := f.getPluginArgs()
	if err != nil {
		return nil, err
	}

	return getFitPredicateFunctions(predicateKeys, *pluginArgs)
}

func (f *ConfigFactory) getPluginArgs() (*PluginFactoryArgs, error) {
	return &PluginFactoryArgs{
		PodLister:         f.podLister,
		ServiceLister:     f.serviceLister,
		ControllerLister:  f.controllerLister,
		ReplicaSetLister:  f.replicaSetLister,
		StatefulSetLister: f.statefulSetLister,
		// All fit predicates only need to consider schedulable nodes.
		NodeLister: &nodePredicateLister{f.nodeLister},
		NodeInfo:   &predicates.CachedNodeInfo{NodeLister: f.nodeLister},
		PVInfo:     &predicates.CachedPersistentVolumeInfo{PersistentVolumeLister: f.pVLister},
		PVCInfo:    &predicates.CachedPersistentVolumeClaimInfo{PersistentVolumeClaimLister: f.pVCLister},
		HardPodAffinitySymmetricWeight: f.hardPodAffinitySymmetricWeight,
	}, nil
}

func (f *ConfigFactory) getNextSchedulingGroup() *schedulerapi.SchedulingGroup {
	for {
		group := cache.Pop(f.groupQueue).(*schedulerapi.SchedulingGroup)
		if f.ResponsibleForGroup(group) {
			glog.V(4).Infof("About to try and schedule group %v", group.Group)
			return group
		}
	}
}

func (f *ConfigFactory) pushbackSchedulingGroup(group *schedulerapi.SchedulingGroup) {
	f.groupQueue.AddIfNotPresent(group)
}

func (f *ConfigFactory) ResponsibleForGroup(group *schedulerapi.SchedulingGroup) bool {
	return f.schedulerName == group.SchedulerName || group.SchedulerName == ""
}

func getNodeConditionPredicate() corelisters.NodeConditionPredicate {
	return func(node *v1.Node) bool {
		for i := range node.Status.Conditions {
			cond := &node.Status.Conditions[i]
			// We consider the node for scheduling only when its:
			// - NodeReady condition status is ConditionTrue,
			// - NodeOutOfDisk condition status is ConditionFalse,
			// - NodeNetworkUnavailable condition status is ConditionFalse.
			if cond.Type == v1.NodeReady && cond.Status != v1.ConditionTrue {
				glog.V(4).Infof("Ignoring node %v with %v condition status %v", node.Name, cond.Type, cond.Status)
				return false
			} else if cond.Type == v1.NodeOutOfDisk && cond.Status != v1.ConditionFalse {
				glog.V(4).Infof("Ignoring node %v with %v condition status %v", node.Name, cond.Type, cond.Status)
				return false
			} else if cond.Type == v1.NodeNetworkUnavailable && cond.Status != v1.ConditionFalse {
				glog.V(4).Infof("Ignoring node %v with %v condition status %v", node.Name, cond.Type, cond.Status)
				return false
			}
		}
		// Ignore nodes that are marked unschedulable
		if node.Spec.Unschedulable {
			glog.V(4).Infof("Ignoring node %v since it is unschedulable", node.Name)
			return false
		}
		return true
	}
}

// unassignedNonTerminatedPod selects pods that are unassigned and non-terminal.
func unassignedNonTerminatedPod(pod *v1.Pod) bool {
	if len(pod.Spec.NodeName) != 0 {
		return false
	}
	if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
		return false
	}
	return true
}

// assignedNonTerminatedPod selects pods that are assigned and non-terminal (scheduled and running).
func assignedNonTerminatedPod(pod *v1.Pod) bool {
	if len(pod.Spec.NodeName) == 0 {
		return false
	}
	if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
		return false
	}
	return true
}

// assignedPodLister filters the pods returned from a PodLister to
// only include those that have a node name set.
type assignedPodLister struct {
	corelisters.PodLister
}

// List lists all Pods in the indexer for a given namespace.
func (l assignedPodLister) List(selector labels.Selector) ([]*v1.Pod, error) {
	list, err := l.PodLister.List(selector)
	if err != nil {
		return nil, err
	}
	filtered := make([]*v1.Pod, 0, len(list))
	for _, pod := range list {
		if len(pod.Spec.NodeName) > 0 {
			filtered = append(filtered, pod)
		}
	}
	return filtered, nil
}

// List lists all Pods in the indexer for a given namespace.
func (l assignedPodLister) Pods(namespace string) corelisters.PodNamespaceLister {
	return assignedPodNamespaceLister{l.PodLister.Pods(namespace)}
}

// assignedPodNamespaceLister filters the pods returned from a PodNamespaceLister to
// only include those that have a node name set.
type assignedPodNamespaceLister struct {
	corelisters.PodNamespaceLister
}

// List lists all Pods in the indexer for a given namespace.
func (l assignedPodNamespaceLister) List(selector labels.Selector) (ret []*v1.Pod, err error) {
	list, err := l.PodNamespaceLister.List(selector)
	if err != nil {
		return nil, err
	}
	filtered := make([]*v1.Pod, 0, len(list))
	for _, pod := range list {
		if len(pod.Spec.NodeName) > 0 {
			filtered = append(filtered, pod)
		}
	}
	return filtered, nil
}

// Get retrieves the Pod from the indexer for a given namespace and name.
func (l assignedPodNamespaceLister) Get(name string) (*v1.Pod, error) {
	pod, err := l.PodNamespaceLister.Get(name)
	if err != nil {
		return nil, err
	}
	if len(pod.Spec.NodeName) > 0 {
		return pod, nil
	}
	return nil, errors.NewNotFound(schema.GroupResource{Resource: "pods"}, name)
}

type podInformer struct {
	informer cache.SharedIndexInformer
}

func (i *podInformer) Informer() cache.SharedIndexInformer {
	return i.informer
}

func (i *podInformer) Lister() corelisters.PodLister {
	return corelisters.NewPodLister(i.informer.GetIndexer())
}

// NewPodInformer creates a shared index informer that returns only non-terminal pods.
func NewPodInformer(client clientset.Interface, resyncPeriod time.Duration) coreinformers.PodInformer {
	selector := fields.ParseSelectorOrDie("status.phase!=" + string(v1.PodSucceeded) + ",status.phase!=" + string(v1.PodFailed))
	lw := cache.NewListWatchFromClient(client.Core().RESTClient(), "pods", metav1.NamespaceAll, selector)
	return &podInformer{
		informer: cache.NewSharedIndexInformer(lw, &v1.Pod{}, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}),
	}
}

func (factory *ConfigFactory) MakeDefaultErrorFunc(backoff *util.PodBackoff, podQueue *cache.FIFO) func(pod *v1.Pod, err error) {
	return func(pod *v1.Pod, err error) {
		if err == core.ErrNoNodesAvailable {
			glog.V(4).Infof("Unable to schedule %v %v: no nodes are registered to the cluster; waiting", pod.Namespace, pod.Name)
		} else {
			if _, ok := err.(*core.FitError); ok {
				glog.V(4).Infof("Unable to schedule %v %v: no fit: %v; waiting", pod.Namespace, pod.Name, err)
			} else {
				glog.Errorf("Error scheduling %v %v: %v; retrying", pod.Namespace, pod.Name, err)
			}
		}
		backoff.Gc()
		// Retry asynchronously.
		// Note that this is extremely rudimentary and we need a more real error handling path.
		go func() {
			defer runtime.HandleCrash()
			podID := types.NamespacedName{
				Namespace: pod.Namespace,
				Name:      pod.Name,
			}

			entry := backoff.GetEntry(podID)
			if !entry.TryWait(backoff.MaxDuration()) {
				glog.Warningf("Request for pod %v already in flight, abandoning", podID)
				return
			}
			// Get the pod again; it may have changed/been scheduled already.
			getBackoff := initialGetBackoff
			for {
				pod, err := factory.client.Core().Pods(podID.Namespace).Get(podID.Name, metav1.GetOptions{})
				if err == nil {
					if len(pod.Spec.NodeName) == 0 {
						podQueue.AddIfNotPresent(pod)
					}
					break
				}
				if errors.IsNotFound(err) {
					glog.Warningf("A pod %v no longer exists", podID)
					return
				}
				glog.Errorf("Error getting pod %v for retry: %v; retrying...", podID, err)
				if getBackoff = getBackoff * 2; getBackoff > maximalGetBackoff {
					getBackoff = maximalGetBackoff
				}
				time.Sleep(getBackoff)
			}
		}()
	}
}

// nodeEnumerator allows a cache.Poller to enumerate items in an v1.NodeList
type nodeEnumerator struct {
	*v1.NodeList
}

// Len returns the number of items in the node list.
func (ne *nodeEnumerator) Len() int {
	if ne.NodeList == nil {
		return 0
	}
	return len(ne.Items)
}

// Get returns the item (and ID) with the particular index.
func (ne *nodeEnumerator) Get(index int) interface{} {
	return &ne.Items[index]
}

type binder struct {
	Client clientset.Interface
}

// Bind just does a POST binding RPC.
func (b *binder) Bind(binding *v1.Binding) error {
	glog.V(3).Infof("Attempting to bind %v to %v", binding.Name, binding.Target.Name)
	return b.Client.CoreV1().Pods(binding.Namespace).Bind(binding)
}

type podConditionUpdater struct {
	Client clientset.Interface
}

func (p *podConditionUpdater) Update(pod *v1.Pod, condition *v1.PodCondition) error {
	glog.V(2).Infof("Updating pod condition for %s/%s to (%s==%s)", pod.Namespace, pod.Name, condition.Type, condition.Status)
	if podutil.UpdatePodCondition(&pod.Status, condition) {
		_, err := p.Client.Core().Pods(pod.Namespace).UpdateStatus(pod)
		return err
	}
	return nil
}

type configMapTool struct {
	Client clientset.Interface
}

func (c *configMapTool) Get(namespace, name string) (*v1.ConfigMap, error) {
	config, err := c.Client.CoreV1().ConfigMaps(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (c *configMapTool) Update(configMap *v1.ConfigMap) error {
	_, err := c.Client.CoreV1().ConfigMaps(configMap.Namespace).Update(configMap)
	return err
}
