package tools

import (
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api/v1"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
	"k8s.io/kubernetes/staging/src/k8s.io/apimachinery/pkg/util/json"
)

const (
	SchedulingGroup = "ecp-scheduling-group"
	DefaultRole     = "default-role"
)

func KeyFunc(obj interface{}) (string, error) {
	group := obj.(*schedulerapi.SchedulingGroup)
	return group.Group, nil
}

func GetSchedulingGroup(pod *v1.Pod) *schedulerapi.MiniGroup {
	if pod.Annotations == nil {
		return NewMiniSchedulerGroup(pod)
	}

	data, ok := pod.Annotations[SchedulingGroup]
	if !ok {
		return NewMiniSchedulerGroup(pod)
	}

	var miniGroup schedulerapi.MiniGroup
	err := json.Unmarshal([]byte(data), &miniGroup)

	if err != nil {
		glog.Errorf("Failed to unmarshal miniGroup: %v\n", data)
		return nil
	}

	return &miniGroup
}

func NewMiniSchedulerGroup(pod *v1.Pod) *schedulerapi.MiniGroup {
	return &schedulerapi.MiniGroup{
		Group:       GetKeyOfPod(pod),
		Role:        DefaultRole,
		RoleCount:   1,
		MaxReplicas: 1,
		MinReplicas: 1,
		Priority:    1,
	}
}

func MiniGroupToGroup(miniGroup *schedulerapi.MiniGroup) *schedulerapi.SchedulingGroup {
	return &schedulerapi.SchedulingGroup{
		Group:         miniGroup.Group,
		ResourceCount: miniGroup.RoleCount,
		Resources:     []*schedulerapi.ResourceObject{},
		Status: &schedulerapi.SchedulerGroupState{
			State:      schedulerapi.Started,
			PodsToBind: make(map[string]*v1.Pod),
		},
	}
}

func GetKeyOfPod(pod *v1.Pod) string {
	return pod.Namespace + "/" + pod.Name
}

func SortGroupResource(group *schedulerapi.SchedulingGroup) {
	l := len(group.Resources)
	for i := 0; i < l; i++ {
		for j := 0; j < l-i-1; j++ {
			if group.Resources[j].Priority < group.Resources[j+1].Priority {
				group.Resources[j], group.Resources[j+1] = group.Resources[j+1], group.Resources[j]
			}
		}
	}
}
