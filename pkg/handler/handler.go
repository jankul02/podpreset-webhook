package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	redhatcopv1alpha1 "github.com/redhat-cop/podpreset-webhook/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	annotationPrefix = "podpreset.admission.kubernetes.io"
)

// +kubebuilder:webhook:path=/mutate,mutating=true,failurePolicy=ignore,groups="",resources=pods,verbs=create,versions=v1,name=mpod.redhatcop.redhat.io,sideEffects=None,admissionReviewVersions={v1,v1beta1}
// +kubebuilder:rbac:groups=redhatcop.redhat.io,resources=podpresets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;update;patch

// PodPresetMutator mutates Pods
type PodPresetMutator struct {
	Client  client.Client
	decoder *admission.Decoder
	Log     logr.Logger
}

// PodPresetMutator adds an annotation to every incoming pods.
func (a *PodPresetMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := a.Log.WithValues("podpreset-webhook", fmt.Sprintf("%s/%s", req.Namespace, req.Name))

	logger.Info("1. Handle")

	// Ignore all calls to subresources or resources other than pods.
	// Ignore all operations other than CREATE.
	if len(req.SubResource) != 0 || req.Resource.Group != "" || req.Operation != "CREATE" {
		logger.Info("Ignoring a  call on subresources or resources other than pods ")
		return admission.Allowed("")
	}

	pod := &corev1.Pod{}

	err := a.decoder.Decode(req, pod)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	logger.Info("2 apparently  a Pod for mutation =" + pod.GetName())
	// Begin Mutation

	if _, isMirrorPod := pod.Annotations[corev1.MirrorPodAnnotationKey]; isMirrorPod {
		return admission.Allowed("Mirror Pod")
	}

	// Ignore if exclusion annotation is present
	if podAnnotations := pod.GetAnnotations(); podAnnotations != nil {
		if podAnnotations[corev1.PodPresetOptOutAnnotationKey] == "true" {
			return admission.Allowed("Exclusion Annotation Present")
		}
	}

	podPresetList := &redhatcopv1alpha1.PodPresetList{}

	err = a.Client.List(context.TODO(), podPresetList, &client.ListOptions{Namespace: req.Namespace})

	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("Error retrieving ist of PodPresets: %v", err))
	}

	matchingPPs, err := filterPodPresets(logger, *podPresetList, pod)
	if err != nil {
		logger.Info("  an error => closing")
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("filtering pod presets failed: %v", err))
	}

	if len(matchingPPs) == 0 {
		logger.Info("  the Pod for mutation 0  matchingPPs => closing")
		return admission.Allowed("")
	}

	logger.Info(" the Pod for mutation =" + pod.GetName() + " has  matchingPPs")

	presetNames := make([]string, len(matchingPPs))
	for i, pp := range matchingPPs {
		presetNames[i] = pp.GetName()
		logger.Info("presetName[i] of pp=" + pp.GetName() + " is=" + presetNames[i])
	}

	// detect merge conflict
	err = safeToApplyPodPresetsOnPod(pod, matchingPPs)
	if err != nil {
		// conflict, ignore the error, but raise an event
		logger.Info("conflict occurred while applying podpresets: %s on pod: %v err: %v",
			strings.Join(presetNames, ","), pod.GetGenerateName(), err)
		admission.Allowed("")
	}

	applyPodPresetsOnPod(logger, pod, matchingPPs)

	// End Mutation
	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

// PodPresetMutator implements admission.DecoderInjector.
// A decoder will be automatically injected.

// InjectDecoder injects the decoder.
func (a *PodPresetMutator) InjectDecoder(d *admission.Decoder) error {
	a.decoder = d
	return nil
}

// filterPodPresets returns list of PodPresets which match given Pod.
func filterPodPresets(logger logr.Logger, list redhatcopv1alpha1.PodPresetList, pod *corev1.Pod) ([]*redhatcopv1alpha1.PodPreset, error) {
	var matchingPPs []*redhatcopv1alpha1.PodPreset

	logger.Info("pod.GetName()=" + pod.GetName())

	for idx, pp := range list.Items {

		selector, err := metav1.LabelSelectorAsSelector(&pp.Spec.Selector)
		if err != nil {
			return nil, fmt.Errorf("label selector conversion failed: %v for selector: %v", pp.Spec.Selector, err)
		}
		logger.Info("selector.String() from podselector=" + selector.String())
		logger.Info("labels.Set(pod.Labels).String() in the given pod =" + labels.Set(pod.Labels).String())

		podnamerequiredvalue, found := selector.RequiresExactMatch("podnamerequired")
		logger.Info("checking if found RequiresExactMatch podnamerequired for the given pod name" + pod.GetName())

		if found {
			logger.Info(">>&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
			logger.Info("A field podnamerequired found with the value=" + podnamerequiredvalue)
			if podnamerequiredvalue != pod.GetName() {
				logger.Info("but the field podnamerequired is not matching the pod name:" + pod.GetName() + "!=" + podnamerequiredvalue + "=====> next loop")
				continue
			} else {
				logger.Info("podnamerequiredvalue is matching the current pod name:" + pod.GetName() + "==" + podnamerequiredvalue)
				// check if general matching
				lbls := pod.Labels
				lbls["podnamerequired"] = pod.GetName()
				logger.Info("labels.Set(lbls).String()=" + labels.Set(lbls).String())
				// check if the pod labels match the selector
				if !selector.Matches(labels.Set(lbls)) {
					logger.Info("!selector.Matches(labels.Set(lbls)=====> next loop")
					continue
				}
			}
			logger.Info("<<&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
		} else {
			logger.Info(">>==============================")
			logger.Info("NO field podnamerequired found => checking if generic label match is there for the pod=" + pod.GetName())
			// check if the pod labels match the selector (generic case, no requirements on pod name)
			if !selector.Matches(labels.Set(pod.Labels)) {
				logger.Info("no generic label match found !selector.Matches(labels.Set(lbls)=====> next loop")
				continue
			}
			logger.Info("<<==============================")
		}
		logger.Info(">>*********************** appending pp=" + pp.Name)
		matchingPPs = append(matchingPPs, &list.Items[idx])
		logger.Info("<<*********************** appending pp=" + pp.Name)

	}
	logger.Info(">>%%%%%%%%%%%%%%%%%%% for  pod=" + pod.GetName())

	if len(matchingPPs) == 0 {
		logger.Info("000000000000 no final preset for pod=" + pod.GetName())
	} else {
		for _, ppr := range matchingPPs {
			logger.Info("final preset for pod=" + pod.GetName() + " is name=" + ppr.GetName())
		}
	}
	logger.Info("<<%%%%%%%%%%%%%%%%%%% for  pod=" + pod.GetName())

	return matchingPPs, nil
}

// safeToApplyPodPresetsOnPod determines if there is any conflict in information
// injected by given PodPresets in the Pod.
func safeToApplyPodPresetsOnPod(pod *corev1.Pod, podPresets []*redhatcopv1alpha1.PodPreset) error {
	var errs []error

	// volumes attribute is defined at the Pod level, so determine if volumes
	// injection is causing any conflict.
	if _, err := mergeVolumes(pod.Spec.Volumes, podPresets); err != nil {
		errs = append(errs, err)
	}

	return utilerrors.NewAggregate(errs)
}

// safeToApplyPodPresetsOnContainer determines if there is any conflict in
// information injected by given PodPresets in the given container.
func safeToApplyPodPresetsOnContainer(logger logr.Logger, ctr *corev1.Container, podPresets []*redhatcopv1alpha1.PodPreset) error {
	var errs []error
	// check if it is safe to merge env vars and volume mounts from given podpresets and
	// container's existing env vars.
	if _, err := mergeEnv(logger, ctr.Env, podPresets); err != nil {
		errs = append(errs, err)
	}
	if _, err := mergeVolumeMounts(ctr.VolumeMounts, podPresets); err != nil {
		errs = append(errs, err)
	}

	return utilerrors.NewAggregate(errs)
}

// mergeEnv merges a list of env vars with the env vars injected by given list podPresets.
// It returns an error if it detects any conflict during the merge.
func mergeEnv(logger logr.Logger, envVars []corev1.EnvVar, podPresets []*redhatcopv1alpha1.PodPreset) ([]corev1.EnvVar, error) {
	origEnv := map[string]corev1.EnvVar{}
	index := make(map[string]int)
	for idx, v := range envVars {
		origEnv[v.Name] = v
		index[v.Name] = idx
	}

	mergedEnv := make([]corev1.EnvVar, len(envVars))
	copy(mergedEnv, envVars)

	var errs []error

	for _, pp := range podPresets {
		logger.Info("merging env from pp.Name " + pp.Name)
		for _, v := range pp.Spec.Env {
			logger.Info("merging env var name:" + v.Name)

			_, ok := origEnv[v.Name]
			if !ok {
				logger.Info("will be appendend - not found in current env:" + v.Name)
				// if we don't already have it append it and continue
				origEnv[v.Name] = v
				mergedEnv = append(mergedEnv, v)
				continue
			} else {
				logger.Info(" found -> overwrite value - found in current env:" + v.Name)
				mergedEnv[index[v.Name]] = v

			}

			// // make sure they are identical or throw an error
			// if !reflect.DeepEqual(found, v) {
			// 	errs = append(errs, fmt.Errorf("merging env for %s has a conflict on %s: \n%#v\ndoes not match\n%#v\n in container", pp.GetName(), v.Name, v, found))
			// }
		}
	}

	err := utilerrors.NewAggregate(errs)
	if err != nil {
		return nil, err
	}

	return mergedEnv, err
}

type envFromMergeKey struct {
	prefix           string
	configMapRefName string
	secretRefName    string
}

func newEnvFromMergeKey(e corev1.EnvFromSource) envFromMergeKey {
	k := envFromMergeKey{prefix: e.Prefix}
	if e.ConfigMapRef != nil {
		k.configMapRefName = e.ConfigMapRef.Name
	}
	if e.SecretRef != nil {
		k.secretRefName = e.SecretRef.Name
	}
	return k
}

func mergeEnvFrom(envSources []corev1.EnvFromSource, podPresets []*redhatcopv1alpha1.PodPreset) ([]corev1.EnvFromSource, error) {
	var mergedEnvFrom []corev1.EnvFromSource

	// merge envFrom using a identify key to ensure Admit reinvocations are idempotent
	origEnvSources := map[envFromMergeKey]corev1.EnvFromSource{}
	for _, envSource := range envSources {
		origEnvSources[newEnvFromMergeKey(envSource)] = envSource
	}
	mergedEnvFrom = append(mergedEnvFrom, envSources...)
	var errs []error
	for _, pp := range podPresets {
		for _, envFromSource := range pp.Spec.EnvFrom {

			found, ok := origEnvSources[newEnvFromMergeKey(envFromSource)]
			if !ok {
				mergedEnvFrom = append(mergedEnvFrom, envFromSource)
				continue
			}
			if !reflect.DeepEqual(found, envFromSource) {
				errs = append(errs, fmt.Errorf("merging envFrom for %s has a conflict: \n%#v\ndoes not match\n%#v\n in container", pp.GetName(), envFromSource, found))
			}
		}

	}

	err := utilerrors.NewAggregate(errs)
	if err != nil {
		return nil, err
	}

	return mergedEnvFrom, nil
}

// mergeVolumeMounts merges given list of VolumeMounts with the volumeMounts
// injected by given podPresets. It returns an error if it detects any conflict during the merge.
func mergeVolumeMounts(volumeMounts []corev1.VolumeMount, podPresets []*redhatcopv1alpha1.PodPreset) ([]corev1.VolumeMount, error) {

	origVolumeMounts := map[string]corev1.VolumeMount{}
	volumeMountsByPath := map[string]corev1.VolumeMount{}
	for _, v := range volumeMounts {
		origVolumeMounts[v.Name] = v
		volumeMountsByPath[v.MountPath] = v
	}

	mergedVolumeMounts := make([]corev1.VolumeMount, len(volumeMounts))
	copy(mergedVolumeMounts, volumeMounts)

	var errs []error

	for _, pp := range podPresets {
		for _, v := range pp.Spec.VolumeMounts {

			found, ok := origVolumeMounts[v.Name]
			if !ok {
				// if we don't already have it append it and continue
				origVolumeMounts[v.Name] = v
				mergedVolumeMounts = append(mergedVolumeMounts, v)
			} else {
				// make sure they are identical or throw an error
				// shall we throw an error for identical volumeMounts ?
				if !reflect.DeepEqual(found, v) {
					errs = append(errs, fmt.Errorf("merging volume mounts for %s has a conflict on %s: \n%#v\ndoes not match\n%#v\n in container", pp.GetName(), v.Name, v, found))
				}
			}

			found, ok = volumeMountsByPath[v.MountPath]
			if !ok {
				// if we don't already have it append it and continue
				volumeMountsByPath[v.MountPath] = v
			} else {
				// make sure they are identical or throw an error
				if !reflect.DeepEqual(found, v) {
					errs = append(errs, fmt.Errorf("merging volume mounts for %s has a conflict on mount path %s: \n%#v\ndoes not match\n%#v\n in container", pp.GetName(), v.MountPath, v, found))
				}
			}
		}
	}

	err := utilerrors.NewAggregate(errs)
	if err != nil {
		return nil, err
	}

	return mergedVolumeMounts, err
}

// mergeVolumes merges given list of Volumes with the volumes injected by given
// podPresets. It returns an error if it detects any conflict during the merge.
func mergeVolumes(volumes []corev1.Volume, podPresets []*redhatcopv1alpha1.PodPreset) ([]corev1.Volume, error) {
	origVolumes := map[string]corev1.Volume{}
	for _, v := range volumes {
		origVolumes[v.Name] = v
	}

	mergedVolumes := make([]corev1.Volume, len(volumes))
	copy(mergedVolumes, volumes)

	var errs []error

	for _, pp := range podPresets {
		for _, v := range pp.Spec.Volumes {

			found, ok := origVolumes[v.Name]
			if !ok {
				// if we don't already have it append it and continue
				origVolumes[v.Name] = v
				mergedVolumes = append(mergedVolumes, v)
				continue
			}

			// make sure they are identical or throw an error
			if !reflect.DeepEqual(found, v) {
				errs = append(errs, fmt.Errorf("merging volumes for %s has a conflict on %s: \n%#v\ndoes not match\n%#v\n in container", pp.GetName(), v.Name, v, found))
			}
		}
	}

	err := utilerrors.NewAggregate(errs)
	if err != nil {
		return nil, err
	}

	if len(mergedVolumes) == 0 {
		return nil, nil
	}

	return mergedVolumes, err
}

// applyPodPresetsOnPod updates the PodSpec with merged information from all the
// applicable PodPresets. It ignores the errors of merge functions because merge
// errors have already been checked in safeToApplyPodPresetsOnPod function.
func applyPodPresetsOnPod(logger logr.Logger, pod *corev1.Pod, podPresets []*redhatcopv1alpha1.PodPreset) {
	if len(podPresets) == 0 {
		return
	}

	volumes, _ := mergeVolumes(pod.Spec.Volumes, podPresets)
	pod.Spec.Volumes = volumes
	logger.Info("applyPodPresetsOnPod 1. Volumes merged")

	for i, ctr := range pod.Spec.Containers {
		applyPodPresetsOnContainer(logger, &ctr, podPresets)
		pod.Spec.Containers[i] = ctr
		logger.Info("appliedPodPresetsOnPod 1. on a container:" + ctr.Name)
		for idx, env := range ctr.Env {
			logger.Info("after RETURNING container env on index " + fmt.Sprintf("%d", idx) + " :" + env.Name + ": " + env.Value)
		}

	}

	for i, iCtr := range pod.Spec.InitContainers {
		applyPodPresetsOnContainer(logger, &iCtr, podPresets)
		pod.Spec.InitContainers[i] = iCtr
		logger.Info("appliedPodPresetsOnPod 1. on a init container:" + iCtr.Name)
	}

	// add annotation
	if pod.ObjectMeta.Annotations == nil {
		pod.ObjectMeta.Annotations = map[string]string{}
	}

	for _, pp := range podPresets {
		pod.ObjectMeta.Annotations[fmt.Sprintf("%s/podpreset-%s", annotationPrefix, pp.GetName())] = pp.GetResourceVersion()
	}
}

// applyPodPresetsOnContainer injects envVars, VolumeMounts and envFrom from
// given podPresets in to the given container. It ignores conflict errors
// because it assumes those have been checked already by the caller.
func applyPodPresetsOnContainer(logger logr.Logger, ctr *corev1.Container, podPresets []*redhatcopv1alpha1.PodPreset) {

	for idx, env := range ctr.Env {
		logger.Info("initial container env on index " + fmt.Sprintf("%d", idx) + " :" + env.Name + ": " + env.Value)
	}

	for idx, env := range ctr.Env {
		logger.Info("INITIAL ctr.Env before merging container env on index " + fmt.Sprintf("%d", idx) + " :" + env.Name + ": " + env.Value)
	}

	for _, preset := range podPresets {
		for jdx, env := range (*preset).Spec.Env {
			logger.Info("initial podPresets env of (*preset).Name:" + (*preset).Name + "on index " + fmt.Sprintf("%d", jdx) + " :" + env.Name + ": " + env.Value)
		}
	}

	envVars, _ := mergeEnv(logger, ctr.Env, podPresets)
	ctr.Env = envVars
	for idx, env := range ctr.Env {
		logger.Info("ctr.Env after merging container env on index " + fmt.Sprintf("%d", idx) + " :" + env.Name + ": " + env.Value)
	}

	volumeMounts, _ := mergeVolumeMounts(ctr.VolumeMounts, podPresets)
	ctr.VolumeMounts = volumeMounts
	for idx, vm := range ctr.VolumeMounts {
		logger.Info("ctr.VolumeMounts after merging container mountvols on index " + fmt.Sprintf("%d", idx) + " :" + vm.Name + ": " + vm.MountPath)
	}

	envFrom, _ := mergeEnvFrom(ctr.EnvFrom, podPresets)
	ctr.EnvFrom = envFrom

	for idx, envfrom := range ctr.EnvFrom {
		logger.Info("ctr.VolumeMounts after merging container envfroms on index " + fmt.Sprintf("%d", idx) + " :" + envfrom.String() + ": prefix " + envfrom.Prefix)
	}
}
