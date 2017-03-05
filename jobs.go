package main

import (
	"strings"

	log "github.com/Sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	apiv1 "k8s.io/client-go/pkg/api/v1"
	batchv1 "k8s.io/client-go/pkg/apis/batch/v1"
)

func launchJobs(jobs *batchv1.JobList, files []string, clientset *kubernetes.Clientset) error {
	launchedFiles := make(map[string]struct{})

	for _, job := range jobs.Items {
		launchedFiles[job.ObjectMeta.Labels["promec-file"]] = struct{}{}

		// Get all the pods we lauched through jobs and see if there is any pending
		pods, err := clientset.CoreV1().Pods(conf.namespace).List(apiv1.ListOptions{LabelSelector: PromecLabel})
		if err != nil {
			log.Error("Failed in getting pods ", err.Error())
			return err
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase == apiv1.PodPending {
				log.Warn(pod.ObjectMeta.Name, " is pending, most likely resource limit has been reached. Will wait before scheduling new jobs")
				return nil
			}
		}

		if job.Status.Succeeded == 1 {
			log.Debug("Job: " + job.ObjectMeta.Name + " is completed and processed file: " + job.ObjectMeta.Labels["promec-file"])
			continue
		} else if job.Status.Active == 1 {
			log.Debug("Job: " + job.ObjectMeta.Name + " is running and processing file: " + job.ObjectMeta.Labels["promec-file"])
			continue
		}
	}

	for _, file := range files {
		// Skip the files which are already processed/processing
		if _, ok := launchedFiles[file]; !ok {
			job, err := clientset.BatchV1().Jobs(conf.namespace).Create(getJob(file))
			if err != nil {
				log.Error("Failed in scheduling job ", err)
				return err
			}
			log.Info("Launched job: ", job.ObjectMeta.Name, " to process file ", file)
		}
	}

	return nil
}

func getJob(file string) *batchv1.Job {
	baseFile := strings.Split(file, "."+conf.srcExtension)

	objectMeta := apiv1.ObjectMeta{
		Name:      "comet-indexer-" + strings.Replace(baseFile[0], "_", "-", -1),
		Namespace: conf.namespace,
		Labels: map[string]string{
			PromecLabel: file,
		},
	}
	template := apiv1.PodTemplateSpec{
		ObjectMeta: objectMeta,
		Spec:       getPodSpec(file, baseFile[0]),
	}
	jobSpec := batchv1.JobSpec{
		Template: template,
	}

	job := batchv1.Job{
		ObjectMeta: objectMeta,
		Spec:       jobSpec,
	}

	return &job
}

func getPodSpec(file string, baseFile string) apiv1.PodSpec {

	cometParamsEnv := apiv1.EnvVar{Name: "COMET_PARAMS", Value: conf.dirname + "/" + baseFile + ".params"}
	cometFileEnv := apiv1.EnvVar{Name: "COMET_INPUT_FILE", Value: conf.dirname + "/" + file}
	directoryEnv := apiv1.EnvVar{Name: "INPUT_DIRECTORY", Value: conf.dirname}
	uidEnv := apiv1.EnvVar{Name: "UID", Value: conf.uid}
	gidEnv := apiv1.EnvVar{Name: "GID", Value: conf.gid}

	// Comet container
	cometContainer := apiv1.Container{
		Name:            "comet",
		Image:           conf.cometImg,
		ImagePullPolicy: apiv1.PullIfNotPresent,
		Command:         []string{"/bin/comet.sh"},
		Resources:       apiv1.ResourceRequirements{Requests: apiv1.ResourceList{"cpu": conf.cometCPU, "memory": conf.cometMemory}},
		VolumeMounts:    []apiv1.VolumeMount{conf.volMount},
		Env:             []apiv1.EnvVar{cometParamsEnv, cometFileEnv, directoryEnv, uidEnv, gidEnv},
	}

	// Indexer container
	indexContainer := apiv1.Container{
		Name:            "indexer",
		Image:           conf.indexerImg,
		ImagePullPolicy: apiv1.PullIfNotPresent,
		Args:            []string{"-pepxml=" + conf.dirname + "/" + baseFile + "." + conf.processedExtension, "-host=" + conf.elsHost, "-index=" + conf.indexName},
		Resources:       apiv1.ResourceRequirements{Requests: apiv1.ResourceList{"cpu": conf.indexerCPU, "memory": conf.indexerMemory}},
		Env:             []apiv1.EnvVar{uidEnv, gidEnv},
		VolumeMounts:    []apiv1.VolumeMount{conf.volMount},
	}

	// Create the Job POD
	podSpec := apiv1.PodSpec{
		RestartPolicy: apiv1.RestartPolicyOnFailure,
		Containers:    []apiv1.Container{cometContainer, indexContainer},
		Volumes:       []apiv1.Volume{conf.pvcVol},
	}

	return podSpec
}
