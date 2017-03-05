package main

import (
	"flag"
	"os"
	"time"

	//"github.com/davecgh/go-spew/spew"
	log "github.com/Sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/resource"
	apiv1 "k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	kubeconfig         = flag.String("kubeconfig", "./config", "absolute path to the kubeconfig file")
	dirname            = flag.String("directory", "", "path to the directory to watch")
	sleepInterval      = flag.Int64("sleep-interval", 10, "Sleep interval in seconds")
	srcExtension       = flag.String("source-extension", "mzML", "Source file extension which will be used to process file")
	processedExtension = flag.String("processed-extension", "pep.xml", "Processed file extension which will be used to skip already processed files")
	loglevel           = flag.String("loglevel", "info", "Log level used for printing logs")
	namespace          = flag.String("namespace", "default", "Kubernetes Namespace to manage")
	indexerCPU         = flag.String("indexer-cpu", "500m", "Amout of CPU to give indexer process. It is multiple of 1024 (1 CPU)")
	indexerMemory      = flag.String("indexer-memory", "500Mi", "Amout of memory to give indexer process")
	indexerImg         = flag.String("indexer-image", "gurvin/promec-indexer:0.1", "Container Image name which has Indexer software installed")
	pvcName            = flag.String("pvc-name", "", "Kubernetes Persistent volume claim name which has proteomics data")
	mountPath          = flag.String("mount-path", "/data", "Mount path where PVC will be mounted inside container")
	elsHost            = flag.String("elasticsearch-host", "http://localhost:9200", "Full path with scheme and port to elasticsearch host for indexing pepm xml data")
	indexName          = flag.String("index-name", "promec", "Elasticsearch index name to put pep xml data in")
	uid                = flag.String("uid", "999", "User ID to be used in lauching the comet and indexer jobs")
	gid                = flag.String("gid", "999", "Group ID to be used in lauching the comet and indexer jobs")
)

type Conf struct {
	dirname            string
	srcExtension       string
	processedExtension string
	namespace          string
	indexerCPU         resource.Quantity
	indexerMemory      resource.Quantity
	indexerImg         string
	pvcVol             apiv1.Volume
	volMount           apiv1.VolumeMount
	mountPath          string
	elsHost            string
	indexName          string
	uid                string
	gid                string
}

var conf = new(Conf)

const PromecLabel = "promec-file"
const PromecVolumeName = "promec-data"

func init() {
	// Log as JSON to stderr
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stderr)
}

func setConf() {
	conf.dirname = *dirname
	conf.srcExtension = *srcExtension
	conf.processedExtension = *processedExtension
	conf.namespace = *namespace
	conf.indexerImg = *indexerImg
	conf.elsHost = *elsHost
	conf.indexName = *indexName
	conf.uid = *uid
	conf.gid = *gid

	// Setup the volume from PVC and Volume Mount
	if *pvcName == "" {
		log.Fatal("You must specify the Persistent Volume Claim")
	}
	if *dirname == "" {
		log.Fatal("You must specify the Directory to watch")
	}

	pvcVolSrc := apiv1.VolumeSource{PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{ClaimName: *pvcName}}
	conf.pvcVol = apiv1.Volume{Name: PromecVolumeName, VolumeSource: pvcVolSrc}
	conf.volMount = apiv1.VolumeMount{Name: PromecVolumeName, MountPath: *mountPath}

	var err error
	conf.indexerCPU, err = resource.ParseQuantity(*indexerCPU)
	if err != nil {
		log.Fatal("Failed in parsing comet CPU value ", err)
	}

	conf.indexerMemory, err = resource.ParseQuantity(*indexerMemory)
	if err != nil {
		log.Fatal("Failed in parsing comet Memory value ", err)
	}
}

func main() {
	flag.Parse()

	// Set up correct log level
	lvl, err := log.ParseLevel(*loglevel)
	if err != nil {
		log.WithFields(log.Fields{
			"detail": err,
		}).Warn("Could not parse log level, using default")
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(lvl)
	}

	// Set up the config struct
	setConf()

	interval := time.Duration(*sleepInterval)
	// creates the in-cluster config
	kubeConf, err := rest.InClusterConfig()
	if err != nil {
		// May be we are running outside cluster
		kubeConf, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			log.Fatal("Failed to create config for API server ", err)
		}
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(kubeConf)
	if err != nil {
		panic(err.Error())
	}
	log.Info("Promec-Watcher started and connected to kubernetes API server")

	opts := apiv1.ListOptions{}
	opts.LabelSelector = PromecLabel
	for {

		//Get the files which are processed yet
		files, err := watchDir()
		if err != nil {
			log.Error("Error in watching directory ", err)
			// Sleep predfined interval and retry
			time.Sleep(interval * time.Second)
			continue
		}

		// Get the list of jobs which is launched by us in our namespace
		jobs, err := clientset.BatchV1().Jobs(conf.namespace).List(opts)
		if err != nil {
			log.Error("Failed in getting jobs ", err.Error())
			// Sleep predfined interval and retry
			time.Sleep(interval * time.Second)
			continue
		}

		err = launchJobs(jobs, files, clientset)
		if err != nil {
			log.Error("Failed in launching job ", err)
		}

		time.Sleep(interval * time.Second)
	}
}
