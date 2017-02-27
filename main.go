package main

import (
	"flag"
	"os"
	"time"

	//"github.com/davecgh/go-spew/spew"
	"k8s.io/client-go/kubernetes"
	//metav1 "k8s.io/client-go/pkg/api/v1"
	log "github.com/Sirupsen/logrus"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	kubeconfig         = flag.String("kubeconfig", "./config", "absolute path to the kubeconfig file")
	dirname            = flag.String("directory", ".", "path to the directory to watch")
	sleepInterval      = flag.Int64("sleep-interval", 10, "Sleep interval in seconds")
	srcExtension       = flag.String("source-extension", "mzML", "Source file extension which will be used to process file")
	processedExtension = flag.String("processed-extension", "pep.xml", "Processed file extension which will be used to skip already processed files")
)

func init() {
	// Log as JSON to stderr
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stderr)
}

func main() {
	flag.Parse()
	interval := time.Duration(*sleepInterval)
	log.Info("Kube-Promec started")
	// uses the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	_, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	for {
		watchDir(*dirname, *srcExtension, *processedExtension)
		time.Sleep(interval * time.Second)
	}
}
