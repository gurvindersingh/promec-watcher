package main

import (
	"io/ioutil"
	"strings"

	log "github.com/Sirupsen/logrus"
)

func watchDir() ([]string, error) {
	allfiles, err := ioutil.ReadDir(conf.dirname)
	if err != nil {
		log.Error("Error in reading directory", err)
		return nil, err
	}

	var files []string
	for _, file := range allfiles {
		if file.Mode().IsRegular() {
			files = append(files, file.Name())
		}
	}
	var processedFiles []string
	for _, file := range files {
		if strings.Contains(file, conf.processedExtension) {
			fName := strings.Split(file, conf.processedExtension)[0]
			processedFiles = append(processedFiles, fName)
		}
	}

	var newFiles []string

	for _, file := range files {
		for _, pfile := range processedFiles {
			if strings.Contains(file, pfile) {
				continue
			}
			if strings.Contains(file, conf.srcExtension) {
				newFiles = append(newFiles, file)
			}
		}
	}

	return newFiles, nil
}
