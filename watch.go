package main

import (
	"fmt"
	"io/ioutil"
	"strings"

	log "github.com/Sirupsen/logrus"
)

func watchDir(dirname string, srcExtension string, processedExtension string) error {
	allfiles, err := ioutil.ReadDir(dirname)
	if err != nil {
		log.Error("Error in reading directory", err)
		return err
	}

	files := make([]string, 1)
	for _, file := range allfiles {
		if file.Mode().IsRegular() {
			files = append(files, file.Name())
		}
	}
	processedFiles := make([]string, 1)
	for _, file := range files {
		if strings.Contains(file, processedExtension) {
			fName := strings.Split(file, processedExtension)[0]
			processedFiles = append(processedFiles, fName)
		}
	}

	for _, file := range files {
		for _, pfile := range processedFiles {
			if strings.Contains(file, pfile) {
				continue
			}
			if strings.Contains(file, srcExtension) {
				fmt.Println(file)
			}
		}
	}

	return nil
}
