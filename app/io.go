package main

import (
	"fmt"
	"os"
	"path"
)

func readRDBFile() (*string, error) {
	byteContent, err := os.ReadFile(path.Join(configRDB["dir"], configRDB["dbfilename"]))
	if err != nil {
		return nil, err
	}

	rdbContent := fmt.Sprintf("%x", byteContent)
	return &rdbContent, nil
}
