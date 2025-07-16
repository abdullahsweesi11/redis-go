package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"path"
)

const EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

func readRDBFile() (*string, error) {
	byteContent, err := os.ReadFile(path.Join(configRDB["dir"], configRDB["dbfilename"]))
	if err != nil {
		return nil, err
	}

	rdbContent := fmt.Sprintf("%x", byteContent)
	return &rdbContent, nil
}

func getEmptyRDBFile() []byte {
	emptyRDB, err := base64.StdEncoding.DecodeString(EMPTY_RDB_BASE64)
	if err != nil {
		fmt.Println("Problem: could not extract empty RDB binary")
		os.Exit(1)
	}

	return emptyRDB
}
