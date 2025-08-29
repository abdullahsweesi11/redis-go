package main

import (
	"encoding/base64"
	"fmt"
	"os"
)

const EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+APsAAP+Z/8ky5l9PGw=="

func readRDBFile() (*string, error) {
	byteContent, err := os.ReadFile(configRDB["name"])
	if err != nil {
		return nil, err
	}

	rdbContent := fmt.Sprintf("%x", byteContent)
	return &rdbContent, nil
}

func writeRDBFile(content []byte) (bool, error) {
	file, openErr := os.Create(configRDB["name"])
	if openErr != nil {
		return false, openErr
	}
	_, writeErr := file.Write(content)
	if writeErr != nil {
		return false, writeErr
	}

	// contents, _ := readRDBFile()
	// fmt.Println(*contents)
	return true, nil
}

func getEmptyRDBFile() []byte {
	emptyRDB, err := base64.StdEncoding.DecodeString(EMPTY_RDB_BASE64)
	if err != nil {
		fmt.Println("Problem: could not extract empty RDB binary")
		os.Exit(1)
	}

	return emptyRDB
}
