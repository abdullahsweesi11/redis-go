package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
)

const EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+APsAAP+Z/8ky5l9PGw=="

func readRDBFile() (*string, error) {
	name, ok := config.GetRDBString("name")
	if !ok {
		return nil, errors.New("Could not find RDB file name in config")
	}

	byteContent, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}

	rdbContent := fmt.Sprintf("%x", byteContent)
	return &rdbContent, nil
}

func writeRDBFile(content []byte) (bool, error) {
	name, ok := config.GetRDBString("name")
	if !ok {
		return false, errors.New("Could not find RDB file name in config")
	}

	file, openErr := os.Create(name)
	if openErr != nil {
		return false, openErr
	}
	_, writeErr := file.Write(content)
	if writeErr != nil {
		return false, writeErr
	}

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
