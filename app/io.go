package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
)

func readRDBFile() (*string, error) {
	file, err := os.Open(path.Join(config["dir"], config["dbfilename"]))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	rdbContents := ""
	for scanner.Scan() {
		rdbContents += fmt.Sprintf("%x", scanner.Bytes())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return &rdbContents, nil
}

func addKey(key, value string) (bool, error) {
	return true, nil
}

func deleteKey(key string) (bool, error) {
	// open rdb file
	out, err := os.Open(path.Join(config["dir"], config["dbfilename"]))
	if err != nil {
		return false, err
	}
	defer out.Close()

	// create temp file
	in, err := os.CreateTemp(os.TempDir(), "redis-codecrafters")
	if err != nil {
		return false, err
	}
	defer in.Close()

	// create the scanner
	scanner := bufio.NewScanner(out)

	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}

	// loop through the scanner:
	// - if you find the hashmap size: record the current position ((*File).Seek(0, io.SeekCurrent))
	// - if you find the key-to-be-deleted: attempt to delete it and its value, and if successful, go back to the recorded position and decrement the hashmap size

	// move the temp file to the original rdb file

	// delete the temp file

	return true, nil
}
