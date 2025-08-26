package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"unicode"
)

func parseFlags() {
	dir := flag.String("dir", "", "Path to RDB file directory")
	dbFilename := flag.String("dbfilename", "", "RDB file name")
	port := flag.String("port", "", "Redis server port")
	master := flag.String("replicaof", "", "Master host and port")

	flag.Parse()

	// Set RDB config data
	if *dir == "" || *dbFilename == "" {
		// should choose a different home, since it isn't a temporary file
		rDBFile, err := os.Create("/tmp/dump.rdb")
		if err != nil {
			fmt.Println("Problem: could not create an RDB file")
			os.Exit(1)
		}
		defer rDBFile.Close()
		configRDB["name"] = rDBFile.Name()

		writeBuffer := getEmptyRDBFile()
		_, err = rDBFile.Write(writeBuffer)
		if err != nil {
			fmt.Println("Problem: could not initialise an empty RDB file")
			os.Exit(1)
		}
	} else {
		configRDB["name"] = path.Join(*dir, *dbFilename)
	}

	// Set replication config data
	if *port == "" {
		configRepl["port"] = "6379"
	} else {
		configRepl["port"] = *port
	}
	configRepl["master"] = *master
}

func parseRedisArray(RESPArray []byte) []string {

	i := 0 // array pointers
	j := i + 1

	for j < len(RESPArray) && unicode.IsDigit(rune(RESPArray[j])) {
		j++
	}

	lengthStr := string(RESPArray[i+1 : j])
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		fmt.Println("Problem: error thrown when parsing Redis array (1)")
		os.Exit(1)
	}

	i += 3 + len(lengthStr) // shift right from `*[length]\r\n`

	result := []string{}
	for range length {
		// get length of element
		k := i + 1
		for k < len(RESPArray) && unicode.IsDigit(rune(RESPArray[k])) {
			k++
		}

		wordLengthStr := string(RESPArray[i+1 : k])
		wordLength, err := strconv.Atoi(wordLengthStr)
		if err != nil {
			fmt.Println("Problem: error thrown when parsing Redis array (2)")
			os.Exit(1)
		}

		i += 3 + len(wordLengthStr)
		end := i + wordLength

		element := RESPArray[i:end]
		result = append(result, string(element))

		i = end + 2
	}

	return result
}

func encodeSimpleString(output string) []byte {
	result := fmt.Sprintf("+%s\r\n", output)
	return []byte(result)
}

func encodeBulkString(output string) []byte {
	result := fmt.Sprintf("$%d\r\n%s\r\n", len(output), output)
	return []byte(result)
}

func encodeBulkArray(output []string) []byte {
	result := fmt.Sprintf("*%d\r\n", len(output))

	for _, o := range output {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(o), o)
	}

	return []byte(result)
}

func encodeRDBFile(length int, content []byte) []byte {
	result := append(fmt.Appendf(nil, "$%d\r\n", length), content...)
	return result
}

func nullBulkString() []byte {
	result := "$-1\r\n"
	return []byte(result)
}

func toggleEndianHex(input string) (string, error) {
	// toggles between little-endian and big-endian for hex strings
	if len(input)%2 != 0 {
		return "", errors.New("input HEX code must represent a whole number of bytes")
	}

	output := ""
	for i := len(input) - 2; i >= 0; i -= 2 {
		output += input[i : i+2]
	}

	return output, nil
}
