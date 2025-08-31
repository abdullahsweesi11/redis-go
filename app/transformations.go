package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"unicode"
)

func parseFlags() {
	dir := flag.String("dir", "", "Path to RDB file directory")
	dbFilename := flag.String("dbfilename", "", "RDB file name")
	port := flag.String("port", "", "Redis server port")
	master := flag.String("replicaof", "", "Master host and port")

	flag.Parse()

	configRDB["dir"] = *dir
	configRDB["dbfilename"] = *dbFilename

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

func parseRESP(RESPContent []byte) ([][]string, []int, error) {
	// first parameter represents the decoded text
	// second parameter represents the purpose of text each one is
	//   (0 for RDB file contents, 1 for command, 2 for other)

	contentLength := len(RESPContent)
	results := [][]string{}
	RESPTypes := []int{} // representing the type of each RESP segment (e.g. simple string, bulk string, bulk array)
	purposes := []int{}  // representing the purpose of each text

	i := 0
	for i < contentLength-1 {
		RESPType, err := categoriseRESPType(string(RESPContent[i]))
		if err != nil {
			return nil, nil, err
		}

		switch RESPType {
		case 0:
			j := i + 1
			for j < len(RESPContent)-1 && RESPContent[j] != 13 && RESPContent[j+1] != 10 {
				j++
			}

			results = append(results, []string{string(RESPContent[i+1 : j])})
			RESPTypes = append(RESPTypes, 0)
			purposes = append(purposes, 2)

			i = j + 2
		case 1:
			j := i + 1

			for j < len(RESPContent) && unicode.IsDigit(rune(RESPContent[j])) {
				j++
			}

			lengthStr := string(RESPContent[i+1 : j])
			length, err := strconv.Atoi(lengthStr)
			if err != nil {
				fmt.Println("Problem: error thrown when parsing Redis array (1)")
			}

			i += 3 + len(lengthStr)

			results = append(results, []string{string(RESPContent[i : i+length])})
			RESPTypes = append(RESPTypes, 1)
			purposes = append(purposes, 0)

			i += length
			if i < len(RESPContent)-1 && RESPContent[i] == 13 && RESPContent[i+1] == 10 {
				i += 2
			}
		case 2:
			j := i + 1

			for j < len(RESPContent) && unicode.IsDigit(rune(RESPContent[j])) {
				j++
			}

			lengthStr := string(RESPContent[i+1 : j])
			length, err := strconv.Atoi(lengthStr)
			if err != nil {
				fmt.Println("Problem: error thrown when parsing Redis array (1)")
				os.Exit(1)
			}

			i += 3 + len(lengthStr)

			result := []string{}
			for range length {
				k := i + 1
				for k < len(RESPContent) && unicode.IsDigit(rune(RESPContent[k])) {
					k++
				}

				wordLengthStr := string(RESPContent[i+1 : k])
				wordLength, err := strconv.Atoi(wordLengthStr)
				if err != nil {
					fmt.Println("Problem: error thrown when parsing Redis array (2)")
					os.Exit(1)
				}

				i += 3 + len(wordLengthStr)

				result = append(result, string(RESPContent[i:i+wordLength]))

				i += 2 + wordLength
			}

			results = append(results, result)
			RESPTypes = append(RESPTypes, 2)
			purposes = append(purposes, 1)
		default:
			return nil, nil, errors.New("first character is not a valid option")
		}
	}

	return results, purposes, nil
}

func categoriseRESPType(firstLetter string) (int, error) {
	if len(firstLetter) != 1 {
		return -1, errors.New("argument must be a single character")
	}

	switch firstLetter {
	case "+":
		return 0, nil
	case "$":
		return 1, nil
	case "*":
		return 2, nil
	default:
		fmt.Println(firstLetter)
		return -1, errors.New("first character is not a valid option")
	}
}

// func parseRedisArray(RESPArray []byte) ([][]string, []int) {
// 	// first parameter represents the decoded text
// 	// second parameter represents the type of text each one is (0 for RDB file contents, 1 for command)

// 	segmentLength := len(RESPArray)
// 	results := [][]string{}
// 	types := []int{}

// 	i := 0 // array pointers
// 	for i < segmentLength-1 {
// 		j := i + 1

// 		for j < len(RESPArray) && unicode.IsDigit(rune(RESPArray[j])) {
// 			j++
// 		}

// 		lengthStr := string(RESPArray[i+1 : j])
// 		length, err := strconv.Atoi(lengthStr)
// 		if err != nil {
// 			fmt.Println("Problem: error thrown when parsing Redis array (1)")
// 			os.Exit(1)
// 		}

// 		i += 3 + len(lengthStr) // shift right from `[length]\r\n`

// 		result := []string{}
// 		for range length {
// 			k := i + 1
// 			for k < len(RESPArray) && unicode.IsDigit(rune(RESPArray[k])) {
// 				k++
// 			}

// 			wordLengthStr := string(RESPArray[i+1 : k])
// 			wordLength, err := strconv.Atoi(wordLengthStr)
// 			if err != nil {
// 				fmt.Println("Problem: error thrown when parsing Redis array (2)")
// 				os.Exit(1)
// 			}

// 			i += 3 + len(wordLengthStr)
// 			end := i + wordLength

// 			element := RESPArray[i:end]
// 			result = append(result, string(element))

// 			i = end + 2
// 		}

// 		results = append(results, result)
// 	}

// 	return results
// }

func sliceEquals(first, second []string) bool {
	return reflect.DeepEqual(first, second)
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

func encodeInteger(num int) []byte {
	result := fmt.Sprintf(":%d\r\n", num)
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

func toggleEndianBinary(input string) (string, error) {
	// toggles between little-endian and big-endian for hex strings
	if len(input)%8 != 0 {
		toBePrepended := ""
		for len(input)%8 != 0 {
			toBePrepended += "0"
		}
		input = toBePrepended + input
	}

	output := ""
	for i := len(input) - 8; i >= 0; i -= 8 {
		output += input[i : i+8]
	}

	return output, nil
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
