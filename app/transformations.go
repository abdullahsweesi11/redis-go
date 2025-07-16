package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
	"unicode"
)

type keyValuePair struct {
	Key    string
	Value  string
	Expiry *expiry
}

func parseRedisArray(RESPArray []byte) []string {
	// get length of array

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

func extractMap(fileEncoding string) (map[string]string, map[string]*expiry) {
	dataLength := len(fileEncoding)
	var intermediateResults []keyValuePair
	for i := 0; i < dataLength; i += 2 {
		if fileEncoding[i:i+2] != "fe" {
			continue
		}

		if i+4 > dataLength || fileEncoding[i+2:i+4] != "00" {
			fmt.Println("Problem: expected database index to be 0")
			os.Exit(1)
		}
		i += 4

		if i+2 > dataLength || fileEncoding[i:i+2] != "fb" {
			fmt.Printf("Problem: could not find the `fb` flag (at index %d)", i)
			os.Exit(1)
		}
		if i+6 > dataLength {
			fmt.Println("Problem: could not find hashmap metadata")
			os.Exit(1)
		}
		i += 6

		// remove ff part
		pairs := fileEncoding[i : dataLength-18]

		intermediateResults = getPairs(pairs)

		break
	}

	results := map[string]string{}
	expiryResults := map[string]*expiry{}

	for _, r := range intermediateResults {
		results[r.Key] = r.Value
		if r.Expiry != nil {
			expiryResults[r.Key] = r.Expiry
		}
	}
	return results, expiryResults
}

func getPairs(pairs string) []keyValuePair {
	dataLength := len(pairs)
	entities := []string{"key", "value"}
	results := []keyValuePair{}

	i := 0
	for i < dataLength {
		if i+2 > dataLength {
			fmt.Println("Problem: could not find start of key-value pair")
			os.Exit(1)
		}
		var pair keyValuePair

		switch pairs[i : i+2] {
		case "fc":
			// timestamp in milliseconds
			i += 2
			if i+16 > dataLength {
				fmt.Println("Problem: could not find expiry timestamp")
				os.Exit(1)
			}

			timestampHex, convertErr := convertLEHex(pairs[i : i+16])
			if convertErr != nil {
				fmt.Printf("Problem: error thrown while converting timestamp from little endian")
				os.Exit(1)
			}
			timestampUnixMilli, err := strconv.ParseInt(timestampHex, 16, 64)
			if err != nil {
				fmt.Printf("Problem: error thrown while parsing expiry timestamp")
				os.Exit(1)
			}

			timestamp := time.UnixMilli(timestampUnixMilli)
			pair.Expiry = &expiry{timestamp}
			i += 16
		}
		if i+2 > dataLength || pairs[i:i+2] != "00" {
			fmt.Println("Problem: expected value type to be string")
			os.Exit(1)
		}

		i += 2

		for _, entity := range entities {
			if i+2 > dataLength {
				fmt.Printf("Problem: could not find %s length", entity)
				os.Exit(1)
			}

			entityLengthInt64, err := strconv.ParseInt(pairs[i:i+2], 16, 64)
			if err != nil {
				fmt.Printf("Problem: error thrown while parsing %s length", entity)
				os.Exit(1)
			}

			entityLength := int(entityLengthInt64)
			// fmt.Println(entityLength)
			i += 2

			if i+(2*entityLength) > dataLength {
				fmt.Printf("Problem: could not find %s data", entity)
				os.Exit(1)
			}

			entityBytes, err := hex.DecodeString(pairs[i : i+(2*entityLength)])
			if err != nil {
				fmt.Printf("Problem: error thrown while parsing %s", entity)
				os.Exit(1)
			}

			if entity == "key" {
				pair.Key = string(entityBytes)
			} else {
				pair.Value = string(entityBytes)
			}
			i += 2 * entityLength
		}

		results = append(results, pair)
	}

	return results
}

func convertLEHex(input string) (string, error) {
	if len(input)%2 != 0 {
		return "", errors.New("input HEX code must represent a whole number of bytes")
	}

	output := ""
	for i := len(input) - 2; i >= 0; i -= 2 {
		output += input[i : i+2]
	}

	return output, nil
}
