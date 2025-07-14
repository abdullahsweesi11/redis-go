package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"unicode"
)

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

func nullBulkString() []byte {
	result := "$-1\r\n"
	return []byte(result)
}

func extractMap(fileEncoding string) map[string]string {
	dataLength := len(fileEncoding)
	results := map[string]string{}
	// fmt.Println(fileEncoding)
	for i := 0; i < dataLength; i += 2 {
		if fileEncoding[i:i+2] != "fe" {
			continue
		}

		// fmt.Println(fileEncoding[i:])

		if i+4 > dataLength || fileEncoding[i+2:i+4] != "00" {
			fmt.Println("Problem: expected database index to be 0")
			os.Exit(1)
		}

		i += 4

		if i+2 > dataLength || fileEncoding[i:i+2] != "fb" {
			fmt.Printf("Problem: could not find the `fb` flag (at index %d)", i)
			os.Exit(1)
		}

		if i+4 > dataLength {
			fmt.Println("Problem: could not find hashmap")
			os.Exit(1)
		}

		i += 6

		entities := []string{"key", "value"}

		for fileEncoding[i:i+2] == "00" {

			if i+2 > dataLength {
				fmt.Println("Problem: expected value encoding to be string (00)")
				os.Exit(1)
			}

			i += 2

			var key string
			var value string

			for _, entity := range entities {
				if i+2 > dataLength {
					fmt.Printf("Problem: could not find %s length", entity)
					os.Exit(1)
				}

				entityLengthInt64, err := strconv.ParseInt(fileEncoding[i:i+2], 16, 64)
				if err != nil {
					fmt.Printf("Problem: error thrown while parsing %s length", entity)
					os.Exit(1)
				}

				entityLength := int(entityLengthInt64)
				// fmt.Println(entityLength)
				i += 2

				if i+(2*entityLength) > dataLength {
					// fmt.Println(i, entityLength, dataLength)
					fmt.Printf("Problem: could not find %s data", entity)
					os.Exit(1)
				}

				entityBytes, err := hex.DecodeString(fileEncoding[i : i+(2*entityLength)])
				if err != nil {
					fmt.Printf("Problem: error thrown while parsing %s", entity)
					os.Exit(1)
				}

				if entity == "key" {
					key = string(entityBytes)
				} else {
					value = string(entityBytes)
				}
				i += 2 * entityLength
			}

			results[key] = value
		}

		break
	}

	return results
}
