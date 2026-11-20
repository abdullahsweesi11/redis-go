package utils

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"unicode"
)

func ParseRESP(RESPContent []byte) ([][]string, []int, []int, error) {
	// first parameter represents the decoded text
	// second parameter represents the number of bytes used
	// third parameter represents the purpose of text each one is
	//   (0 for RDB file contents, 1 for command, 2 for other)

	contentLength := len(RESPContent)
	results := [][]string{}
	consumed := []int{}
	purposes := []int{} // representing the purpose of each text

	i := 0
	for i < contentLength-1 {
		startIndex := i

		RESPType, err := CategoriseRESPType(string(RESPContent[i]))
		if err != nil {
			return nil, nil, nil, err
		}

		switch RESPType {
		case 0:
			j := i + 1
			for j < len(RESPContent)-1 && !(RESPContent[j] == 13 && RESPContent[j+1] == 10) {
				j++
			}

			results = append(results, []string{string(RESPContent[i+1 : j])})
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
			purposes = append(purposes, 1)
		default:
			return nil, nil, nil, errors.New("first character is not a valid option")
		}

		consumed = append(consumed, i-startIndex)
	}

	return results, consumed, purposes, nil
}

func CategoriseRESPType(firstLetter string) (int, error) {
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

func SliceEquals(first, second []string) bool {
	return reflect.DeepEqual(first, second)
}

func EncodeSimpleString(output string) []byte {
	result := fmt.Sprintf("+%s\r\n", output)
	return []byte(result)
}

func EncodeBulkString(output string) []byte {
	result := fmt.Sprintf("$%d\r\n%s\r\n", len(output), output)
	return []byte(result)
}

func EncodeBulkArray(output []string) []byte {
	result := fmt.Sprintf("*%d\r\n", len(output))

	for _, o := range output {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(o), o)
	}

	return []byte(result)
}

func EncodeInteger(num int) []byte {
	result := fmt.Sprintf(":%d\r\n", num)
	return []byte(result)
}

func EncodeRDBFile(length int, content []byte) []byte {
	result := append(fmt.Appendf(nil, "$%d\r\n", length), content...)
	return result
}

func NullBulkString() []byte {
	result := "$-1\r\n"
	return []byte(result)
}

func EncodeSimpleError(message string) []byte {
	result := fmt.Sprintf("-ERR %s\r\n", message)
	return []byte(result)
}

func EncodeBulkError(message string) []byte {
	result := fmt.Sprintf("!%d\r\nERR %s\r\n", len(message), message)
	return []byte(result)
}

func ToggleEndianBinary(input string) (string, error) {
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

func ToggleEndianHex(input string) (string, error) {
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

func RandomAlphanumGenerator(length int) string {
	// Note: a truly random seed would be used in production
	characters := "abcdefghijklmnopqrstuvwxyz0123456789"
	resultBytes := make([]byte, length)
	for i := range resultBytes {
		resultBytes[i] = characters[rand.Intn(len(characters))]
	}

	return string(resultBytes)
}
