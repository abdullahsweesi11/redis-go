package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"hash/crc64"
	"os"
	"path"
	"strconv"
	"time"
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

type keyValuePair struct {
	Key       string
	Value     string
	ExpiryPtr *expiry
}

func getChecksum(fileEncoding string) string {
	table := crc64.MakeTable(crc64.ISO)
	code, err := hex.DecodeString(fileEncoding)
	if err != nil {
		fmt.Println(err)
	}
	checksum := crc64.Checksum(code, table)

	var buffer bytes.Buffer

	binary.Write(&buffer, binary.LittleEndian, checksum)
	newChecksumBytes := buffer.Bytes()
	return fmt.Sprintf("%8x", newChecksumBytes)
}

func storeInMap(fileEncoding, key, val string, expiryPtr *expiry) bool {
	// assumes key doesn't already exist in hashmap
	dataLength := len(fileEncoding)
	insertionIndex := dataLength - 18
	insertion := ""

	// TODO: increment 'fb' op code

	if expiryPtr != nil {
		insertion += "fc"
		insertion += fmt.Sprintf("%x", (*expiryPtr).Timestamp)
	}

	// value of type string
	insertion += "00"

	// insert key
	insertion += fmt.Sprintf("%2x", len([]byte(key)))
	insertion += fmt.Sprintf("%x", []byte(key))

	// insert value
	insertion += fmt.Sprintf("%2x", len([]byte(val)))
	insertion += fmt.Sprintf("%x", []byte(val))

	// remove old end-of-file checksum
	payload := fileEncoding[:insertionIndex] + insertion
	// compute new end-of-file checksum
	checksum := getChecksum(payload)

	success, err := writeRDBFile([]byte(payload + "ff" + checksum))
	if !success {
		fmt.Println("Problem: RDB file could not be written")
		fmt.Println(err)
		return false
	}

	return true
}

func getFromMap(fileEncoding, key string) []byte {
	hashmap, expiries := extractMap(fileEncoding)
	fmt.Println(hashmap)

	value, valueExists := hashmap[key]
	if !valueExists {
		return nullBulkString()
	}

	expiryPtr, expiryPtrExists := expiries[key]
	if expiryPtrExists && expiryPtr != nil && time.Now().Compare((*expiryPtr).Timestamp) > 0 {
		return nullBulkString()
	}

	return []byte(value)
}

func extractMap(fileEncoding string) (map[string]string, map[string]*expiry) {
	dataLength := len(fileEncoding)
	var intermediateResults []keyValuePair
	for i := 0; i < dataLength; i += 2 {
		if fileEncoding[i:i+2] != "fe" {
			continue
		}

		if i+4 > dataLength || fileEncoding[i+2:i+4] != "00" {
			fmt.Println("Warning: expected database index to be 0, terminating early")
			break
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

		// remove end of file
		pairs := fileEncoding[i : dataLength-18]

		intermediateResults = getPairs(pairs)

		break
	}

	results := map[string]string{}
	expiryResults := map[string]*expiry{}

	for _, r := range intermediateResults {
		results[r.Key] = r.Value
		if r.ExpiryPtr != nil {
			expiryResults[r.Key] = r.ExpiryPtr
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

			// switch from little-endian hex to big-endian hex
			timestampHex, convertErr := toggleEndianHex(pairs[i : i+16])
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
			pair.ExpiryPtr = &expiry{timestamp}
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
