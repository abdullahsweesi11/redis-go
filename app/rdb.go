package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc64"
	"math"
	"os"
	"strconv"
	"time"
	"unicode"
)

type LengthEncodingType int

const (
	remaining6Bits LengthEncodingType = iota
	remaining14Bits
	remaining4CompleteBytes
	specialCase
)

func encodeValue(value string) (string, error) {
	length := len([]byte(value))

	encodedLength, err := encodeLength(length)
	encodedValue := fmt.Sprintf("%x", []byte(value))

	return encodedLength + encodedValue, err
}

func getLengthEncodingType(firstByte byte) LengthEncodingType {
	prefix := int(firstByte >> 6)
	switch prefix {
	case 0:
		return remaining6Bits
	case 1:
		return remaining14Bits
	case 2:
		return remaining4CompleteBytes
	case 3:
		return specialCase
	default:
		return -1
	}
}

func decodeValue(encoding []byte) (value string, bytesRead int, err error) {
	lengthEncodingType := getLengthEncodingType(encoding[0])

	switch lengthEncodingType {
	case remaining6Bits:
		length := int(encoding[0] & 0x3F)
		return string(encoding[1 : 1+length]), 1 + length, nil
	case remaining14Bits:
		length := int(int(int(encoding[0]&0x3F)<<8) | int(encoding[1]))
		return string(encoding[2 : 2+length]), 2 + length, nil
	case remaining4CompleteBytes:
		length := int(binary.BigEndian.Uint32(encoding[1:5]))
		return string(encoding[5 : 5+length]), 5 + length, nil
	case specialCase:
		specialCaseType := int(encoding[0] & 0x3F)
		valueBytes := encoding[1 : 1+int(math.Pow(2, float64(specialCaseType)))]
		for i, j := 0, len(valueBytes)-1; i < j; i, j = i+1, j-1 {
			valueBytes[i], valueBytes[j] = valueBytes[j], valueBytes[i]
		}
		var valueInt int32
		for _, b := range valueBytes {
			valueInt = (valueInt << 8) | int32(b)
		}
		return string(valueInt), 1 + int(math.Pow(2, float64(specialCaseType))), nil
	default:
		return "", 0, errors.New("could not determine length encoding type")
	}
}

func encodeLength(length int) (string, error) {
	if length < (1 << 6) {
		return fmt.Sprintf("%02x", length), nil
	} else if length < (1 << 14) {
		binaryLength := fmt.Sprintf("%014b", length)
		decimalLength, err := strconv.ParseInt("01"+binaryLength, 2, 64)
		if err != nil {
			return "", errors.New("could not convert binary length encoding into decimal (1)")
		}
		return fmt.Sprintf("%04x", decimalLength), nil
	} else if uint64(length) < (1 << 32) {
		binaryLength, binaryErr := toggleEndianBinary(fmt.Sprintf("%032b", length))
		if binaryErr != nil {
			return "", errors.New("could not convert from big-endian to little-endian")
		}
		decimalLength, err := strconv.ParseInt("10000000"+binaryLength, 2, 64)
		if err != nil {
			return "", errors.New("could not convert binary length encoding into decimal (2)")
		}
		return fmt.Sprintf("%010x", decimalLength), nil
	}

	return "", errors.New("value length is too large")
}

func decodeLength(encoding []byte) (int, error) {
	lengthEncodingType := getLengthEncodingType(encoding[0])

	switch lengthEncodingType {
	case remaining6Bits:
		return int(encoding[0] & 0x3F), nil
	case remaining14Bits:
		return int(int(int(encoding[0]&0x3F)<<8) | int(encoding[1])), nil
	case remaining4CompleteBytes:
		return int(binary.BigEndian.Uint32(encoding[1:5])), nil
	case specialCase:
		specialCaseType := int(encoding[0] & 0x3F)
		valueBytes := encoding[1 : 1+int(math.Pow(2, float64(specialCaseType)))]
		for i, j := 0, len(valueBytes)-1; i < j; i, j = i+1, j-1 {
			valueBytes[i], valueBytes[j] = valueBytes[j], valueBytes[i]
		}
		var valueInt int32
		for _, b := range valueBytes {
			valueInt = (valueInt << 8) | int32(b)
		}
		return len(string(valueInt)), nil
	default:
		return 0, errors.New("could not determine length encoding type")
	}
}

func parseRDBFile(fileBinary []byte) []byte {
	i := 0
	j := i + 1

	for j < len(fileBinary) && unicode.IsDigit(rune(fileBinary[j])) {
		j++
	}

	lengthStr := string(fileBinary[i+1 : j])
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		fmt.Println("Problem: error thrown when parsing Redis array (1)")
	}
	i += 3 + len(lengthStr)
	return fileBinary[i : i+length]
}

func readRDBHeader(encoding []byte) (string, int, error) {
	i := 0
	for fmt.Sprintf("%02x", encoding[i]) != "fa" {
		i++
	}

	return string(encoding[:i]), i, nil
}

func readMetadata(encoding []byte) (map[string]string, int, error) {
	mapping := make(map[string]string)
	i := 0
	for fmt.Sprintf("%02x", encoding[i]) != "fe" && fmt.Sprintf("%02x", encoding[i]) != "ff" {
		if fmt.Sprintf("%02x", encoding[i]) != "fa" {
			return nil, 0, errors.New("expected 'fa' indicator, but found something else")
		}

		i += 1

		// get metadata key
		key, n, keyErr := decodeValue(encoding[i:])
		if keyErr != nil {
			return nil, 0, keyErr
		}

		i += n

		value, m, valueErr := decodeValue(encoding[i:])
		if valueErr != nil {
			return nil, 0, valueErr
		}

		i += m
		mapping[key] = value
	}

	return mapping, i, nil
}

func storeInMap(fileEncoding []byte, key, val string, expiryPtr *expiry) bool {
	// assumes key doesn't already exist in hashmap
	toBeInserted := ""

	filePointer := 0

	_, n, headerErr := readRDBHeader(fileEncoding[filePointer:])
	if headerErr != nil {
		fmt.Println("Problem: error thrown when reading RDB header")
	}
	filePointer += n

	_, n, metadataErr := readMetadata(fileEncoding[filePointer:])
	if metadataErr != nil {
		fmt.Println("Problem: error thrown when reading RDB metadata")
	}
	filePointer += n

	if fmt.Sprintf("%02x", fileEncoding[filePointer]) != "fe" {
		// TODO: address when database is empty (add a database section)
		toBeInserted += "fe00fb01"

		if expiryPtr == nil {
			toBeInserted += "00"
		} else {
			timestampHex, err := toggleEndianHex(fmt.Sprintf("%016x", (*expiryPtr).Timestamp.UnixMilli()))
			if err != nil {
				fmt.Println("Problem: error thrown when converting expiry timestamp to little-endian")
			}
			toBeInserted += "01fc" + timestampHex
		}

		encodedKey, keyErr := encodeValue(key)
		if keyErr != nil {
			fmt.Println("Problem: error thrown when encoding key")
			return false
		}
		encodedValue, valueErr := encodeValue(val)
		if valueErr != nil {
			fmt.Println("Problem: error thrown when encoding value")
			return false
		}

		toBeInserted += "00" + encodedKey + encodedValue

	} else {
		originalFilePointer := filePointer
		if _, exists := getFromMap(fmt.Sprintf("%x", fileEncoding), key); exists {
			// TODO: address when key already exists (including expiry)

		} else {
			// fe section
			filePointer += 2
			toBeInserted += "fe00"

			// fb section
			filePointer += 1
			toBeInserted += "fb"

			databaseSize, decodeDatabaseLengthErr := decodeLength(fileEncoding[filePointer:])
			if decodeDatabaseLengthErr != nil {
				fmt.Println("Problem: error thrown when decoding length of entire database")
				return false
			}
			databaseSize += 1
			filePointer += 1

			expirySize, decodeExpiryLengthErr := decodeLength(fileEncoding[filePointer:])
			if decodeExpiryLengthErr != nil {
				fmt.Println("Problem: error thrown when decoding length of expiry database")
				return false
			}
			if expiryPtr != nil {
				expirySize += 1
			}
			filePointer += 1

			encodedDatabaseLength, encodeDatabaseLengthErr := encodeLength(databaseSize)
			if encodeDatabaseLengthErr != nil {
				fmt.Println("Problem: error thrown when encoding length of entire database")
			}
			encodedExpiryLength, encodeExpiryLengthErr := encodeLength(expirySize)
			if encodeExpiryLengthErr != nil {
				fmt.Println("Problem: error thrown when encoding length of expiry database")
			}
			toBeInserted += encodedDatabaseLength + encodedExpiryLength

			if expiryPtr != nil {
				timestampHex, err := toggleEndianHex(fmt.Sprintf("%016x", (*expiryPtr).Timestamp.UnixMilli()))
				if err != nil {
					fmt.Println("Problem: error thrown when converting expiry timestamp to little-endian")
				}
				toBeInserted += "fc" + timestampHex
			}

			encodedKey, keyErr := encodeValue(key)
			if keyErr != nil {
				fmt.Println("Problem: error thrown when encoding key")
				return false
			}
			encodedValue, valueErr := encodeValue(val)
			if valueErr != nil {
				fmt.Println("Problem: error thrown when encoding value")
				return false
			}

			toBeInserted += "00" + encodedKey + encodedValue
		}

		for fmt.Sprintf("%02x", fileEncoding[filePointer]) != "ff" {
			if fmt.Sprintf("%02x", fileEncoding[filePointer]) == "00" {
				filePointer += 1
				toBeInserted += "00"
				_, n, err := decodeValue(fileEncoding[filePointer:])
				if err != nil {
					fmt.Println("Problem: error thrown when decoding the value of a key")
					return false
				}
				toBeInserted += fmt.Sprintf("%*x", n, fileEncoding[filePointer:filePointer+n])
				filePointer += n

				_, m, err := decodeValue(fileEncoding[filePointer:])
				if err != nil {
					fmt.Println("Problem: error thrown when decoding the value of a value")
					return false
				}
				toBeInserted += fmt.Sprintf("%*x", m, fileEncoding[filePointer:filePointer+n])
				filePointer += n
			} else if fmt.Sprintf("%02x", fileEncoding[filePointer]) == "fc" {
				filePointer += 1
				toBeInserted += "fc" + fmt.Sprintf("%x", fileEncoding[filePointer:filePointer+8])
				filePointer += 8

				toBeInserted += "00"
				_, n, err := decodeValue(fileEncoding[filePointer:])
				if err != nil {
					fmt.Println("Problem: error thrown when decoding the value of a key")
					return false
				}
				toBeInserted += fmt.Sprintf("%*x", n, fileEncoding[filePointer:filePointer+n])
				filePointer += n

				_, m, err := decodeValue(fileEncoding[filePointer:])
				if err != nil {
					fmt.Println("Problem: error thrown when decoding the value of a value")
					return false
				}
				toBeInserted += fmt.Sprintf("%*x", m, fileEncoding[filePointer:filePointer+n])
				filePointer += n
			} else if fmt.Sprintf("%02x", fileEncoding[filePointer]) == "fd" {
				filePointer += 1
				toBeInserted += "fd" + fmt.Sprintf("%x", fileEncoding[filePointer:filePointer+4])
				filePointer += 4

				toBeInserted += "00"
				_, n, err := decodeValue(fileEncoding[filePointer:])
				if err != nil {
					fmt.Println("Problem: error thrown when decoding the value of a key")
					return false
				}
				toBeInserted += fmt.Sprintf("%*x", n, fileEncoding[filePointer:filePointer+n])
				filePointer += n

				_, m, err := decodeValue(fileEncoding[filePointer:])
				if err != nil {
					fmt.Println("Problem: error thrown when decoding the value of a value")
					return false
				}
				toBeInserted += fmt.Sprintf("%*x", m, fileEncoding[filePointer:filePointer+n])
				filePointer += n
			} else {
				fmt.Println("Problem: unexpected first byte for a database entry")
				return false
			}
		}

		filePointer = originalFilePointer
	}

	databaseBytes, decodeHexErr := hex.DecodeString(toBeInserted)
	if decodeHexErr != nil {
		fmt.Println("Problem: error thrown when decoding database hex encoding")
		return false
	}
	payload := append(fileEncoding[:filePointer], databaseBytes...)

	// compute new end-of-file checksum
	checksum := getChecksum(payload)

	newFileEncoding := append(append(payload, 0xFF), checksum...)

	success, _ := writeRDBFile(newFileEncoding)
	if !success {
		fmt.Println("Problem: RDB file could not be written")
		return false
	}

	return true
}

func getChecksum(payload []byte) []byte {
	table := crc64.MakeTable(crc64.ECMA)
	checksum := crc64.Checksum(payload, table)

	var buffer bytes.Buffer

	binary.Write(&buffer, binary.LittleEndian, checksum)
	newChecksumBytes := buffer.Bytes()
	return newChecksumBytes
}

func getFromMap(fileEncoding, key string) ([]byte, bool) {
	hashmap, expiries := extractMap(fileEncoding)
	// fmt.Printf("After getting the RDB file hashmap, the result was: %v\n", hashmap)

	value, valueExists := hashmap[key]
	if !valueExists {
		return nullBulkString(), false
	}

	expiryPtr, expiryPtrExists := expiries[key]
	if expiryPtrExists && expiryPtr != nil && time.Now().Compare((*expiryPtr).Timestamp) > 0 {
		return nullBulkString(), false
	}

	return encodeSimpleString(value), true
}

type keyValuePair struct {
	Key       string
	Value     string
	ExpiryPtr *expiry
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
