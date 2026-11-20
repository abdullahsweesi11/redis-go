package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/codecrafters-io/redis-starter-go/app/db"
)

func parseFlags() (string, string) {
	dir := flag.String("dir", "", "Path to RDB file directory")
	dbFilename := flag.String("dbfilename", "", "RDB file name")
	port := flag.String("port", "6379", "Redis server port")
	master := flag.String("replicaof", "", "Master host and port")

	flag.Parse()

	db.RDBConfig.SetString("dir", *dir)
	db.RDBConfig.SetString("dbfilename", *dbFilename)

	// Set RDB config data
	if *dir == "" || *dbFilename == "" {
		// should choose a different home, since it isn't a temporary file
		rDBFile, err := os.Create("/tmp/dump.rdb")
		if err != nil {
			fmt.Println("Problem: could not create an RDB file")
			os.Exit(1)
		}
		defer rDBFile.Close()
		db.RDBConfig.SetString("name", rDBFile.Name())

		writeBuffer := db.GetEmptyRDBFile()
		_, err = rDBFile.Write(writeBuffer)
		if err != nil {
			fmt.Println("Problem: could not initialise an empty RDB file")
			os.Exit(1)
		}
	} else {
		db.RDBConfig.SetString("name", path.Join(*dir, *dbFilename))
	}

	return *port, *master
}
