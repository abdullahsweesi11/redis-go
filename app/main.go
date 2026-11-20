package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/db"
	"github.com/codecrafters-io/redis-starter-go/app/repl"
	"github.com/codecrafters-io/redis-starter-go/app/server"
)

func main() {

	// Store any CLI flags
	port, master := parseFlags()

	// Set up replication
	repl.InitReplication(port, master)

	// Load initial data from disk
	if rdbContents, err := db.ReadRDBFile(); err == nil {
		if err = db.ExtractFromDisk(rdbContents); err != nil {
			fmt.Println("Problem: error thrown while migrating disk data:", err)
		}
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		fmt.Printf("Problem: failed to bind to port %s", port)
		os.Exit(1)
	}

	defer l.Close()

	// Accept connections
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Problem: error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go server.HandleConnection(conn)
	}

}
