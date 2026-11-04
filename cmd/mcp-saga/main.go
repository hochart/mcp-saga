package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"mcp-saga/internal/compensator"
	"mcp-saga/internal/proxy"
	"mcp-saga/internal/store"

	_ "modernc.org/sqlite"
)

var version = "0.1.0-alpha"

func main() {
	var (
		dbPath      string
		appDBPath   string
		showVersion bool
		verbose     bool
	)

	flag.StringVar(&dbPath, "db", "mcp_saga_wal.db", "Path to SQLite WAL database")
	flag.StringVar(&appDBPath, "app-db", "", "Path to target application SQLite database (defaults to SQLITE_DB_PATH)")
	flag.BoolVar(&showVersion, "version", false, "Print version")
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose logging to stderr")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "mcp-saga (v%s): Deterministic Transactional Kernel for MCP\n\n", version)
		fmt.Fprintf(os.Stderr, "Usage: mcp-saga [flags] -- <downstream_cmd> [args...]\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if showVersion {
		fmt.Printf("mcp-saga %s\n", version)
		os.Exit(0)
	}

	downstreamArgs := flag.Args()
	if len(downstreamArgs) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	if !verbose {
		log.SetOutput(io.Discard)
	} else {
		log.SetOutput(os.Stderr)
		log.SetPrefix("[mcp-saga] ")
	}

	walStore, err := store.NewSagaStore(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize SQLite WAL at %s: %v\n", dbPath, err)
		os.Exit(1)
	}
	defer walStore.Close()

	downstream, err := proxy.StartDownstreamMCP(downstreamArgs[0], downstreamArgs[1:]...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to launch downstream process: %v\n", err)
		os.Exit(1)
	}
	defer downstream.Close()

	if appDBPath == "" {
		appDBPath = os.Getenv("SQLITE_DB_PATH")
	}
	var comp *compensator.PostgresCompensator
	if appDBPath != "" {
		// Read-only pre-image connection to the database the downstream server mutates.
		appDB, err := sql.Open("sqlite", appDBPath+"?_pragma=busy_timeout(5000)")
		if err == nil {
			err = appDB.Ping()
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open application database %s: %v\n", appDBPath, err)
			os.Exit(1)
		}
		defer appDB.Close()
		comp = compensator.NewPostgresCompensator(appDB)
		log.Printf("pre-image compensation enabled for %s", appDBPath)
	} else {
		fmt.Fprintln(os.Stderr, "[mcp-saga] warning: no -app-db / SQLITE_DB_PATH set; SQL steps will not get automatic compensations")
	}
	sagaProxy := proxy.NewSagaProxy(walStore, downstream, comp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
		os.Exit(0)
	}()

	scanner := bufio.NewScanner(os.Stdin)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 4*1024*1024)

	out := bufio.NewWriter(os.Stdout)
	for scanner.Scan() {
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		resp := sagaProxy.HandleRequest(ctx, raw)
		if resp != nil {
			out.Write(resp)
			out.WriteByte('\n')
			out.Flush()
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "[mcp-saga] stdin read error: %v\n", err)
	}
}
