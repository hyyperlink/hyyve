# HyyveKV

HyyveKV is a specialized storage engine designed for high-throughput transaction storage and fast reference traversal. It provides a persistent, hash-indexed database optimized for blockchain-like transaction systems where transactions can reference other transactions.

## Features

- Fast hash-based transaction lookups
- Efficient reference graph traversal with cycle detection
- Address-based transaction history with timestamp filtering
- Fixed-size record format for core transaction data
- Bloom filter for quick existence checks
- Skip list for efficient timestamp-based queries
- Batch operations for high throughput
- Concurrent read access with proper synchronization
- Automatic reference counting and validation

## Installation
```bash
go get github.com/hyyperlink/hyyve
```

## Concepts

### Transactions
A transaction is the basic unit of storage in HyyveKV. Each transaction has:
- A unique hash identifier
- A source address (From)
- A timestamp
- One or more changes (e.g., transfers, instructions)
- Optional references to other transactions
- A cryptographic signature
- A transaction fee

### References
Transactions can reference other transactions, creating a directed graph. HyyveKV:
- Validates references exist before storage
- Prevents circular references
- Maintains reference counts
- Supports efficient graph traversal
- Limits reference chain depth (configurable)

### Storage Format
HyyveKV uses a hybrid storage format optimized for both random access and sequential reads:

1. Fixed-Size Records (144 bytes):
   - Header (14 bytes):
     * Timestamp (8 bytes)
     * Reference count (2 bytes)
     * Change count (2 bytes)
     * Fee (2 bytes)
   - Core (128 bytes):
     * Hash (32 bytes)
     * From address (32 bytes)
     * Signature (64 bytes)

2. Variable-Length Data:
   - JSON-encoded transaction changes
   - Reference list
   - Custom instruction data

### Indexes
Multiple indexes are maintained for efficient queries:
- Hash → offset map for direct lookups
- Address → transaction list for history queries
- Forward/backward reference maps for graph traversal
- Skip list for timestamp-based queries
- Bloom filter for fast existence checks

## Quick Start

```go
package main

import (
    "fmt"
    "time"
    "github.com/hyyperlink/hyyve"
)

func main() {
	// Open database
	db, err := hyyve.Open(hyyve.Options{
		FilePath: "transactions.db",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// Create a transaction
	tx := &hyyve.Transaction{
		Timestamp: time.Now().UnixNano(),
		Hash:      "0123456789abcdef0123456789abcdef",
		From:      "sender_address",
		Signature: "signature_data",
		Changes: []hyyve.TransactionChange{{
			To:     "recipient_address",
			Amount: 1000,
		}},
		Fee: 12345,
	}
	// Store transaction
	if err := db.SetTransaction(tx); err != nil {
		panic(err)
	}
	// Retrieve transaction
	retrieved, err := db.GetTransaction(tx.Hash)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Retrieved transaction: %+v\n", retrieved)
	// Get address history
	txs, err := db.GetAddressTransactions("sender_address", 0, 10)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found %d transactions for address\n", len(txs))
}

```
## Usage Guidelines

### Opening/Closing
Always close the database when done to ensure proper cleanup:
```go
db, err := hyyve.Open(hyyve.Options{FilePath: "path/to/db"})
if err != nil {
    // Handle error
}
defer db.Close()
```

### Transaction References
When creating transactions with references:
- References must exist in the database
- Maximum reference chain depth is enforced (default: 100)
- Circular references are detected and prevented
- References are validated before storage

### Batch Operations
For high-throughput scenarios, use batch operations:
- BatchSetTransactions for writing multiple transactions
- BatchGetTransactions for reading multiple transactions
- Transactions are automatically sorted by dependencies

### Address History
Query transaction history for addresses with optional filtering:
```go
// Get last 10 transactions after timestamp 0
txs, err := db.GetAddressTransactions("address", 0, 10)
```

## Performance

- Hash lookups: O(1) with constant-time access
- Reference traversal: O(1) per reference with cached mappings
- Address history: O(log n) using skip list indexing
- Batch operations: Optimized for sequential disk I/O
- Memory usage: Configurable Bloom filter size (default: 16MB)
- Concurrent reads: Multiple readers supported

## License

MIT License - Copyright (c) 2024 Hyyperlink

## Acknowledgments

Inspired by research from Martin Kleppmann on hash graph synchronization and efficient storage formats.