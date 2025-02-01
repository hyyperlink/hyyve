![Image](https://github.com/user-attachments/assets/6e892647-e93d-43ba-890f-493457981da3)

# HyyveKV

HyyveKV is a specialized storage engine designed for high-throughput transaction storage and fast reference traversal. It provides a persistent, hash-indexed database optimized for directed acyclic graphs (DAG) where transactions can reference multiple previous transactions.

## Features

- Fast hash-based transaction lookups
- Efficient reference graph traversal with cycle detection
- DAG-based transaction organization with multi-reference support
- Address-based transaction history with timestamp filtering
- Fixed-size record format for core transaction data
- Bloom filter for quick existence checks
- Skip list for efficient timestamp-based queries
- Batch operations for high throughput
- Concurrent read access with proper synchronization
- Automatic reference counting and validation


## Performance

- Hash lookups: O(1) with constant-time access
- Reference traversal: O(1) per reference with cached mappings
- Address history: O(log n) using skip list indexing
- Batch operations: Optimized for sequential disk I/O
- Memory usage: Configurable Bloom filter size (default: 16MB)
- Concurrent reads: Multiple readers supported

## Installation
```bash
go get github.com/hyyperlink/hyyve
```

## Basic Usage

```go
package main

import (
    "fmt"
    "time"
    "errors"
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

    // Store a transaction
    tx := &hyyve.Transaction{
        Timestamp: time.Now().UnixNano(),
        Hash:      "0123456789abcdef0123456789abcdef",
        From:      "sender_address",
        Signature: "signature_data",
        Changes: []hyyve.TransactionChange{{
            To:     "recipient_address",
            Amount: 1000,
        }},
        Fee: 10,
    }
    if err := db.SetTransaction(tx); err != nil {
        panic(err)
    }

    // Retrieve by hash
    retrieved, err := db.GetTransaction(tx.Hash)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Retrieved: %+v\n", retrieved)
}
```

## Reference Handling
```go
// Create transactions with references
tx1 := &hyyve.Transaction{
    Hash: "tx1_hash",
    From: "sender",
    Changes: []hyyve.TransactionChange{{
        To: "recipient",
        Amount: 1000,
    }},
}

// Store first transaction
if err := db.SetTransaction(tx1); err != nil {
    panic(err)
}

// Create transaction that references tx1
tx2 := &hyyve.Transaction{
    Hash: "tx2_hash",
    From: "sender",
    References: []string{"tx1_hash"}, // Can reference multiple transactions
    Changes: []hyyve.TransactionChange{{
        To: "recipient",
        Amount: 500,
    }},
}
```

## Core Concepts

### Transactions
Each transaction contains:
- Hash: Unique identifier (32 bytes)
- From: Source address (32 bytes)
- Changes: List of operations with:
  - To: Recipient address
  - Amount: Value
  - InstructionType (optional)
  - InstructionData (optional JSON)
- References: Optional links to other transactions
- Signature: 64-byte signature
- Fee: Transaction fee (uint64)
- Timestamp: Unix nano timestamp

### Configuration
```go
const (
    MaxReferenceDepth = 100  // Maximum depth of reference chain
    BloomFilterSize  = 1<<24 // 16MB Bloom filter
    MaxSkipListLevel = 32    // Skip list height for timestamp indexing
)
```

### Storage Format
HyyveKV uses a hybrid storage format optimized for both random access and sequential reads:

1. Fixed-size record header (14 bytes)
- Fixed-size core fields (128 bytes)
- Variable-length data using binary encoding
  - Changes array
  - References array

### Indexes
Multiple indexes are maintained for efficient queries:
- Hash → offset map for direct lookups
- Address → transaction list for history queries
- Forward/backward reference maps for graph traversal
- Skip list for timestamp-based queries
- Bloom filter for fast existence checks

## Error Handling
HyyveKV provides specific error types:
```go
var (
    ErrKeyNotFound       = errors.New("key not found")
    ErrDatabaseClosed    = errors.New("database is closed")
    ErrInvalidKeyPair    = errors.New("invalid key pair")
    ErrCorruptedData     = errors.New("data corruption detected")
    ErrCircularReference = errors.New("circular reference detected")
    ErrMaxDepthExceeded  = errors.New("reference chain too deep")
    ErrInvalidReference  = errors.New("invalid reference")
    ErrBatchValidation   = errors.New("batch validation failed")
    ErrDependencyCycle   = errors.New("dependency cycle detected")
)
```

## Thread Safety
HyyveKV provides thread-safe operations:
- Multiple concurrent readers
- Single writer with readers
- Safe concurrent access to reference counts
- Proper file position locking for disk operations

## Database Operations

### Opening/Closing
```go
// Open with options
db, err := hyyve.Open(hyyve.Options{
    FilePath: "path/to/db",
})
if err != nil {
    // Handle error
}
defer db.Close()
```

### Transaction Validation
All transactions are validated before storage:
- Hash uniqueness
- Reference existence
- Circular reference detection
- Reference chain depth
- Field size limits

### Batch Processing
For high-throughput scenarios:
```go
// Write batch with automatic dependency sorting
batch := []*hyyve.Transaction{tx1, tx2, tx3}
result := db.BatchSetTransactions(batch)

// Read batch with optimized disk access
hashes := []string{"hash1", "hash2"}
result := db.BatchGetTransactions(hashes)
```


## Common Patterns

### Reference Graph Traversal
```go
// Get all referenced transactions
refs, err := db.GetForwardRefs(hash)

// Get all referencing transactions
backRefs, err := db.GetBackwardRefs(hash)

// Check reference count
if db.CanArchive(hash) {
    // No transactions reference this one
}
```

### Address History
```go
// Get recent transactions
txs, err := db.GetAddressTransactions(addr, time.Now().Add(-24*time.Hour).UnixNano(), 100)
```

## License

MIT License - Copyright (c) 2024 Hyyperlink

## Acknowledgments

Inspired by [research from Martin Kleppmann](https://martin.kleppmann.com/2020/12/02/bloom-filter-hash-graph-sync.html) on hash graph synchronization and efficient storage formats.