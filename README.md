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
- Automatic reference counting with atomic operations
- Auto-tuning capabilities for optimal batch sizes

## Performance

- Hash lookups: O(1) with constant-time access
- Reference traversal: O(1) per reference with cached mappings
- Address history: O(log n) using skip list indexing
- Batch operations: Auto-tuned for optimal throughput
- Memory usage: Configurable Bloom filter size (default: 16MB)
- Concurrent reads: Multiple readers with optimized synchronization

### Benchmark Results

**System Information:**
- CPU: AMD Ryzen 9 5900X 12-Core Processor
- OS: Linux
- Architecture: amd64

**Auto-tuned Configuration:**
- Write Batch Size: 823 transactions
- Read Batch Size: 205 transactions
- Memory Usage: 1.0 MB
- Target Latency: 4.236ms
- Max Throughput: 235,243 TPS

**Performance by Batch Size:**

| Batch Size | Batch Data | DB Size | Write TPS | Read TPS |
|------------|------------|----------|-----------|-----------|
| 82        | 82.0 KB    | 48.0 MB  | 169,470   | 225,374   |
| 411       | 411.0 KB   | 102.0 MB | 169,287   | 222,231   |
| 823       | 823.0 KB   | 166.7 MB | 171,172   | 221,749   |
| 1646      | 1.6 MB     | 204.9 MB | 162,931   | 223,710   |

All benchmarks run with 24 parallel workers. Numbers represent average performance across multiple runs.

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
        FilePath: "transactions.hv",
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
            To:              "recipient_address",
            Amount:          1000,
            InstructionType: "transfer",
            InstructionData: []byte(`{"memo":"test"}`),
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

## Core Concepts

### Transactions
Each transaction contains:
- Hash: Unique identifier (32 bytes)
- From: Source address (32 bytes)
- Changes: List of operations with:
  - To: Recipient address
  - Amount: Value
  - InstructionType: Operation type
  - InstructionData: JSON-encoded instruction data
- References: Optional links to other transactions
- Signature: 64-byte signature
- Fee: Transaction fee (uint64)
- Timestamp: Unix nano timestamp

### Configuration
```go
const (
    MaxReferenceDepth = 100     // Maximum depth of reference chain
    BloomFilterSize   = 1<<24   // 16MB Bloom filter
    MaxSkipListLevel  = 32      // Skip list height for timestamp indexing
    SkipListP         = 0.5      // Skip list level probability
)
```

### Storage Format
HyyveKV uses a hybrid storage format optimized for both random access and sequential reads:

1. Fixed-size record header (14 bytes)
   - Timestamp (8 bytes)
   - Reference count (2 bytes)
   - Change count (2 bytes)
   - Fee (2 bytes)
2. Fixed-size core fields (128 bytes)
   - Hash (32 bytes)
   - From address (32 bytes)
   - Signature (64 bytes)
3. Variable-length data using binary encoding
   - Changes array
   - References array

### Indexes
Multiple indexes are maintained for efficient queries:
- Hash → offset map for direct lookups
- Address → transaction list for history queries
- Forward/backward reference maps for graph traversal
- Skip list for timestamp-based queries
- Bloom filter for fast existence checks

### Buffer Pooling
HyyveKV uses a sync.Pool to efficiently manage buffers and reduce GC pressure.
This pool is used throughout the system for temporary allocations during reads and writes.

### Skip List Implementation
The timestamp index uses a probabilistic skip list for efficient range queries:
- Dynamic level selection based on SkipListP probability
- Multiple values per timestamp support
- O(log n) search complexity
- Efficient range scan capabilities



### Bloom Filter Implementation
The system uses a space-efficient Bloom filter with:
- Double hashing technique for reduced collisions
- Bit-level operations for memory efficiency
- Configurable number of hash functions (default: 8)
- 16MB default size with ~1% false positive rate


### Binary Serialization
HyyveKV implements efficient binary serialization for all data structures:
- Fixed-size record headers
- Variable-length transaction data
- Optimized change and reference encoding
- Length-prefixed variable data

Example of transaction change serialization:
```go
type TransactionChange struct {
    To              string
    Amount          uint64
    InstructionType string
    InstructionData []byte
}

func (tc *TransactionChange) MarshalBinary() ([]byte, error) {
    // Calculate total size needed
    size := 8 + // Amount (uint64)
        4 + len(tc.To) + // To length + string
        4 + len(tc.InstructionType) + // Type length + string
        4 + len(tc.InstructionData) // Data length + bytes

    buf := make([]byte, size)
    offset := 0

    // Write Amount
    binary.BigEndian.PutUint64(buf[offset:], tc.Amount)
    offset += 8

    // Write To
    binary.BigEndian.PutUint32(buf[offset:], uint32(len(tc.To)))
    offset += 4
    copy(buf[offset:], tc.To)
    offset += len(tc.To)

    // Write InstructionType
    binary.BigEndian.PutUint32(buf[offset:], uint32(len(tc.InstructionType)))
    offset += 4
    copy(buf[offset:], tc.InstructionType)
    offset += len(tc.InstructionType)

    // Write InstructionData
    binary.BigEndian.PutUint32(buf[offset:], uint32(len(tc.InstructionData)))
    offset += 4
    copy(buf[offset:], tc.InstructionData)

    return buf, nil
}
```

## Advanced Features

### Auto-Tuning System
HyyveKV includes sophisticated auto-tuning capabilities that automatically optimize batch sizes based on system performance. The tuner:

- Tests batch sizes from 10 to 1000 transactions
- Measures both read and write performance
- Monitors memory allocation
- Implements cooldown periods to avoid thermal throttling
- Weights read performance more heavily (2x) than write performance

```go
// Example auto-tuning configuration
const (
    minBatchSize = 10                    // Minimum batch size
    maxBatchSize = 1000                  // Maximum to avoid memory bloat
    targetMem    = 256 * 1024 * 1024     // 256MB target memory limit
    cooldown     = 50 * time.Millisecond // Cooldown between tests
)

// Get optimal configuration
config := db.AutoTuneBatchSize()
// Returns BatchConfig with:
type BatchConfig struct {
    WriteBatchSize int           // Optimal write batch size
    ReadBatchSize  int           // Optimized read batch size (1/4 of write)
    MemoryLimit    int64         // Memory usage target
    TargetLatency  time.Duration // Expected operation latency
    MaxThroughput  float64       // Maximum transactions per second
}
```

The auto-tuner prioritizes read performance while staying within memory constraints, typically setting read batch sizes to 1/4 of write batch sizes for better throughput.

### Batch Processing Internals
The batch processing system includes:
- Topological sorting for dependency ordering
- Parallel validation where possible
- Optimized disk I/O patterns
- Transaction dependency resolution

```go
// Example of dependency sorting
sorted, err := db.DependencySort(batch)
if err != nil {
    return fmt.Errorf("dependency sort failed: %w", err)
}
```

### Index Recovery and Maintenance
The database automatically maintains multiple indices for fast lookups. These are rebuilt on startup and kept in sync during operations:

```go
// Open database with existing data
db, err := hyyve.Open(hyyve.Options{
    FilePath: "existing.hv",
})
if err != nil {
    log.Fatal("Failed to open:", err)
}

// Indices are automatically rebuilt
// Now you can query by:

// 1. Hash lookups
tx, err := db.GetTransaction("abc123")

// 2. Address history
txs, err := db.GetAddressTransactions(
    "user123",
    time.Now().Add(-24*time.Hour).UnixNano(),
    100,
)

// 3. Reference traversal
refs, err := db.GetForwardRefs("abc123")

// 4. Timestamp queries (using skip list)
// ... timestamp-based queries
```

The database maintains and automatically recovers:
- Hash → offset mappings for O(1) lookups
- Address → transaction lists for history queries
- Forward/backward reference maps for graph traversal
- Skip list index for timestamp-based queries
- Bloom filter for fast existence checks

All indices are automatically rebuilt on database open and kept consistent during operations.

### Reference Management
The reference system provides robust transaction graph management with:
- Atomic reference counting
- Cycle detection in transaction graphs
- Maximum depth enforcement
- Bidirectional reference tracking

Example usage:
```go
// Create a chain of transactions
tx1 := &Transaction{
    Hash:      "tx1",
    From:      "sender",
    Timestamp: time.Now().UnixNano(),
    Changes: []TransactionChange{{
        To:              "recipient",
        Amount:          1000,
        InstructionType: "transfer",
        InstructionData: []byte(`{"memo":"first"}`),
    }},
}

tx2 := &Transaction{
    Hash:       "tx2",
    From:       "sender",
    Timestamp:  time.Now().UnixNano(),
    References: []string{"tx1"},
    Changes: []TransactionChange{{
        To:              "recipient",
        Amount:          500,
        InstructionType: "transfer",
        InstructionData: []byte(`{"memo":"second"}`),
    }},
}

tx3 := &Transaction{
    Hash:       "tx3",
    From:       "sender",
    Timestamp:  time.Now().UnixNano(),
    References: []string{"tx2"},
    Changes: []TransactionChange{{
        To:              "recipient",
        Amount:          250,
        InstructionType: "transfer",
        InstructionData: []byte(`{"memo":"third"}`),
    }},
}

// Store them in order
if err := db.SetTransaction(tx1); err != nil {
    log.Fatal("Failed to store tx1:", err)
}
if err := db.SetTransaction(tx2); err != nil {
    // Will fail if tx1 doesn't exist
    log.Fatal("Failed to store tx2:", err)
}
if err := db.SetTransaction(tx3); err != nil {
    // Will fail if:
    // - tx2 doesn't exist
    // - Would create a cycle
    // - Would exceed max depth
    log.Fatal("Failed to store tx3:", err)
}

// Check reference relationships
refs, _ := db.GetForwardRefs("tx2")  // Returns ["tx1"]
backs, _ := db.GetBackwardRefs("tx2") // Returns ["tx3"]

// Check if a transaction can be archived
if db.CanArchive("tx1") {
    fmt.Println("tx1 has no active references")
}
```

### Auto-Tuning System
The auto-tuning system helps optimize performance for your specific hardware and workload.

Example usage:

```go
// Get optimal configuration
config := db.AutoTuneBatchSize()

// Use the configuration
batchSize := config.WriteBatchSize
txs := make([]*Transaction, 0, batchSize)

// Build batch
for i := 0; i < batchSize; i++ {
    tx := &Transaction{
        Hash: fmt.Sprintf("tx%d", i),
        // ... other fields
    }
    txs = append(txs, tx)
}

// Write batch using optimal size
if err := db.BatchSetTransactions(txs); err != nil {
    log.Printf("Batch write failed: %v", err)
}

// Read using optimal read size
readBatch := make([]string, 0, config.ReadBatchSize)
for i := 0; i < config.ReadBatchSize; i++ {
    readBatch = append(readBatch, fmt.Sprintf("tx%d", i))
}
result := db.BatchGetTransactions(readBatch)

// Monitor performance
fmt.Printf("Write throughput: %.2f TPS\n", config.MaxThroughput)
fmt.Printf("Target latency: %v\n", config.TargetLatency)
```

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
- Multiple concurrent readers with RWMutex
- Single writer with proper lock ordering
- Atomic reference counting
- File position synchronization for disk operations
- Buffer pooling for improved performance

## Database Operations

### Opening/Closing
```go
// Open with options
db, err := hyyve.Open(hyyve.Options{
    FilePath: "path/to/db.hv",
})
if err != nil {
    // Handle error
}
defer db.Close()
```

### Transaction Validation
All transactions are validated before storage:
- Hash uniqueness
- Reference existence and validity
- Circular reference detection
- Reference chain depth limits
- Field size constraints
- Duplicate reference detection

### Batch Processing
For high-throughput scenarios:
```go
// Write batch with automatic dependency sorting
batch := []*hyyve.Transaction{tx1, tx2, tx3}
if err := db.BatchSetTransactions(batch); err != nil {
    // Handle error
}

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

### Auto-Tuning
```go
// Get optimal batch sizes for current system
config := db.AutoTuneBatchSize()
fmt.Printf("Optimal write batch size: %d\n", config.WriteBatchSize)
fmt.Printf("Optimal read batch size: %d\n", config.ReadBatchSize)
fmt.Printf("Memory usage: %d bytes\n", config.MemoryLimit)
fmt.Printf("Target latency: %v\n", config.TargetLatency)
fmt.Printf("Max throughput: %.2f TPS\n", config.MaxThroughput)
```

## License

MIT License - Copyright (c) 2024 Hyyperlink

## Acknowledgments

Inspired by [research from Martin Kleppmann](https://martin.kleppmann.com/2020/12/02/bloom-filter-hash-graph-sync.html) on hash graph synchronization and efficient storage formats.