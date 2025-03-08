package hyyve

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

var (
	// ErrKeyNotFound is returned when a requested transaction hash doesn't exist
	ErrKeyNotFound = errors.New("key not found")
	// ErrDatabaseClosed is returned when attempting operations on a closed database
	ErrDatabaseClosed = errors.New("database is closed")
	// ErrInvalidKeyPair is returned when a cryptographic operation fails
	ErrInvalidKeyPair = errors.New("invalid key pair")
	// ErrCorruptedData is returned when stored data fails validation
	ErrCorruptedData = errors.New("data corruption detected")
	// ErrCircularReference is returned when a transaction would create a reference cycle
	ErrCircularReference = errors.New("circular reference detected")
	// ErrMaxDepthExceeded is returned when a reference chain exceeds MaxReferenceDepth
	ErrMaxDepthExceeded = errors.New("reference chain too deep")
	// ErrInvalidReference is returned when a transaction references a non-existent transaction
	ErrInvalidReference = errors.New("invalid reference")
	// ErrBatchValidation is returned when batch transaction validation fails
	ErrBatchValidation = errors.New("batch validation failed")
	// ErrDependencyCycle is returned when transactions in a batch have circular dependencies
	ErrDependencyCycle = errors.New("dependency cycle detected")
)

const (
	// Fixed record sizes
	// HeaderSize is the size in bytes of the record header (timestamp + counts + fee)
	HeaderSize = 14 // 8 + 2 + 2 + 2 (removed Flags)
	// CoreSize is the size in bytes of the core transaction fields
	CoreSize = HashSize + AddressSize + SignatureSize
	// MinRecordSize is the minimum size of a complete transaction record
	MinRecordSize = HeaderSize + CoreSize

	// Field sizes
	// HashSize is the size in bytes of transaction hashes
	HashSize = 64
	// AddressSize is the size in bytes of addresses
	AddressSize = 44
	// SignatureSize is the size in bytes of signatures
	SignatureSize = 64

	// MaxSkipListLevel is the maximum height of the skip list
	MaxSkipListLevel = 32
	// SkipListP is the probability of adding another level to a skip list node
	SkipListP = 0.5

	// BloomFilterSize is the size in bits of the Bloom filter (16MB)
	BloomFilterSize = 1 << 24
	// BloomHashCount is the number of hash functions used in the Bloom filter
	BloomHashCount = 8

	// MaxReferenceDepth is the maximum allowed depth of transaction reference chains
	MaxReferenceDepth = 100
)

// Transaction represents a single transaction in the database.
// Each transaction can reference multiple previous transactions,
// forming a directed acyclic graph.
type Transaction struct {
	Timestamp  int64
	Hash       string
	From       string
	Signature  string
	Changes    []TransactionChange
	References []string
	Fee        uint64
}

// TransactionChange represents a single change operation within a transaction.
// It includes the recipient address, amount, and optional instruction data.
type TransactionChange struct {
	To              string
	Amount          uint64
	InstructionType string
	InstructionData []byte
}

// DB represents a HyyveKV database instance.
// It provides thread-safe access to the underlying storage
// and maintains multiple indices for efficient queries.
type DB struct {
	mu           sync.RWMutex
	file         *os.File
	filepath     string
	isClosed     bool
	hashIndex    map[string]int64    // hash -> file offset
	addressIndex map[string][]string // address -> []hash (changed from []CompositeKey)
	forwardRefs  map[string][]string // hash -> []referenced_hash
	backwardRefs map[string][]string // hash -> []referencing_hash
	refCounts    map[string]*atomic.Uint32
	timeSkipList *SkipList
	bloom        *BloomFilter
	filePos      sync.Mutex // Protects file position during reads/writes
}

// Options configures the database instance.
type Options struct {
	FilePath string // Path to the database file
}

// RecordHeader represents the fixed-size header portion of a transaction record.
// It contains metadata about the transaction's size and structure.
type RecordHeader struct {
	Timestamp   int64
	RefCount    uint16
	ChangeCount uint16
	Fee         uint16
}

// RecordCore contains the fixed-size core fields of a transaction.
// These fields have predefined sizes and are stored in a binary format.
type RecordCore struct {
	Hash      [HashSize]byte
	From      [AddressSize]byte
	Signature [SignatureSize]byte
}

// FixedRecord combines the header and core fields of a transaction record.
// This structure is used for efficient disk storage and retrieval.
type FixedRecord struct {
	Header RecordHeader
	Core   RecordCore
}

// SkipNode represents a node in the skip list index structure.
// It maintains multiple forward pointers for efficient traversal.
type SkipNode struct {
	key     int64       // timestamp
	value   []string    // transaction hashes at this timestamp
	forward []*SkipNode // array of pointers to next nodes
	level   int         // current node level
}

// SkipList implements a probabilistic data structure for efficient
// timestamp-based queries with O(log n) complexity.
type SkipList struct {
	head     *SkipNode
	maxLevel int
}

// BatchGetResult holds the results of a batch transaction retrieval operation.
// It maps transaction hashes to either successfully retrieved transactions or errors.
type BatchGetResult struct {
	Transactions map[string]*Transaction
	Errors       map[string]error
}

// BatchValidationResult contains the results of batch transaction validation.
// It separates valid transactions from invalid ones and includes error details.
type BatchValidationResult struct {
	Valid   []*Transaction
	Invalid map[*Transaction]error
}

// BloomFilter implements a space-efficient probabilistic data structure
// used to test whether a transaction hash might exist in the database.
type BloomFilter struct {
	bits    []uint64
	numHash uint
}
