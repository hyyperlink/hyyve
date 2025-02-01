package hyyve

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

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

const (
	// Fixed record sizes
	HeaderSize    = 14  // 8 + 2 + 2 + 2 (removed Flags)
	CoreSize      = 128 // 32 + 32 + 64
	MinRecordSize = HeaderSize + CoreSize

	// Field sizes
	HashSize      = 32
	AddressSize   = 32
	SignatureSize = 64

	MaxSkipListLevel = 32  // Maximum number of levels in skip list
	SkipListP        = 0.5 // Probability of adding level

	BloomFilterSize = 1 << 24 // 16MB of bloom filter
	BloomHashCount  = 8       // Number of hash functions

	MaxReferenceDepth = 100 // Maximum depth of reference chain
)

type Transaction struct {
	Timestamp  int64               `json:"timestamp"`
	Hash       string              `json:"hash"`
	From       string              `json:"from"`
	Signature  string              `json:"signature"`
	Changes    []TransactionChange `json:"changes"`
	References []string            `json:"references"`
	Fee        uint64              `json:"fee"`
}

type TransactionChange struct {
	To              string          `json:"to"`
	Amount          uint64          `json:"amount"`
	InstructionType string          `json:"instruction_type,omitempty"`
	InstructionData json.RawMessage `json:"instruction_data,omitempty"`
}

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
}

type Options struct {
	FilePath string
}

// Fixed-size record header
type RecordHeader struct {
	Timestamp   int64
	RefCount    uint16
	ChangeCount uint16
	Fee         uint16
}

// Fixed-size core fields
type RecordCore struct {
	Hash      [HashSize]byte
	From      [AddressSize]byte
	Signature [SignatureSize]byte
}

// Complete fixed-size record
type FixedRecord struct {
	Header RecordHeader
	Core   RecordCore
}

// Skip list node structure
type SkipNode struct {
	key     int64       // timestamp
	value   []string    // transaction hashes at this timestamp
	forward []*SkipNode // array of pointers to next nodes
	level   int         // current node level
}

// Skip list structure
type SkipList struct {
	head     *SkipNode
	maxLevel int
}

// BatchGetResult holds results for batch get operations
type BatchGetResult struct {
	Transactions map[string]*Transaction
	Errors       map[string]error
}

// BatchValidationResult holds validation results for multiple transactions
type BatchValidationResult struct {
	Valid   []*Transaction
	Invalid map[*Transaction]error
}

// Bloom filter implementation
type BloomFilter struct {
	bits    []uint64
	numHash uint
}
