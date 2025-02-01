package hyyve

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
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

// Bloom filter implementation
type BloomFilter struct {
	bits    []uint64
	numHash uint
}

// Create a new skip list
func NewSkipList() *SkipList {
	return &SkipList{
		head: &SkipNode{
			forward: make([]*SkipNode, MaxSkipListLevel),
			level:   MaxSkipListLevel,
		},
		maxLevel: MaxSkipListLevel,
	}
}

// Randomly determine level for a new node
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < sl.maxLevel && rand.Float64() < SkipListP {
		level++
	}
	return level
}

// Insert a value into the skip list
func (sl *SkipList) Insert(timestamp int64, hash string) {
	update := make([]*SkipNode, sl.maxLevel)
	current := sl.head

	for i := sl.maxLevel - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < timestamp {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	if current != nil && current.key == timestamp {
		current.value = append(current.value, hash)
		return
	}

	level := sl.randomLevel()
	newNode := &SkipNode{
		key:     timestamp,
		value:   []string{hash},
		forward: make([]*SkipNode, level),
		level:   level,
	}

	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

// Create a new Bloom filter
func NewBloomFilter() *BloomFilter {
	return &BloomFilter{
		bits:    make([]uint64, BloomFilterSize/64), // 64 bits per uint64
		numHash: BloomHashCount,
	}
}

// Add an item to the Bloom filter
func (bf *BloomFilter) Add(item string) {
	h1, h2 := hash128(item)
	for i := uint(0); i < bf.numHash; i++ {
		h := h1 + uint64(i)*h2
		pos := h % uint64(len(bf.bits)*64)
		bf.bits[pos/64] |= 1 << (pos % 64)
	}
}

// Check if an item might exist
func (bf *BloomFilter) MightContain(item string) bool {
	h1, h2 := hash128(item)
	for i := uint(0); i < bf.numHash; i++ {
		h := h1 + uint64(i)*h2
		pos := h % uint64(len(bf.bits)*64)
		if bf.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// Simple hash function for strings
func hash128(s string) (uint64, uint64) {
	h1 := uint64(0)
	h2 := uint64(0)
	for i := 0; i < len(s); i++ {
		h1 = h1*31 + uint64(s[i])
		h2 = h2*37 + uint64(s[i])
	}
	return h1, h2
}

func Open(opts Options) (*DB, error) {
	file, err := os.OpenFile(opts.FilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	db := &DB{
		file:         file,
		filepath:     opts.FilePath,
		hashIndex:    make(map[string]int64),
		addressIndex: make(map[string][]string),
		forwardRefs:  make(map[string][]string),
		backwardRefs: make(map[string][]string),
		refCounts:    make(map[string]*atomic.Uint32),
		timeSkipList: NewSkipList(),
		bloom:        NewBloomFilter(),
	}

	if err := db.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}

	return db, nil
}

func (db *DB) loadIndex() error {
	const batchSize = 1000
	var batch []*Transaction
	var currentOffset int64
	var err error

	for {
		// Get current position
		currentOffset, err = db.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		// Read fixed record
		record, err := db.readFixedRecord(currentOffset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Read variable data
		var varDataSize uint32
		if err := binary.Read(db.file, binary.BigEndian, &varDataSize); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		varData := make([]byte, varDataSize)
		if _, err := io.ReadFull(db.file, varData); err != nil {
			return err
		}

		// Unmarshal variable data
		var extraData struct {
			Changes    []TransactionChange `json:"changes"`
			References []string            `json:"references"`
		}
		if err := json.Unmarshal(varData, &extraData); err != nil {
			return err
		}

		// Create transaction
		tx := fixedRecordToTransaction(record)
		tx.Changes = extraData.Changes
		tx.References = extraData.References

		// Add to batch
		batch = append(batch, tx)

		// Process batch if full
		if len(batch) >= batchSize {
			if err := db.processBatch(batch, currentOffset-int64(len(batch))*MinRecordSize); err != nil {
				return err
			}
			batch = batch[:0] // Clear batch
		}
	}

	// Process remaining transactions
	if len(batch) > 0 {
		if err := db.processBatch(batch, currentOffset-int64(len(batch))*MinRecordSize); err != nil {
			return err
		}
	}

	return nil
}

// Helper function to process a batch of transactions
func (db *DB) processBatch(batch []*Transaction, startOffset int64) error {
	// Sort transactions by dependencies
	sorted, err := db.DependencySort(batch)
	if err != nil {
		return err
	}

	// Process sorted transactions
	offset := startOffset
	for _, tx := range sorted {
		// Update indices
		db.hashIndex[tx.Hash] = offset
		db.bloom.Add(tx.Hash)
		db.timeSkipList.Insert(tx.Timestamp, tx.Hash)
		db.addressIndex[tx.From] = append(db.addressIndex[tx.From], tx.Hash)

		// Update reference maps
		for _, refHash := range tx.References {
			if err := db.AddReference(tx.Hash, refHash); err != nil {
				return err
			}
		}

		offset += MinRecordSize + int64(binary.Size(tx.Changes)+binary.Size(tx.References))
	}

	return nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isClosed {
		return nil
	}

	db.isClosed = true
	return db.file.Close()
}

// Internal method for setting transactions
func (db *DB) setTransactionInternal(tx *Transaction) error {
	if db.isClosed {
		return ErrDatabaseClosed
	}

	// Validate references
	if err := db.ValidateReferences(tx); err != nil {
		return err
	}

	// Add to Bloom filter
	db.bloom.Add(tx.Hash)

	// Convert to fixed record
	record, err := transactionToFixedRecord(tx)
	if err != nil {
		return err
	}

	// Write fixed record
	offset, err := db.writeFixedRecord(record)
	if err != nil {
		return err
	}

	// Prepare and write variable length data
	varData := struct {
		Changes    []TransactionChange `json:"changes"`
		References []string            `json:"references"`
	}{
		Changes:    tx.Changes,
		References: tx.References,
	}

	varBytes, err := json.Marshal(varData)
	if err != nil {
		return err
	}

	// Write variable data size and content
	if err := binary.Write(db.file, binary.BigEndian, uint32(len(varBytes))); err != nil {
		return err
	}
	if _, err := db.file.Write(varBytes); err != nil {
		return err
	}

	// Update indices
	db.hashIndex[tx.Hash] = offset
	db.timeSkipList.Insert(tx.Timestamp, tx.Hash)
	db.addressIndex[tx.From] = append(db.addressIndex[tx.From], tx.Hash)

	// Add references
	for _, refHash := range tx.References {
		if err := db.AddReference(tx.Hash, refHash); err != nil {
			return err
		}
	}

	return nil
}

// Update SetTransaction to use internal method
func (db *DB) SetTransaction(tx *Transaction) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.setTransactionInternal(tx)
}

func (db *DB) readTransactionFromOffset(offset int64) (*Transaction, error) {
	// Read fixed record
	record, err := db.readFixedRecord(offset)
	if err != nil {
		return nil, err
	}

	// Convert fixed record to transaction
	tx := fixedRecordToTransaction(record)

	// Read variable length data size
	var varDataSize uint32
	if err := binary.Read(db.file, binary.BigEndian, &varDataSize); err != nil {
		return nil, err
	}

	// Read variable length data
	varData := make([]byte, varDataSize)
	if _, err := io.ReadFull(db.file, varData); err != nil {
		return nil, err
	}

	// Unmarshal variable data
	var extraData struct {
		Changes    []TransactionChange `json:"changes"`
		References []string            `json:"references"`
	}
	if err := json.Unmarshal(varData, &extraData); err != nil {
		return nil, err
	}

	// Add variable data to transaction
	tx.Changes = extraData.Changes
	tx.References = extraData.References

	return tx, nil
}

func (db *DB) GetTransaction(hash string) (*Transaction, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isClosed {
		return nil, ErrDatabaseClosed
	}

	offset, exists := db.hashIndex[hash]
	if !exists {
		return nil, ErrKeyNotFound
	}

	return db.readTransactionFromOffset(offset)
}

// Serialize header to bytes
func (h *RecordHeader) MarshalBinary() ([]byte, error) {
	buf := make([]byte, HeaderSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(h.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], h.RefCount)
	binary.BigEndian.PutUint16(buf[10:12], h.ChangeCount)
	binary.BigEndian.PutUint16(buf[12:14], h.Fee)
	return buf, nil
}

// Deserialize header from bytes
func (h *RecordHeader) UnmarshalBinary(data []byte) error {
	if len(data) < HeaderSize {
		return ErrCorruptedData
	}
	h.Timestamp = int64(binary.BigEndian.Uint64(data[0:8]))
	h.RefCount = binary.BigEndian.Uint16(data[8:10])
	h.ChangeCount = binary.BigEndian.Uint16(data[10:12])
	h.Fee = binary.BigEndian.Uint16(data[12:14])
	return nil
}

// Convert transaction to fixed record
func transactionToFixedRecord(tx *Transaction) (*FixedRecord, error) {
	record := &FixedRecord{}

	// Set header fields
	record.Header.Timestamp = tx.Timestamp
	record.Header.Fee = uint16(tx.Fee)
	record.Header.RefCount = uint16(len(tx.References))
	record.Header.ChangeCount = uint16(len(tx.Changes))

	// Copy hash (with padding/truncation if necessary)
	hashBytes := []byte(tx.Hash)
	if len(hashBytes) > HashSize {
		return nil, errors.New("hash too long")
	}
	copy(record.Core.Hash[:], hashBytes)

	// Copy from address
	fromBytes := []byte(tx.From)
	if len(fromBytes) > AddressSize {
		return nil, errors.New("address too long")
	}
	copy(record.Core.From[:], fromBytes)

	// Copy signature
	sigBytes := []byte(tx.Signature)
	if len(sigBytes) > SignatureSize {
		return nil, errors.New("signature too long")
	}
	copy(record.Core.Signature[:], sigBytes)

	return record, nil
}

// Convert fixed record back to transaction
func fixedRecordToTransaction(record *FixedRecord) *Transaction {
	tx := &Transaction{
		Timestamp: record.Header.Timestamp,
		Fee:       uint64(record.Header.Fee),
	}

	// Trim any zero padding from fixed fields
	tx.Hash = string(bytes.TrimRight(record.Core.Hash[:], "\x00"))
	tx.From = string(bytes.TrimRight(record.Core.From[:], "\x00"))
	tx.Signature = string(bytes.TrimRight(record.Core.Signature[:], "\x00"))

	return tx
}

// Write a fixed record to disk
func (db *DB) writeFixedRecord(record *FixedRecord) (int64, error) {
	headerBytes, err := record.Header.MarshalBinary()
	if err != nil {
		return 0, err
	}

	offset, err := db.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Write header
	if _, err := db.file.Write(headerBytes); err != nil {
		return 0, err
	}

	// Write core fields
	if err := binary.Write(db.file, binary.BigEndian, record.Core); err != nil {
		return 0, err
	}

	return offset, nil
}

// Read a fixed record from disk
func (db *DB) readFixedRecord(offset int64) (*FixedRecord, error) {
	record := &FixedRecord{}

	_, err := db.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	// Read header
	headerBytes := make([]byte, HeaderSize)
	if _, err := io.ReadFull(db.file, headerBytes); err != nil {
		return nil, err
	}
	if err := record.Header.UnmarshalBinary(headerBytes); err != nil {
		return nil, err
	}

	// Read core fields
	if err := binary.Read(db.file, binary.BigEndian, &record.Core); err != nil {
		return nil, err
	}

	return record, nil
}

// Reference graph operations
func (db *DB) AddReference(fromHash, toHash string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Verify both transactions exist
	if _, exists := db.hashIndex[fromHash]; !exists {
		return ErrKeyNotFound
	}
	if _, exists := db.hashIndex[toHash]; !exists {
		return ErrKeyNotFound
	}

	// Update forward references
	if db.forwardRefs[fromHash] == nil {
		db.forwardRefs[fromHash] = make([]string, 0)
	}
	db.forwardRefs[fromHash] = append(db.forwardRefs[fromHash], toHash)

	// Update backward references
	if db.backwardRefs[toHash] == nil {
		db.backwardRefs[toHash] = make([]string, 0)
	}
	db.backwardRefs[toHash] = append(db.backwardRefs[toHash], fromHash)

	// Increment reference count
	if db.refCounts[toHash] == nil {
		db.refCounts[toHash] = &atomic.Uint32{}
	}
	db.refCounts[toHash].Add(1)

	return nil
}

// Get all transactions this transaction references
func (db *DB) GetForwardRefs(hash string) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	refs, exists := db.forwardRefs[hash]
	if !exists {
		return nil, ErrKeyNotFound
	}

	// Return a copy to prevent external modification
	result := make([]string, len(refs))
	copy(result, refs)
	return result, nil
}

// Get all transactions that reference this transaction
func (db *DB) GetBackwardRefs(hash string) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	refs, exists := db.backwardRefs[hash]
	if !exists {
		return nil, ErrKeyNotFound
	}

	result := make([]string, len(refs))
	copy(result, refs)
	return result, nil
}

// Check if a transaction can be safely archived (no active references)
func (db *DB) CanArchive(hash string) bool {
	if count, exists := db.refCounts[hash]; exists {
		return count.Load() == 0
	}
	return true
}

// Consolidate reference validation into a single function
func (db *DB) validateReferences(tx *Transaction, tempRefs map[string][]string) error {
	seen := make(map[string]struct{})
	for _, refHash := range tx.References {
		// Check for duplicate references
		if _, exists := seen[refHash]; exists {
			return fmt.Errorf("%w: duplicate reference to %s", ErrInvalidReference, refHash)
		}
		seen[refHash] = struct{}{}

		// Check existence (only need hashIndex check, bloom filter is redundant)
		if _, exists := db.hashIndex[refHash]; !exists {
			return fmt.Errorf("%w: transaction %s not found", ErrInvalidReference, refHash)
		}
	}

	// Check for cycles
	visited := make(map[string]struct{})
	visiting := make(map[string]struct{})

	var checkCycles func(string, int) error
	checkCycles = func(hash string, depth int) error {
		if depth > MaxReferenceDepth {
			return fmt.Errorf("%w: chain exceeds %d references", ErrInvalidReference, MaxReferenceDepth)
		}

		if _, beingVisited := visiting[hash]; beingVisited {
			return fmt.Errorf("%w: cycle detected through %s", ErrInvalidReference, hash)
		}

		if _, alreadyVisited := visited[hash]; alreadyVisited {
			return nil
		}

		visiting[hash] = struct{}{}
		defer delete(visiting, hash)

		// Check references in both temp and permanent graphs
		refs := db.forwardRefs[hash]
		if tempRefs != nil {
			if tempRefs[hash] != nil {
				refs = append(refs, tempRefs[hash]...)
			}
		}

		for _, ref := range refs {
			if err := checkCycles(ref, depth+1); err != nil {
				return fmt.Errorf("%w: path: %s -> %s", err, hash, ref)
			}
		}

		visited[hash] = struct{}{}
		return nil
	}

	return checkCycles(tx.Hash, 0)
}

// Update ValidateReferences to use consolidated function
func (db *DB) ValidateReferences(tx *Transaction) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.validateReferences(tx, nil)
}

// Helper to get the full reference chain
func (db *DB) GetReferenceChain(hash string) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var chain []string
	visited := make(map[string]struct{})

	var traverse func(string, int) error
	traverse = func(current string, depth int) error {
		if depth > MaxReferenceDepth {
			return ErrMaxDepthExceeded
		}

		if _, exists := visited[current]; exists {
			return nil // Already processed
		}

		visited[current] = struct{}{}
		chain = append(chain, current)

		refs, exists := db.forwardRefs[current]
		if !exists {
			return nil
		}

		for _, ref := range refs {
			if err := traverse(ref, depth+1); err != nil {
				return err
			}
		}
		return nil
	}

	if err := traverse(hash, 0); err != nil {
		return nil, err
	}

	return chain, nil
}

// BatchValidationResult holds validation results for multiple transactions
type BatchValidationResult struct {
	Valid   []*Transaction
	Invalid map[*Transaction]error
}

// Update ValidateTransactionBatch to use consolidated function
func (db *DB) ValidateTransactionBatch(txs []*Transaction) *BatchValidationResult {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := &BatchValidationResult{
		Valid:   make([]*Transaction, 0, len(txs)),
		Invalid: make(map[*Transaction]error),
	}

	// Build temporary reference map
	tempRefs := make(map[string][]string)
	for _, tx := range txs {
		tempRefs[tx.Hash] = tx.References
	}

	// Validate each transaction
	for _, tx := range txs {
		if err := db.validateReferences(tx, tempRefs); err != nil {
			result.Invalid[tx] = err
			continue
		}
		result.Valid = append(result.Valid, tx)
	}

	return result
}

// DependencySort sorts transactions based on their references
func (db *DB) DependencySort(txs []*Transaction) ([]*Transaction, error) {
	// Build adjacency graph
	graph := make(map[string][]string)     // hash -> dependent hashes
	inDegree := make(map[string]int)       // hash -> number of dependencies
	txMap := make(map[string]*Transaction) // hash -> transaction

	// Initialize maps
	for _, tx := range txs {
		graph[tx.Hash] = make([]string, 0)
		inDegree[tx.Hash] = 0
		txMap[tx.Hash] = tx
	}

	// Build dependency graph
	for _, tx := range txs {
		for _, ref := range tx.References {
			// Only consider references within our batch
			if _, exists := txMap[ref]; exists {
				graph[ref] = append(graph[ref], tx.Hash)
				inDegree[tx.Hash]++
			}
		}
	}

	// Find all roots (transactions with no dependencies)
	var queue []string
	for hash, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, hash)
		}
	}

	// Process queue
	var sorted []*Transaction
	for len(queue) > 0 {
		// Pop from queue
		current := queue[0]
		queue = queue[1:]

		// Add to sorted list
		sorted = append(sorted, txMap[current])

		// Process dependents
		for _, dependent := range graph[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	// Check for cycles
	if len(sorted) != len(txs) {
		return nil, fmt.Errorf("%w: not all transactions could be sorted", ErrDependencyCycle)
	}

	return sorted, nil
}

// Update BatchSetTransactions to use sorted transactions
func (db *DB) BatchSetTransactions(txs []*Transaction) error {
	validationResult := db.ValidateTransactionBatch(txs)

	if len(validationResult.Invalid) > 0 {
		var errMsg strings.Builder
		errMsg.WriteString("batch validation failed:\n")
		for tx, err := range validationResult.Invalid {
			fmt.Fprintf(&errMsg, "- tx %s: %v\n", tx.Hash, err)
		}
		return fmt.Errorf("%w: %s", ErrBatchValidation, errMsg.String())
	}

	sorted, err := db.DependencySort(validationResult.Valid)
	if err != nil {
		return err // Error is already appropriately wrapped
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for _, tx := range sorted {
		if err := db.setTransactionInternal(tx); err != nil {
			return fmt.Errorf("failed to store transaction %s: %w", tx.Hash, err)
		}
	}

	return nil
}

// Add address-based queries
func (db *DB) GetAddressTransactions(addr string, after int64, limit int) ([]*Transaction, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isClosed {
		return nil, ErrDatabaseClosed
	}

	hashes, exists := db.addressIndex[addr]
	if !exists {
		return nil, nil
	}

	var txs []*Transaction
	for _, hash := range hashes {
		tx, err := db.GetTransaction(hash)
		if err != nil {
			continue
		}
		// Filter by timestamp after getting the transaction
		if tx.Timestamp <= after {
			continue
		}
		if len(txs) >= limit {
			break
		}
		txs = append(txs, tx)
	}

	// Sort by timestamp (newest first)
	sort.Slice(txs, func(i, j int) bool {
		return txs[i].Timestamp > txs[j].Timestamp
	})

	return txs, nil
}

// BatchGetResult holds results for batch get operations
type BatchGetResult struct {
	Transactions map[string]*Transaction
	Errors       map[string]error
}

// Update BatchGetTransactions to use helper
func (db *DB) BatchGetTransactions(hashes []string) *BatchGetResult {
	db.mu.RLock()
	defer db.mu.RUnlock()

	result := &BatchGetResult{
		Transactions: make(map[string]*Transaction),
		Errors:       make(map[string]error),
	}

	// Sort hashes by file offset for sequential reads
	type hashOffset struct {
		hash   string
		offset int64
	}
	offsets := make([]hashOffset, 0, len(hashes))
	for _, hash := range hashes {
		if offset, exists := db.hashIndex[hash]; exists {
			offsets = append(offsets, hashOffset{hash, offset})
		} else {
			result.Errors[hash] = ErrKeyNotFound
		}
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i].offset < offsets[j].offset
	})

	// Fetch transactions sequentially
	for _, ho := range offsets {
		tx, err := db.readTransactionFromOffset(ho.offset)
		if err != nil {
			result.Errors[ho.hash] = err
			continue
		}
		result.Transactions[ho.hash] = tx
	}

	return result
}
