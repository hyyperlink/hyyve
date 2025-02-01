// Package hyyve provides a specialized storage engine designed for high-throughput
// transaction storage and fast reference traversal. It implements a persistent,
// hash-indexed database optimized for directed acyclic graphs (DAG) where
// transactions can reference multiple previous transactions.
//
// Key features:
//   - O(1) hash-based transaction lookups
//   - Efficient reference graph traversal with cycle detection
//   - Address-based transaction history with timestamp filtering
//   - Auto-tuning for optimal batch sizes
//   - Thread-safe concurrent access
//   - Buffer pooling for reduced GC pressure
//
// Basic usage:
//
//	db, err := hyyve.Open(hyyve.Options{
//		FilePath: "transactions.hv",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
// For detailed benchmarks, examples, and documentation, visit:
//
//	https://pkg.go.dev/github.com/hyyperlink/hyyve
//	https://github.com/hyyperlink/hyyve
package hyyve

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 4096)
		return &b
	},
}

// Create a new skip list
// NewSkipList creates a new skip list with the maximum configured height.
func NewSkipList() *SkipList {
	return &SkipList{
		head: &SkipNode{
			forward: make([]*SkipNode, MaxSkipListLevel),
			level:   MaxSkipListLevel,
		},
		maxLevel: MaxSkipListLevel,
	}
}

// randomLevel determines the height of a new skip list node.
// It uses a probabilistic algorithm where each level has SkipListP chance
// of being included, up to MaxSkipListLevel.
func (sl *SkipList) randomLevel() int {
	level := 1
	for level < sl.maxLevel && rand.Float64() < SkipListP {
		level++
	}
	return level
}

// Insert adds a transaction hash to the skip list at the given timestamp.
// If multiple transactions share the same timestamp, they are stored together.
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
// NewBloomFilter creates a new Bloom filter with the configured size and hash count.
func NewBloomFilter() *BloomFilter {
	return &BloomFilter{
		bits:    make([]uint64, BloomFilterSize/64), // 64 bits per uint64
		numHash: BloomHashCount,
	}
}

// Add inserts an item into the Bloom filter.
// The item is hashed multiple times to set corresponding bits in the filter.
func (bf *BloomFilter) Add(item string) {
	h1, h2 := hash128(item)
	for i := uint(0); i < bf.numHash; i++ {
		h := h1 + uint64(i)*h2
		pos := h % uint64(len(bf.bits)*64)
		bf.bits[pos/64] |= 1 << (pos % 64)
	}
}

// MightContain checks if an item might exist in the Bloom filter.
// Returns false if the item definitely doesn't exist, true if it might exist.
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
// hash128 generates two 64-bit hash values for use in the Bloom filter.
// It uses a simple but effective string hashing algorithm.
func hash128(s string) (uint64, uint64) {
	h1 := uint64(0)
	h2 := uint64(0)
	for i := 0; i < len(s); i++ {
		h1 = h1*31 + uint64(s[i])
		h2 = h2*37 + uint64(s[i])
	}
	return h1, h2
}

// Open creates or opens a HyyveKV database at the specified file path.
// It initializes all necessary indices and recovers the database state.
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
		filePos:      sync.Mutex{},
	}

	if err := db.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}

	return db, nil
}

func (db *DB) loadIndex() error {
	db.filePos.Lock()
	defer db.filePos.Unlock()

	// Start from beginning of file
	if _, err := db.file.Seek(0, 0); err != nil {
		return fmt.Errorf("seek to start: %w", err)
	}

	const batchSize = 1000
	var batch []*Transaction
	var batchStartOffset int64 // Track where this batch started

	for {
		// Get current position before read
		currentOffset, err := db.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("get current offset: %w", err)
		}

		// If this is the start of a new batch, record the offset
		if len(batch) == 0 {
			batchStartOffset = currentOffset
		}

		// Read fixed record
		record, err := db.readFixedRecord(currentOffset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read record at offset %d: %w", currentOffset, err)
		}

		// Read variable data
		var varDataSize uint32
		if err := binary.Read(db.file, binary.BigEndian, &varDataSize); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		varDataPtr := bufferPool.Get().(*[]byte)
		varData := *varDataPtr
		if cap(varData) < int(varDataSize) {
			varData = make([]byte, varDataSize) // If too big, allocate new
			*varDataPtr = varData               // Update the pooled slice
		} else {
			varData = varData[:varDataSize]
		}
		defer bufferPool.Put(varDataPtr)

		// Read variable length data
		if _, err := io.ReadFull(db.file, varData); err != nil {
			return err
		}

		// Read variable data
		var varDataVar variableData
		if err := varDataVar.UnmarshalBinary(varData); err != nil {
			return fmt.Errorf("unmarshal var data: %w", err)
		}

		// Create transaction
		tx := fixedRecordToTransaction(record)
		tx.Changes = varDataVar.Changes
		tx.References = varDataVar.References

		// Add to batch
		batch = append(batch, tx)

		// Process batch if full
		if len(batch) >= batchSize {
			if err := db.processBatch(batch, batchStartOffset); err != nil {
				return err
			}
			batch = batch[:0] // Clear batch
		}
	}

	// Process remaining transactions
	if len(batch) > 0 {
		if err := db.processBatch(batch, batchStartOffset); err != nil {
			return err
		}
	}

	return nil
}

// Helper function to process a batch of transactions
// processBatch handles a batch of transactions during index loading.
// It sorts transactions by dependencies and updates all indices.
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

// Close safely shuts down the database, ensuring all data is written.
// After closing, no further operations can be performed on the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isClosed {
		return nil
	}

	db.isClosed = true
	return db.file.Close()
}

// SetTransaction stores a single transaction in the database.
// It validates the transaction and its references before storage.
func (db *DB) SetTransaction(tx *Transaction) error {
	// Validate before taking the write lock
	if err := db.ValidateReferences(tx); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isClosed {
		return ErrDatabaseClosed
	}

	return db.setTransactionInternal(tx)
}

// Update setTransactionInternal to use binary format
// setTransactionInternal performs the actual transaction storage operation.
// It handles binary serialization and index updates.
func (db *DB) setTransactionInternal(tx *Transaction) error {
	// Add to Bloom filter
	db.bloom.Add(tx.Hash)

	// Convert to fixed record
	record, err := transactionToFixedRecord(tx)
	if err != nil {
		return fmt.Errorf("fixed record conversion: %w", err)
	}

	// Write fixed record
	offset, err := db.writeFixedRecord(record)
	if err != nil {
		return fmt.Errorf("write fixed record: %w", err)
	}

	// Prepare and write variable length data
	varData := &variableData{
		Changes:    tx.Changes,
		References: tx.References,
	}

	// Use binary marshaling
	varBytes, err := varData.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal var data: %w", err)
	}

	// Write size and data
	if err := binary.Write(db.file, binary.BigEndian, uint32(len(varBytes))); err != nil {
		return fmt.Errorf("write var size: %w", err)
	}

	if _, err := db.file.Write(varBytes); err != nil {
		return fmt.Errorf("write var data: %w", err)
	}

	// Update indices
	db.hashIndex[tx.Hash] = offset
	db.timeSkipList.Insert(tx.Timestamp, tx.Hash)
	db.addressIndex[tx.From] = append(db.addressIndex[tx.From], tx.Hash)

	// Add references without taking another lock
	for _, refHash := range tx.References {
		// Update forward references
		if db.forwardRefs[tx.Hash] == nil {
			db.forwardRefs[tx.Hash] = make([]string, 0)
		}
		db.forwardRefs[tx.Hash] = append(db.forwardRefs[tx.Hash], refHash)

		// Update backward references
		if db.backwardRefs[refHash] == nil {
			db.backwardRefs[refHash] = make([]string, 0)
		}
		db.backwardRefs[refHash] = append(db.backwardRefs[refHash], tx.Hash)

		// Increment reference count
		if db.refCounts[refHash] == nil {
			db.refCounts[refHash] = &atomic.Uint32{}
		}
		db.refCounts[refHash].Add(1)
	}

	return nil
}

// Update readTransactionFromOffset to use binary format
// readTransactionFromOffset reads and deserializes a transaction from disk.
// It uses buffer pooling to minimize allocations during reads.
func (db *DB) readTransactionFromOffset(offset int64) (*Transaction, error) {
	db.filePos.Lock()
	defer db.filePos.Unlock()

	// Seek to position
	if _, err := db.file.Seek(offset, 0); err != nil {
		return nil, err
	}

	// Get buffer from pool for header
	headerBufPtr := bufferPool.Get().(*[]byte)
	headerBuf := (*headerBufPtr)[:HeaderSize]
	defer bufferPool.Put(headerBufPtr)

	// Read header
	if _, err := io.ReadFull(db.file, headerBuf); err != nil {
		return nil, err
	}

	record := &FixedRecord{}
	if err := record.Header.UnmarshalBinary(headerBuf); err != nil {
		return nil, err
	}

	// Read core fields
	if err := binary.Read(db.file, binary.BigEndian, &record.Core); err != nil {
		return nil, err
	}

	// Convert fixed record to transaction
	tx := fixedRecordToTransaction(record)

	// Read variable length data size
	var varDataSize uint32
	if err := binary.Read(db.file, binary.BigEndian, &varDataSize); err != nil {
		return nil, err
	}

	// Get buffer from pool for variable data
	varDataPtr := bufferPool.Get().(*[]byte)
	varData := *varDataPtr
	if cap(varData) < int(varDataSize) {
		varData = make([]byte, varDataSize) // If too big, allocate new
		*varDataPtr = varData               // Update the pooled slice
	} else {
		varData = varData[:varDataSize]
	}
	defer bufferPool.Put(varDataPtr)

	// Read variable length data
	if _, err := io.ReadFull(db.file, varData); err != nil {
		return nil, err
	}

	// Read variable data
	var varDataVar variableData
	if err := varDataVar.UnmarshalBinary(varData); err != nil {
		return nil, fmt.Errorf("unmarshal var data: %w", err)
	}

	// Add variable data to transaction
	tx.Changes = varDataVar.Changes
	tx.References = varDataVar.References

	return tx, nil
}

// GetTransaction retrieves a transaction by its hash.
// Returns ErrKeyNotFound if the transaction doesn't exist.
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

// MarshalBinary serializes the record header into a binary format.
// The resulting bytes are in a fixed-size format suitable for disk storage.
func (h *RecordHeader) MarshalBinary() ([]byte, error) {
	buf := make([]byte, HeaderSize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(h.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], h.RefCount)
	binary.BigEndian.PutUint16(buf[10:12], h.ChangeCount)
	binary.BigEndian.PutUint16(buf[12:14], h.Fee)
	return buf, nil
}

// UnmarshalBinary deserializes a binary format into a record header.
// It includes validation of the deserialized values.
func (h *RecordHeader) UnmarshalBinary(data []byte) error {
	if len(data) < HeaderSize {
		return ErrCorruptedData
	}
	h.Timestamp = int64(binary.BigEndian.Uint64(data[0:8]))
	h.RefCount = binary.BigEndian.Uint16(data[8:10])
	h.ChangeCount = binary.BigEndian.Uint16(data[10:12])
	h.Fee = binary.BigEndian.Uint16(data[12:14])

	// Add validation
	if h.Timestamp < 0 {
		return fmt.Errorf("%w: invalid timestamp %d", ErrCorruptedData, h.Timestamp)
	}
	if h.RefCount > 1000 || h.ChangeCount > 1000 { // reasonable limits
		return fmt.Errorf("%w: invalid counts ref=%d change=%d", ErrCorruptedData, h.RefCount, h.ChangeCount)
	}

	return nil
}

// transactionToFixedRecord converts a transaction into a fixed-size record format.
// It handles padding and size validation of fields.
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

// fixedRecordToTransaction converts a fixed-size record back into a transaction.
// It handles trimming of padded fields.
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

// writeFixedRecord writes a fixed-size record to disk.
// Returns the offset where the record was written.
func (db *DB) writeFixedRecord(record *FixedRecord) (int64, error) {
	db.filePos.Lock()
	defer db.filePos.Unlock()

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

	// First check if transaction exists
	if _, exists := db.hashIndex[hash]; !exists {
		return nil, ErrKeyNotFound
	}

	// Return empty slice if no references (instead of error)
	refs, exists := db.forwardRefs[hash]
	if !exists {
		return []string{}, nil
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

	// First check if transaction exists
	if _, exists := db.hashIndex[hash]; !exists {
		return nil, ErrKeyNotFound
	}

	// Return empty slice if no references
	refs, exists := db.backwardRefs[hash]
	if !exists {
		return []string{}, nil
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

// BatchSetTransactions stores multiple transactions in dependency order.
// It validates all transactions and their references before storage.
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

// GetAddressTransactions retrieves transactions associated with an address.
// Returns transactions newer than 'after' timestamp, limited to 'limit' results.
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

// BatchGetTransactions retrieves multiple transactions by their hashes.
// It optimizes disk access by sorting reads by file offset.
func (db *DB) BatchGetTransactions(hashes []string) *BatchGetResult {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Pre-allocate slices with capacity
	result := &BatchGetResult{
		Transactions: make(map[string]*Transaction, len(hashes)),
		Errors:       make(map[string]error, len(hashes)),
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

// MarshalBinary serializes a transaction change into a binary format.
// The format includes length-prefixed variable-size fields.
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

// UnmarshalBinary deserializes a binary format into a transaction change.
// It includes validation of the data format and field sizes.
func (tc *TransactionChange) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return ErrCorruptedData
	}

	offset := 0

	// Read Amount
	tc.Amount = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Read To
	if len(data) < offset+4 {
		return ErrCorruptedData
	}
	toLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	if len(data) < offset+int(toLen) {
		return ErrCorruptedData
	}
	tc.To = string(data[offset : offset+int(toLen)])
	offset += int(toLen)

	// Read InstructionType
	if len(data) < offset+4 {
		return ErrCorruptedData
	}
	typeLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	if len(data) < offset+int(typeLen) {
		return ErrCorruptedData
	}
	tc.InstructionType = string(data[offset : offset+int(typeLen)])
	offset += int(typeLen)

	// Read InstructionData
	if len(data) < offset+4 {
		return ErrCorruptedData
	}
	dataLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	if len(data) < offset+int(dataLen) {
		return ErrCorruptedData
	}
	tc.InstructionData = make([]byte, dataLen)
	copy(tc.InstructionData, data[offset:offset+int(dataLen)])

	return nil
}

// Add binary marshaling for variable data struct
type variableData struct {
	Changes    []TransactionChange
	References []string
}

func (vd *variableData) MarshalBinary() ([]byte, error) {
	// Calculate size
	size := 4 // Number of changes
	for _, change := range vd.Changes {
		changeBytes, err := change.MarshalBinary()
		if err != nil {
			return nil, err
		}
		size += 4 + len(changeBytes) // Length + data
	}

	size += 4 // Number of references
	for _, ref := range vd.References {
		size += 4 + len(ref) // Length + string
	}

	// Write data
	buf := make([]byte, size)
	offset := 0

	// Write changes
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(vd.Changes)))
	offset += 4
	for _, change := range vd.Changes {
		changeBytes, _ := change.MarshalBinary()
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(changeBytes)))
		offset += 4
		copy(buf[offset:], changeBytes)
		offset += len(changeBytes)
	}

	// Write references
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(vd.References)))
	offset += 4
	for _, ref := range vd.References {
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(ref)))
		offset += 4
		copy(buf[offset:], ref)
		offset += len(ref)
	}

	return buf, nil
}

func (vd *variableData) UnmarshalBinary(data []byte) error {
	if len(data) < 4 {
		return ErrCorruptedData
	}

	offset := 0

	// Read changes
	changeCount := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	vd.Changes = make([]TransactionChange, changeCount)
	for i := range vd.Changes {
		if len(data) < offset+4 {
			return ErrCorruptedData
		}
		changeLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		if len(data) < offset+int(changeLen) {
			return ErrCorruptedData
		}
		if err := vd.Changes[i].UnmarshalBinary(data[offset : offset+int(changeLen)]); err != nil {
			return err
		}
		offset += int(changeLen)
	}

	// Read references
	if len(data) < offset+4 {
		return ErrCorruptedData
	}
	refCount := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	vd.References = make([]string, refCount)
	for i := range vd.References {
		if len(data) < offset+4 {
			return ErrCorruptedData
		}
		refLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		if len(data) < offset+int(refLen) {
			return ErrCorruptedData
		}
		vd.References[i] = string(data[offset : offset+int(refLen)])
		offset += int(refLen)
	}

	return nil
}

// BatchConfig holds batch processing configuration
type BatchConfig struct {
	WriteBatchSize int
	ReadBatchSize  int
	MemoryLimit    int64
	TargetLatency  time.Duration
	MaxThroughput  float64
}

// AutoTuneBatchSize determines optimal batch sizes for the current system.
// It runs performance tests to find the best balance of throughput and memory usage.
func (db *DB) AutoTuneBatchSize() BatchConfig {
	const (
		minBatchSize = 10                    // Keep small for writes
		maxBatchSize = 1000                  // Cap max to avoid memory bloat
		targetMem    = 256 * 1024 * 1024     // 256MB (reduced further)
		cooldown     = 50 * time.Millisecond // Faster testing
	)

	var optimal BatchConfig
	var bestWriteThroughput float64
	var bestReadThroughput float64

	// Test batch sizes in smaller increments for more granular optimization
	for batchSize := minBatchSize; batchSize <= maxBatchSize; batchSize += batchSize / 2 {
		runtime.GC()

		// Measure write performance
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		startMem := m.HeapAlloc

		txs := make([]*Transaction, batchSize)
		for i := range txs {
			tx := &Transaction{
				Hash:      fmt.Sprintf("tune_%d", i),
				Timestamp: time.Now().UnixNano(),
			}
			txs[i] = tx
		}

		writeStart := time.Now()
		if err := db.BatchSetTransactions(txs); err != nil {
			continue
		}
		writeDuration := time.Since(writeStart)

		runtime.ReadMemStats(&m)
		memUsed := int64(m.HeapAlloc - startMem)
		if memUsed < 0 {
			memUsed = 0
		}

		writeThroughput := float64(batchSize) / writeDuration.Seconds()

		// Measure read performance
		hashes := make([]string, batchSize)
		for i := range hashes {
			hashes[i] = fmt.Sprintf("tune_%d", i)
		}

		readStart := time.Now()
		result := db.BatchGetTransactions(hashes)
		readDuration := time.Since(readStart)

		if len(result.Transactions) != batchSize {
			continue
		}

		readThroughput := float64(batchSize) / readDuration.Seconds()

		// Weight read performance more heavily in the decision
		if memUsed < targetMem {
			// Prioritize read performance (2x weight)
			effectiveThroughput := readThroughput*2 + writeThroughput

			if effectiveThroughput > (bestReadThroughput*2 + bestWriteThroughput) {
				bestWriteThroughput = writeThroughput
				bestReadThroughput = readThroughput
				optimal.WriteBatchSize = batchSize
				optimal.ReadBatchSize = batchSize
				optimal.MemoryLimit = memUsed
				optimal.TargetLatency = writeDuration
				optimal.MaxThroughput = readThroughput // Focus on read throughput
			}
		}

		time.Sleep(cooldown)
	}

	// Adjust read batch size independently
	optimal.ReadBatchSize = optimal.WriteBatchSize / 4 // Smaller read batches for better throughput

	return optimal
}
