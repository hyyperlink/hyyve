package hyyve

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// createTestDB creates and returns a DB with cleanup function
func createTestDB(t *testing.T) (*DB, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "hyyve-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	db, err := Open(Options{
		FilePath: filepath.Join(dir, "test.hv"),
	})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to open test db: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	return db, cleanup
}

// createTestTransaction creates a transaction with known data
func createTestTransaction() *Transaction {
	return &Transaction{
		Timestamp: time.Now().UnixNano(),
		Hash:      "0123456789abcdef0123456789abcdef",
		From:      "abcdef0123456789abcdef0123456789",
		Signature: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		Changes: []TransactionChange{{
			To:              "1111111111111111111111111111111111111111",
			Amount:          1000,
			InstructionType: "transfer",
			InstructionData: []byte(`{"memo":"test"}`),
		}},
		References: []string{},
		Fee:        500,
	}
}

// writeCorruptData writes corrupted data at the current offset
func writeCorruptData(db *DB) (offset int64, err error) {
	offset, err = db.file.Seek(0, 2) // Seek to end
	if err != nil {
		return 0, err
	}

	// Write invalid header size
	header := make([]byte, HeaderSize-1) // Truncated header
	_, err = db.file.Write(header)
	return offset, err
}

// compareTransactions compares two transactions field by field
func compareTransactions(t *testing.T, expected, actual *Transaction) {
	t.Helper()

	if expected.Timestamp != actual.Timestamp {
		t.Errorf("timestamp mismatch: want %d, got %d", expected.Timestamp, actual.Timestamp)
	}
	if expected.Hash != actual.Hash {
		t.Errorf("hash mismatch: want %s, got %s", expected.Hash, actual.Hash)
	}
	if expected.From != actual.From {
		t.Errorf("from mismatch: want %s, got %s", expected.From, actual.From)
	}
	if expected.Signature != actual.Signature {
		t.Errorf("signature mismatch: want %s, got %s", expected.Signature, actual.Signature)
	}
	if expected.Fee != actual.Fee {
		t.Errorf("fee mismatch: want %d, got %d", expected.Fee, actual.Fee)
	}

	if len(expected.Changes) != len(actual.Changes) {
		t.Errorf("changes length mismatch: want %d, got %d", len(expected.Changes), len(actual.Changes))
		return
	}

	// Compare changes field by field since they contain json.RawMessage
	for i := range expected.Changes {
		if expected.Changes[i].To != actual.Changes[i].To {
			t.Errorf("change %d 'to' mismatch: want %s, got %s", i, expected.Changes[i].To, actual.Changes[i].To)
		}
		if expected.Changes[i].Amount != actual.Changes[i].Amount {
			t.Errorf("change %d amount mismatch: want %d, got %d", i, expected.Changes[i].Amount, actual.Changes[i].Amount)
		}
		if expected.Changes[i].InstructionType != actual.Changes[i].InstructionType {
			t.Errorf("change %d instruction type mismatch: want %s, got %s", i, expected.Changes[i].InstructionType, actual.Changes[i].InstructionType)
		}
		// Compare InstructionData as strings since they're json.RawMessage
		if string(expected.Changes[i].InstructionData) != string(actual.Changes[i].InstructionData) {
			t.Errorf("change %d instruction data mismatch: want %s, got %s",
				i,
				string(expected.Changes[i].InstructionData),
				string(actual.Changes[i].InstructionData))
		}
	}

	if len(expected.References) != len(actual.References) {
		t.Errorf("references length mismatch: want %d, got %d", len(expected.References), len(actual.References))
		return
	}
	for i := range expected.References {
		if expected.References[i] != actual.References[i] {
			t.Errorf("reference %d mismatch: want %s, got %s", i, expected.References[i], actual.References[i])
		}
	}
}

func TestReadTransactionFromOffset(t *testing.T) {
	t.Run("basic transaction", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		// Create a simple transaction (like a genesis/initial tx)
		tx := createTestTransaction()
		offset, err := db.file.Seek(0, 2) // Get current offset
		if err != nil {
			t.Fatalf("failed to get offset: %v", err)
		}

		// Write it directly using internal methods
		if err := db.setTransactionInternal(tx); err != nil {
			t.Fatalf("failed to write transaction: %v", err)
		}

		// Read it back
		got, err := db.readTransactionFromOffset(offset)
		if err != nil {
			t.Fatalf("failed to read transaction: %v", err)
		}

		compareTransactions(t, tx, got)
	})

	t.Run("with variable data", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		// Create transaction with actual instruction data
		tx := createTestTransaction()
		tx.Changes = []TransactionChange{{
			To:              "recipient",
			Amount:          1000,
			InstructionType: "transfer",
			InstructionData: json.RawMessage(`{"memo":"test payment"}`),
		}}

		offset, err := db.file.Seek(0, 2)
		if err != nil {
			t.Fatalf("failed to get offset: %v", err)
		}

		if err := db.setTransactionInternal(tx); err != nil {
			t.Fatalf("failed to write transaction: %v", err)
		}

		got, err := db.readTransactionFromOffset(offset)
		if err != nil {
			t.Fatalf("failed to read transaction: %v", err)
		}

		compareTransactions(t, tx, got)
	})

	t.Run("error cases", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		tests := []struct {
			name    string
			offset  int64
			wantErr error
		}{
			{
				name:    "invalid offset",
				offset:  999999,
				wantErr: io.EOF,
			},
			{
				name:    "zero offset",
				offset:  0,
				wantErr: io.EOF, // Empty DB
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := db.readTransactionFromOffset(tt.offset)
				if err == nil {
					t.Error("expected error, got nil")
				}
			})
		}
	})

	t.Run("data corruption", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		// Write a valid transaction first
		tx := createTestTransaction()
		offset, err := db.file.Seek(0, 2)
		if err != nil {
			t.Fatalf("failed to get offset: %v", err)
		}

		if err := db.setTransactionInternal(tx); err != nil {
			t.Fatalf("failed to write transaction: %v", err)
		}

		// Corrupt the header by writing invalid timestamp and sizes
		corruptHeader := RecordHeader{
			Timestamp:   -1,     // Invalid timestamp
			RefCount:    0xFFFF, // Impossibly large
			ChangeCount: 0xFFFF, // Impossibly large
			Fee:         0xFFFF,
		}
		headerBytes, _ := corruptHeader.MarshalBinary()
		if _, err := db.file.WriteAt(headerBytes, offset); err != nil {
			t.Fatalf("failed to corrupt data: %v", err)
		}

		// Try to read
		_, err = db.readTransactionFromOffset(offset)
		if err == nil || err == io.EOF {
			t.Errorf("expected corruption error, got %v", err)
		}
	})
}

func TestReadTransactionFromOffset_Corrupted(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	offset, err := writeCorruptData(db)
	if err != nil {
		t.Fatalf("failed to write corrupt data: %v", err)
	}

	_, err = db.readTransactionFromOffset(offset)
	if err == nil {
		t.Error("expected error reading corrupted data, got nil")
	}
}

func TestLoadIndex(t *testing.T) {
	db, cleanup := createTestDB(t)

	// Write a single known transaction first
	tx := createTestTransaction()
	if err := db.SetTransaction(tx); err != nil {
		t.Fatalf("failed to write transaction: %v", err)
	}

	// Close and reopen the DB
	db.Close()
	db2, err := Open(Options{FilePath: db.filepath})
	if err != nil {
		t.Fatalf("failed to reopen db: %v", err)
	}
	defer cleanup()

	// Verify transaction was reloaded
	got, err := db2.GetTransaction(tx.Hash)
	if err != nil {
		t.Fatalf("failed to get transaction: %v", err)
	}
	compareTransactions(t, tx, got)
}

func TestConcurrentAccess(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// Write initial transaction
	tx := createTestTransaction()
	if err := db.SetTransaction(tx); err != nil {
		t.Fatalf("failed to write initial transaction: %v", err)
	}

	const goroutines = 10
	errc := make(chan error, goroutines*100)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				got, err := db.GetTransaction(tx.Hash)
				if err != nil {
					errc <- fmt.Errorf("read error: %v", err)
					return
				}
				if got.Hash != tx.Hash {
					errc <- fmt.Errorf("hash mismatch: want %s, got %s", tx.Hash, got.Hash)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errc)

	for err := range errc {
		t.Error(err)
	}
}

func TestReferenceValidation(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// Try to store tx2 before tx1 exists
	tx2 := createTestTransaction()
	tx2.References = []string{"nonexistent_tx"}

	err := db.SetTransaction(tx2)
	if !errors.Is(err, ErrInvalidReference) {
		t.Errorf("expected ErrInvalidReference, got %v", err)
	}
}

func TestMultipleReferences(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// Create first transaction
	tx1 := createTestTransaction()
	tx1.Hash = "tx1"
	tx1.References = []string{} // Use empty slice instead of nil

	if err := db.SetTransaction(tx1); err != nil {
		t.Fatal(err)
	}

	// Verify tx1 exists
	_, err := db.GetTransaction("tx1")
	if err != nil {
		t.Fatalf("tx1 not found: %v", err)
	}

	// Create second transaction that references the first
	tx2 := createTestTransaction()
	tx2.Hash = "tx2"
	tx2.References = []string{"tx1"}

	if err := db.SetTransaction(tx2); err != nil {
		t.Fatal(err)
	}
}

func TestAddressHistoryOrder(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	addr := "test_address"
	now := time.Now().UnixNano()

	// Create transactions at different times
	txs := []*Transaction{
		{Hash: "tx1", From: addr, Timestamp: now - 2},
		{Hash: "tx2", From: addr, Timestamp: now - 1},
		{Hash: "tx3", From: addr, Timestamp: now},
	}

	for _, tx := range txs {
		if err := db.SetTransaction(tx); err != nil {
			t.Fatal(err)
		}
	}

	// Get history
	history, err := db.GetAddressTransactions(addr, 0, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Verify newest first order
	if len(history) != 3 {
		t.Fatalf("expected 3 transactions, got %d", len(history))
	}
	if history[0].Hash != "tx3" || history[2].Hash != "tx1" {
		t.Error("transactions not in correct order")
	}
}

func TestReferenceScenarios(t *testing.T) {
	t.Run("missing reference", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		tx := createTestTransaction()
		tx.References = []string{"nonexistent"}

		err := db.SetTransaction(tx)
		if !errors.Is(err, ErrInvalidReference) {
			t.Errorf("expected ErrInvalidReference, got %v", err)
		}
	})
}

func TestDuplicateReferences(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// Create base transaction
	tx1 := createTestTransaction()
	tx1.Hash = "tx1"
	if err := db.SetTransaction(tx1); err != nil {
		t.Fatal(err)
	}

	// Create transaction with duplicate references
	tx2 := createTestTransaction()
	tx2.Hash = "tx2"
	tx2.References = []string{"tx1", "tx1"} // Duplicate reference

	err := db.SetTransaction(tx2)
	if !errors.Is(err, ErrInvalidReference) {
		t.Errorf("expected ErrInvalidReference for duplicate reference, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "duplicate reference") {
		t.Errorf("expected error message to mention duplicate reference, got %v", err)
	}
}

func TestCanArchive(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// Create base transaction
	tx1 := createTestTransaction()
	tx1.Hash = "tx1"
	if err := db.SetTransaction(tx1); err != nil {
		t.Fatal(err)
	}

	// Initially should be archivable (no references)
	if !db.CanArchive("tx1") {
		t.Error("tx1 should be archivable when it has no references")
	}

	// Create transaction that references tx1
	tx2 := createTestTransaction()
	tx2.Hash = "tx2"
	tx2.References = []string{"tx1"}
	if err := db.SetTransaction(tx2); err != nil {
		t.Fatal(err)
	}

	// Now tx1 should not be archivable
	if db.CanArchive("tx1") {
		t.Error("tx1 should not be archivable when referenced by tx2")
	}

	// tx2 should be archivable (nothing references it)
	if !db.CanArchive("tx2") {
		t.Error("tx2 should be archivable when it has no references")
	}
}

func TestReferenceQueries(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// Create a chain of transactions: tx1 <- tx2 <- tx3
	tx1 := createTestTransaction()
	tx1.Hash = "tx1"
	if err := db.SetTransaction(tx1); err != nil {
		t.Fatal(err)
	}

	tx2 := createTestTransaction()
	tx2.Hash = "tx2"
	tx2.References = []string{"tx1"}
	if err := db.SetTransaction(tx2); err != nil {
		t.Fatal(err)
	}

	tx3 := createTestTransaction()
	tx3.Hash = "tx3"
	tx3.References = []string{"tx2"}
	if err := db.SetTransaction(tx3); err != nil {
		t.Fatal(err)
	}

	// Check forward references (what each tx references)
	fwd1, err := db.GetForwardRefs("tx1")
	if err != nil {
		t.Errorf("unexpected error for tx1 forward refs: %v", err)
	}
	if len(fwd1) != 0 {
		t.Errorf("tx1 should have no forward refs, got %v", fwd1)
	}

	fwd2, err := db.GetForwardRefs("tx2")
	if err != nil {
		t.Errorf("failed to get tx2 forward refs: %v", err)
	}
	if len(fwd2) != 1 || fwd2[0] != "tx1" {
		t.Errorf("tx2 should reference tx1, got %v", fwd2)
	}

	// Check backward references (what references each tx)
	back1, err := db.GetBackwardRefs("tx1")
	if err != nil {
		t.Errorf("failed to get tx1 backward refs: %v", err)
	}
	if len(back1) != 1 || back1[0] != "tx2" {
		t.Errorf("tx1 should be referenced by tx2, got %v", back1)
	}

	back2, err := db.GetBackwardRefs("tx2")
	if err != nil {
		t.Errorf("failed to get tx2 backward refs: %v", err)
	}
	if len(back2) != 1 || back2[0] != "tx3" {
		t.Errorf("tx2 should be referenced by tx3, got %v", back2)
	}

	// Test non-existent transaction
	_, err = db.GetForwardRefs("nonexistent")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound for nonexistent tx, got %v", err)
	}
}

func TestBatchGetTransactions(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// Create several transactions
	txs := make([]*Transaction, 3)
	for i := range txs {
		tx := createTestTransaction()
		tx.Hash = fmt.Sprintf("tx%d", i)
		tx.Changes[0].Amount = uint64(1000 * (i + 1)) // Different amounts to verify correct retrieval
		txs[i] = tx
		if err := db.SetTransaction(tx); err != nil {
			t.Fatalf("failed to store tx%d: %v", i, err)
		}
	}

	// Test batch retrieval
	hashes := []string{"tx0", "tx1", "nonexistent", "tx2"}
	result := db.BatchGetTransactions(hashes)

	// Verify successful retrievals
	for i := range txs {
		hash := fmt.Sprintf("tx%d", i)
		got, exists := result.Transactions[hash]
		if !exists {
			t.Errorf("transaction %s not found in results", hash)
			continue
		}
		if got.Changes[0].Amount != uint64(1000*(i+1)) {
			t.Errorf("wrong amount for tx%d: want %d, got %d", i, 1000*(i+1), got.Changes[0].Amount)
		}
	}

	// Verify error for nonexistent transaction
	err, exists := result.Errors["nonexistent"]
	if !exists {
		t.Error("expected error for nonexistent transaction")
	} else if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func humanReadableSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func BenchmarkAll(b *testing.B) {
	db, cleanup := createBenchDB(b)
	defer cleanup()

	config := db.AutoTuneBatchSize()
	b.Logf("\nAuto-tuned Configuration:")
	b.Logf("=======================")
	b.Logf("Write Batch Size: %d", config.WriteBatchSize)
	b.Logf("Read Batch Size:  %d", config.ReadBatchSize)
	b.Logf("Memory Usage:     %s", humanReadableSize(config.MemoryLimit))
	b.Logf("Target Latency:   %v", config.TargetLatency)
	b.Logf("Max Throughput:   %.2f TPS", config.MaxThroughput)

	var results strings.Builder
	results.WriteString("\nTransaction Processing Speed Summary:\n")
	results.WriteString("=====================================\n")
	results.WriteString("Batch Size |  Batch Data  |    DB Size    |    Write TPS    |    Read TPS     \n")
	results.WriteString("-----------|--------------|---------------|-----------------|----------------\n")

	// Test around the optimal sizes
	writeSizes := []int{
		config.WriteBatchSize / 10,
		config.WriteBatchSize / 2,
		config.WriteBatchSize,
		config.WriteBatchSize * 2,
	}

	// Run benchmarks with different combinations
	for _, writeSize := range writeSizes {
		// Calculate batch data size
		txSize := int64(writeSize * 1024)
		batchSize := humanReadableSize(txSize)

		var writeTime time.Duration
		var readTime time.Duration
		var writeOps int
		var readOps int

		// Measure write speed
		b.Run(fmt.Sprintf("write_batch_%d", writeSize), func(b *testing.B) {
			start := time.Now()
			writeOps = b.N
			for i := 0; i < b.N; i++ {
				txs := make([]*Transaction, writeSize)
				for j := range txs {
					tx := createTestTransaction()
					tx.Hash = fmt.Sprintf("tune_%d", j)
					txs[j] = tx
				}
				if err := db.BatchSetTransactions(txs); err != nil {
					b.Fatalf("batch write failed: %v", err)
				}
			}
			writeTime = time.Since(start)
		})

		// Get DB size after writing
		dbInfo, err := os.Stat(db.filepath)
		if err != nil {
			b.Fatalf("failed to get db size: %v", err)
		}
		dbSize := humanReadableSize(dbInfo.Size())

		// Measure read speed
		b.Run(fmt.Sprintf("read_batch_%d", writeSize), func(b *testing.B) {
			start := time.Now()
			readOps = b.N
			for i := 0; i < b.N; i++ {
				hashes := make([]string, writeSize)
				for j := range hashes {
					hashes[j] = fmt.Sprintf("tune_%d", j)
				}
				result := db.BatchGetTransactions(hashes)
				if len(result.Transactions) != writeSize {
					b.Fatalf("expected %d transactions, got %d", writeSize, len(result.Transactions))
				}
			}
			readTime = time.Since(start)
		})

		writeTPS := float64(writeOps*writeSize) / writeTime.Seconds()
		readTPS := float64(readOps*writeSize) / readTime.Seconds()

		fmt.Fprintf(&results, "%10d | %12s | %13s | %15.2f | %14.2f\n",
			writeSize, batchSize, dbSize, writeTPS, readTPS)
	}

	b.Log(results.String())
}

func BenchmarkOperations(b *testing.B) {
	// Create test DB
	dir, err := os.MkdirTemp("", "hyyve-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{
		FilePath: filepath.Join(dir, "bench.hv"),
	})
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Test different batch sizes
	for _, batchSize := range []int{1, 10, 100, 1000} {
		// Create test transactions
		txs := make([]*Transaction, batchSize)
		var hashes []string
		for i := range txs {
			tx := createTestTransaction()
			tx.Hash = fmt.Sprintf("tx%d", i)
			txs[i] = tx
			hashes = append(hashes, tx.Hash)
		}

		b.Run(fmt.Sprintf("write_batch_%d", batchSize), func(b *testing.B) {
			b.ResetTimer() // Don't count setup time
			for i := 0; i < b.N; i++ {
				if err := db.BatchSetTransactions(txs); err != nil {
					b.Fatalf("batch write failed: %v", err)
				}
			}
			b.SetBytes(int64(batchSize * 1024)) // Approximate tx size
		})

		// Write data for read benchmarks
		if err := db.BatchSetTransactions(txs); err != nil {
			b.Fatalf("failed to prepare read benchmark: %v", err)
		}

		b.Run(fmt.Sprintf("read_batch_%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result := db.BatchGetTransactions(hashes)
				if len(result.Transactions) != batchSize {
					b.Fatalf("expected %d transactions, got %d", batchSize, len(result.Transactions))
				}
			}
			b.SetBytes(int64(batchSize * 1024))
		})

		b.Run(fmt.Sprintf("read_sequential_%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, hash := range hashes {
					if _, err := db.GetTransaction(hash); err != nil {
						b.Fatalf("sequential read failed: %v", err)
					}
				}
			}
			b.SetBytes(int64(batchSize * 1024))
		})
	}
}

func BenchmarkProfile(b *testing.B) {
	// Create test DB
	dir, err := os.MkdirTemp("", "hyyve-profile-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	db, err := Open(Options{
		FilePath: filepath.Join(dir, "profile.hv"),
	})
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create test transactions
	const batchSize = 100
	txs := make([]*Transaction, batchSize)
	var hashes []string
	for i := range txs {
		tx := createTestTransaction()
		tx.Hash = fmt.Sprintf("tx%d", i)
		txs[i] = tx
		hashes = append(hashes, tx.Hash)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Write batch
		if err := db.BatchSetTransactions(txs); err != nil {
			b.Fatalf("batch write failed: %v", err)
		}

		// Read batch
		result := db.BatchGetTransactions(hashes)
		if len(result.Transactions) != batchSize {
			b.Fatalf("expected %d transactions, got %d", batchSize, len(result.Transactions))
		}
	}
}

// Add helper for benchmarks
func createBenchDB(b *testing.B) (*DB, func()) {
	b.Helper()

	dir, err := os.MkdirTemp("", "hyyve-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}

	db, err := Open(Options{
		FilePath: filepath.Join(dir, "bench.hv"),
	})
	if err != nil {
		os.RemoveAll(dir)
		b.Fatalf("failed to open db: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	return db, cleanup
}

func BenchmarkAutoTune(b *testing.B) {
	db, cleanup := createBenchDB(b)
	defer cleanup()

	config := db.AutoTuneBatchSize()
	b.Logf("\nAuto-tuned Configuration:")
	b.Logf("=======================")
	b.Logf("Write Batch Size: %d", config.WriteBatchSize)
	b.Logf("Read Batch Size:  %d", config.ReadBatchSize)
	b.Logf("Memory Usage:     %s", humanReadableSize(config.MemoryLimit))
	b.Logf("Target Latency:   %v", config.TargetLatency)
	b.Logf("Max Throughput:   %.2f TPS", config.MaxThroughput)
}
