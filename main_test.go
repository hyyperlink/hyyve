package hyyve

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"crypto/ed25519"

	"encoding/hex"

	"github.com/mr-tron/base58"
	"golang.org/x/crypto/sha3"
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
		Hash:      "29fcb4a4281a05d4569deb542e0612a3c8ca6c08d242eaee724d2ac20ab31696",
		From:      "612a3c8ca6fc24d2a569deb542e0612c20abb4a42298",
		Signature: "d6a1183a14bda344fc101243ce4a91e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed",
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
	tx1.Hash = "d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed"
	tx1.References = []string{} // Use empty slice instead of nil

	if err := db.SetTransaction(tx1); err != nil {
		t.Fatal(err)
	}

	// Verify tx1 exists
	_, err := db.GetTransaction(tx1.Hash)
	if err != nil {
		t.Fatalf("tx1 not found: %v", err)
	}

	// Create second transaction that references the first
	tx2 := createTestTransaction()
	tx2.Hash = "410f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47fabbdb"
	tx2.References = []string{"d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed"}

	if err := db.SetTransaction(tx2); err != nil {
		t.Fatal(err)
	}
}

func TestAddressHistoryOrder(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	addr := "612a3c8ca6fc24d2a569deb542e0612c20abb4a42298"
	now := time.Now().UnixNano()

	// Create transactions at different times
	txs := []*Transaction{
		{Hash: "110f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47fabbdb", From: addr, Timestamp: now - 2, Signature: "d6a1183a14bda344fc101243ce4a91e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed"},
		{Hash: "210f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47fabbdb", From: addr, Timestamp: now - 1, Signature: "d6a1183a14bda344fc101243ce4a91e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed"},
		{Hash: "310f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47fabbdb", From: addr, Timestamp: now, Signature: "d6a1183a14bda344fc101243ce4a91e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed"},
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
	if history[0].Hash != "310f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47fabbdb" || history[2].Hash != "110f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47fabbdb" {
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
	tx1.Hash = "d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed"
	if err := db.SetTransaction(tx1); err != nil {
		t.Fatal(err)
	}

	// Create transaction with duplicate references
	tx2 := createTestTransaction()
	tx2.Hash = "410f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47fabbdb"
	tx2.References = []string{"d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed", "d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed"} // Duplicate reference

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
	tx1.Hash = "d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed"
	if err := db.SetTransaction(tx1); err != nil {
		t.Fatal(err)
	}

	// Initially should be archivable (no references)
	if !db.CanArchive("tx1") {
		t.Error("tx1 should be archivable when it has no references")
	}

	// Create transaction that references tx1
	tx2 := createTestTransaction()
	tx2.Hash = "410f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47fabbdb"
	tx2.References = []string{"d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed"}
	if err := db.SetTransaction(tx2); err != nil {
		t.Fatal(err)
	}

	// Now tx1 should not be archivable
	if db.CanArchive("d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e008eed") {
		t.Error("tx1 should not be archivable when referenced by tx2")
	}

	// tx2 should be archivable (nothing references it)
	if !db.CanArchive("410f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47fabbdb") {
		t.Error("tx2 should be archivable when it has no references")
	}
}

func TestReferenceQueries(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// Create a chain of transactions: tx1 <- tx2 <- tx3
	tx1 := createTestTransaction()
	tx1.Hash = "hash1_d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e"
	tx1.References = []string{}
	if err := db.SetTransaction(tx1); err != nil {
		t.Fatal(err)
	}

	tx2 := createTestTransaction()
	tx2.Hash = "hash2_410f47fabbdb4cfd4eef5297e008eedd6a118e1cc10f47fabbdb410f47"
	tx2.References = []string{tx1.Hash}
	if err := db.SetTransaction(tx2); err != nil {
		t.Fatal(err)
	}

	tx3 := createTestTransaction()
	tx3.Hash = "hash3_d6a118e1cc10f47fabbdb410f47fabbdb410f47fabbdb4cfd4eef5297e"
	tx3.References = []string{tx2.Hash}
	if err := db.SetTransaction(tx3); err != nil {
		t.Fatal(err)
	}

	// Check forward references (what each tx references)
	fwd1, err := db.GetForwardRefs(tx1.Hash)
	if err != nil {
		t.Errorf("unexpected error for tx1 forward refs: %v", err)
	}
	if len(fwd1) != 0 {
		t.Errorf("tx1 should have no forward refs, got %v", fwd1)
	}

	fwd2, err := db.GetForwardRefs(tx2.Hash)
	if err != nil {
		t.Errorf("failed to get tx2 forward refs: %v", err)
	}
	if len(fwd2) != 1 || fwd2[0] != tx1.Hash {
		t.Errorf("tx2 should reference tx1, got %v", fwd2)
	}

	// Check backward references (what references each tx)
	back1, err := db.GetBackwardRefs(tx1.Hash)
	if err != nil {
		t.Errorf("failed to get tx1 backward refs: %v", err)
	}
	if len(back1) != 1 || back1[0] != tx2.Hash {
		t.Errorf("tx1 should be referenced by tx2, got %v", back1)
	}

	back2, err := db.GetBackwardRefs(tx2.Hash)
	if err != nil {
		t.Errorf("failed to get tx2 backward refs: %v", err)
	}
	if len(back2) != 1 || back2[0] != tx3.Hash {
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
		tx.Hash = fmt.Sprintf("hash%d_03d0f902018561802b66b0408b578373d02947e47d4e2a477526856f66", i)
		tx.Changes[0].Amount = uint64(1000 * (i + 1)) // Different amounts to verify correct retrieval
		txs[i] = tx
		if err := db.SetTransaction(tx); err != nil {
			t.Fatalf("failed to store tx%d: %v", i, err)
		}
	}

	// Test batch retrieval
	hashes := []string{
		"hash0_03d0f902018561802b66b0408b578373d02947e47d4e2a477526856f66",
		"hash1_03d0f902018561802b66b0408b578373d02947e47d4e2a477526856f66",
		"nonexistent",
		"hash2_03d0f902018561802b66b0408b578373d02947e47d4e2a477526856f66",
	}
	result := db.BatchGetTransactions(hashes)

	// Verify successful retrievals
	for i := range txs {
		hash := fmt.Sprintf("hash%d_03d0f902018561802b66b0408b578373d02947e47d4e2a477526856f66", i)
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

	if config.WriteBatchSize == 0 {
		b.Fatalf("write batch size is 0")
	}
	if config.ReadBatchSize == 0 {
		b.Fatalf("read batch size is 0")
	}

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
					sha3Sum256HashBytes := sha3.Sum256([]byte(fmt.Sprintf("tune_%d", j)))
					txHash := hex.EncodeToString(sha3Sum256HashBytes[:])
					tx := createTestTransaction()
					tx.Hash = txHash
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
					sha3Sum256HashBytes := sha3.Sum256([]byte(fmt.Sprintf("tune_%d", j)))
					hashes[j] = hex.EncodeToString(sha3Sum256HashBytes[:])
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
			sha3Sum256HashBytes := sha3.Sum256([]byte(fmt.Sprintf("tx%d", i)))
			txHash := hex.EncodeToString(sha3Sum256HashBytes[:])
			tx := createTestTransaction()
			tx.Hash = txHash
			txs[i] = tx
			hashes = append(hashes, txHash)
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
		sha3Sum256HashBytes := sha3.Sum256([]byte(fmt.Sprintf("tx%d", i)))
		txHash := hex.EncodeToString(sha3Sum256HashBytes[:])
		tx := createTestTransaction()
		tx.Hash = txHash
		txs[i] = tx
		hashes = append(hashes, txHash)
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

func TestSignatureLengths(t *testing.T) {
	// Create a test key pair
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	// Create a bunch of test messages and collect signature lengths
	lengths := make(map[int]int) // length -> count
	minLen := 999999
	maxLen := 0
	maxSig := ""

	// Test 10,000 messages to be more thorough
	for i := 0; i < 10000; i++ {
		msg := fmt.Sprintf("test message %d", i)
		sig := ed25519.Sign(priv, []byte(msg))

		base58Sig := base58.Encode(sig)
		length := len(base58Sig)

		lengths[length]++

		if length < minLen {
			minLen = length
		}
		if length > maxLen {
			maxLen = length
			maxSig = base58Sig
		}
	}

	t.Logf("Signature length analysis:")
	t.Logf("Min length: %d", minLen)
	t.Logf("Max length: %d", maxLen)
	t.Logf("Longest signature: %s", maxSig)
	t.Logf("Distribution:")
	for length, count := range lengths {
		percentage := float64(count) / 100.0
		t.Logf("  %d chars: %d signatures (%.2f%%)", length, count, percentage)
	}

	// Verify our constant is correct
	if maxLen > SignatureSize {
		t.Errorf("SignatureSize constant %d is too small, found signature of length %d",
			SignatureSize, maxLen)
	}
}

func TestBloomFilterMightContain(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		// Create a new bloom filter
		bf := NewBloomFilter()

		// Real transaction hashes from the transaction data provided
		hashes := []string{
			"8c3e73b8a8a8996485693057f8d141ac9cf81a01486071e1bbe025d2f0570fa3",
			"e99ff77fc4dd8563fcb8862d98a36bab05418d4b876186947af7271b4e5b0e9f",
			"c57ade8fc059ece500e7a01b3a4766187df93ac53edd196b252f8801b75ce067",
		}

		// Add the items to the filter
		for _, hash := range hashes {
			bf.Add(hash)
		}

		// Test items that were added (should return true)
		for _, hash := range hashes {
			if !bf.MightContain(hash) {
				t.Errorf("MightContain(%q) = false, want true", hash)
			}
		}

		// Test items that were not added (should return false)
		notAddedHashes := []string{
			"482385ca509ac2a0b4cb0392ca354ba4d00a3a4245c57756e7f07decc43cc641",
			"67e0b4967d4d557b47576e2875bca440dea438697d38036441314d2ebc0b6fce",
			"51680c0dbc21b3f9403604cbf5c50ea106258755a91579759cc27f9f96e32f69",
		}

		// These should not be in the filter, but there's a small chance of false positives
		falsePositives := 0
		for _, hash := range notAddedHashes {
			if bf.MightContain(hash) {
				falsePositives++
			}
		}

		// Given the size of our filter, we expect very few false positives
		// for just 3 items. Zero is most likely, but we'll allow up to 1.
		if falsePositives > 1 {
			t.Errorf("Got %d false positives out of %d items, expected at most 1",
				falsePositives, len(notAddedHashes))
		}
	})

	t.Run("multiple items with similar hashes", func(t *testing.T) {
		bf := NewBloomFilter()

		// Addresses from the sample transactions
		addresses := []string{
			"DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd", // From address
			"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE", // To address
		}

		// Add addresses to the filter
		for _, addr := range addresses {
			bf.Add(addr)
		}

		// Verify all added addresses return true
		for _, addr := range addresses {
			if !bf.MightContain(addr) {
				t.Errorf("Added address %q not found in bloom filter", addr)
			}
		}

		// Check similar but different addresses
		similarAddresses := []string{
			"DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpc", // Last char changed
			"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwF", // Last char changed
		}

		// These should mostly return false, but some false positives are possible
		falsePositives := 0
		for _, addr := range similarAddresses {
			if bf.MightContain(addr) {
				falsePositives++
			}
		}

		// Allow some false positives, but not too many
		if falsePositives == len(similarAddresses) {
			t.Errorf("All similar addresses returned true, suggesting poor hash distribution")
		}
	})

	t.Run("empty items", func(t *testing.T) {
		bf := NewBloomFilter()

		// Empty strings should still be hashable
		bf.Add("")

		if !bf.MightContain("") {
			t.Error("Empty string not found after adding")
		}

		// Adding a non-empty string shouldn't make MightContain return true for other strings
		bf = NewBloomFilter()
		bf.Add("8c3e73b8a8a8996485693057f8d141ac9cf81a01486071e1bbe025d2f0570fa3")

		if bf.MightContain("") {
			t.Error("Empty string should not be found when not added")
		}
	})

	t.Run("large number of elements", func(t *testing.T) {
		bf := NewBloomFilter()

		// Create a large set of realistic transaction hashes
		var hashes []string
		for i := 0; i < 1000; i++ {
			// Generate a hash-like string based on a pattern from the sample
			hash := fmt.Sprintf("%032x%032x", i, i*2+1)
			hashes = append(hashes, hash)
		}

		// Add all hashes to the filter
		for _, hash := range hashes {
			bf.Add(hash)
		}

		// Check that all added hashes are found (no false negatives)
		for _, hash := range hashes {
			if !bf.MightContain(hash) {
				t.Errorf("Added hash %q not found in bloom filter", hash)
			}
		}

		// Generate some hashes that weren't added
		var notAddedHashes []string
		for i := 1000; i < 1100; i++ {
			hash := fmt.Sprintf("%032x%032x", i, i*2+1)
			notAddedHashes = append(notAddedHashes, hash)
		}

		// Count false positives
		falsePositives := 0
		for _, hash := range notAddedHashes {
			if bf.MightContain(hash) {
				falsePositives++
			}
		}

		// For BloomFilterSize = 1<<24 and BloomHashCount = 8, with 1000 items,
		// the false positive rate should be extremely low
		falsePositiveRate := float64(falsePositives) / float64(len(notAddedHashes))
		if falsePositiveRate > 0.05 { // Allow up to 5% false positives
			t.Errorf("False positive rate too high: %f", falsePositiveRate)
		}
	})

	t.Run("signatures and transactions", func(t *testing.T) {
		bf := NewBloomFilter()

		// Add some realistic signatures from the sample data
		signatures := []string{
			"43ijbQTetb2e6LdzRd6Yfdo1fg3jyAhMWEu44UWw5RcMB459PJPMM68kZ6MsgVkDWmR4KHwoJse3tirzPHr5nW16",
			"2eCFzDh6a1dsmBf4AFVEmUBxSTagSQtqQoMHiq6Ts2D4idhJM3QLTR49ZmMe9Dy8mmxmZoxnRhRCdxRrZZphTNYX",
			"4Ho32ymZ8JM8XQ2NBQUNFK175ooLo4bzzJvkimhbgcPNLopDqc9fL11jehZnCL1joEvcVHyN3kSH4e9vSv31iRZ7",
		}

		// Add transaction hashes
		txHashes := []string{
			"8c3e73b8a8a8996485693057f8d141ac9cf81a01486071e1bbe025d2f0570fa3",
			"e99ff77fc4dd8563fcb8862d98a36bab05418d4b876186947af7271b4e5b0e9f",
			"c57ade8fc059ece500e7a01b3a4766187df93ac53edd196b252f8801b75ce067",
		}

		// Add both signatures and transaction hashes
		for _, sig := range signatures {
			bf.Add(sig)
		}

		for _, hash := range txHashes {
			bf.Add(hash)
		}

		// Verify all items are found
		for _, sig := range signatures {
			if !bf.MightContain(sig) {
				t.Errorf("Signature %q not found after adding", sig)
			}
		}

		for _, hash := range txHashes {
			if !bf.MightContain(hash) {
				t.Errorf("Transaction hash %q not found after adding", hash)
			}
		}

		// Verify we don't get false positives for similar but different items
		// Note: there's always a small chance of false positives, so this is probabilistic
		modifiedHashes := make([]string, len(txHashes))
		for i, hash := range txHashes {
			// Change one character in the middle
			midpoint := len(hash) / 2
			modified := hash[:midpoint] + "f" + hash[midpoint+1:]
			modifiedHashes[i] = modified
		}

		falsePositives := 0
		for _, hash := range modifiedHashes {
			if bf.MightContain(hash) {
				falsePositives++
			}
		}

		// The probability of false positives should be low for a small number of items
		if falsePositives > 1 {
			t.Logf("Note: Got %d false positives out of %d modified hashes",
				falsePositives, len(modifiedHashes))
		}
	})
}

func TestSkipListInsertDuplicateTimestamp(t *testing.T) {
	// Create a new skip list
	sl := NewSkipList()

	// Use realistic timestamp
	timestamp := int64(1742443164867545494)

	// Use realistic transaction hashes
	hash1 := "8c3e73b8a8a8996485693057f8d141ac9cf81a01486071e1bbe025d2f0570fa3"
	hash2 := "e99ff77fc4dd8563fcb8862d98a36bab05418d4b876186947af7271b4e5b0e9f"
	hash3 := "c57ade8fc059ece500e7a01b3a4766187df93ac53edd196b252f8801b75ce067"

	// First insertion creates a new node
	sl.Insert(timestamp, hash1)

	// Find the node for verification
	current := sl.head
	for i := 0; i < sl.maxLevel; i++ {
		for current.forward[i] != nil && current.forward[i].key < timestamp {
			current = current.forward[i]
		}
	}

	// Move to the actual node with our timestamp
	current = current.forward[0]

	// Verify the first hash was inserted
	if current == nil || current.key != timestamp {
		t.Fatalf("Failed to find node with timestamp %d", timestamp)
	}

	if len(current.value) != 1 || current.value[0] != hash1 {
		t.Errorf("Expected value [%s], got %v", hash1, current.value)
	}

	// Insert a second hash with the same timestamp (this tests the red part)
	sl.Insert(timestamp, hash2)

	// Verify both hashes are in the value slice
	if len(current.value) != 2 || current.value[0] != hash1 || current.value[1] != hash2 {
		t.Errorf("Expected values [%s, %s], got %v", hash1, hash2, current.value)
	}

	// Insert a third hash with the same timestamp
	sl.Insert(timestamp, hash3)

	// Verify all three hashes are in the value slice
	if len(current.value) != 3 {
		t.Errorf("Expected 3 values, got %d", len(current.value))
	}

	// Check the values are all there (regardless of order)
	foundHashes := make(map[string]bool)
	for _, h := range current.value {
		foundHashes[h] = true
	}

	if !foundHashes[hash1] || !foundHashes[hash2] || !foundHashes[hash3] {
		t.Errorf("Not all hashes were found in the node's value slice: %v", current.value)
	}
}

func TestOpenErrorCase(t *testing.T) {
	// Create a test directory that we'll make inaccessible
	dir, err := os.MkdirTemp("", "hyyve-test-open-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// First, let's verify that the normal case works with a valid path
	validPath := filepath.Join(dir, "valid.hv")
	db, err := Open(Options{
		FilePath: validPath,
	})
	if err != nil {
		t.Fatalf("failed to open DB with valid path: %v", err)
	}
	db.Close() // Clean up

	// Test case 1: Invalid path (directory that doesn't exist)
	invalidDirPath := filepath.Join(dir, "nonexistent-dir", "invalid.hv")
	_, err = Open(Options{
		FilePath: invalidDirPath,
	})
	if err == nil {
		t.Error("expected error when opening DB with invalid directory path, got nil")
	}

	// Test case 2: Path without permissions
	if runtime.GOOS != "windows" { // Skip on Windows as permissions work differently
		// Create a directory with no write permissions
		noPermDir := filepath.Join(dir, "noperm")
		if err := os.Mkdir(noPermDir, 0500); err != nil { // read + execute, but no write
			t.Fatalf("failed to create no-permission directory: %v", err)
		}

		noPermPath := filepath.Join(noPermDir, "noperm.hv")
		_, err = Open(Options{
			FilePath: noPermPath,
		})
		if err == nil {
			t.Error("expected error when opening DB with no-permission path, got nil")
		}
	}

	// Test case 3: Path is actually a directory, not a file
	dirAsFilePath := dir // Try to use the directory itself as a file
	_, err = Open(Options{
		FilePath: dirAsFilePath,
	})
	if err == nil {
		t.Error("expected error when opening DB with directory as file path, got nil")
	}
}

func TestOpenLoadIndexError(t *testing.T) {
	// Create a test directory
	dir, err := os.MkdirTemp("", "hyyve-test-loadindex-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a valid file path
	dbPath := filepath.Join(dir, "corrupt.hv")

	// First create a file with corrupted data that will cause loadIndex to fail
	file, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Write some invalid data that would cause loadIndex to fail
	// This simulates a corrupted database file
	corruptData := []byte{0x01, 0x02, 0x03} // Not enough bytes for a valid header
	if _, err := file.Write(corruptData); err != nil {
		file.Close()
		t.Fatalf("failed to write corrupt data: %v", err)
	}
	file.Close()

	// Now try to open the database with the corrupted file
	// This should cause loadIndex to fail, triggering the error path we want to test
	_, err = Open(Options{
		FilePath: dbPath,
	})

	// We expect an error
	if err == nil {
		t.Error("expected error when opening DB with corrupted file, got nil")
	}

	// Verify the file was closed by checking if we can open it again
	file, err = os.OpenFile(dbPath, os.O_RDWR, 0666)
	if err != nil {
		t.Errorf("failed to reopen test file, suggesting it wasn't properly closed: %v", err)
	} else {
		file.Close()
	}
}

func TestLoadIndexSeekToStartError(t *testing.T) {
	// Create temp directory
	dir, err := os.MkdirTemp("", "hyyve-test-loadindex-seek-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a real DB first
	dbPath := filepath.Join(dir, "test.hv")
	db, err := Open(Options{
		FilePath: dbPath,
	})
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}

	// Make a copy of the original file for restoration
	originalFile := db.file

	// Clean up properly at the end
	defer func() {
		// Restore original file before closing
		db.file = originalFile
		db.Close()
	}()

	// Create a closed file that will fail on Seek operations
	closedFile, err := os.CreateTemp(dir, "closed-*.tmp")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	closedFile.Close() // Close it to cause errors on operations

	// Replace the DB's file with our closed file
	db.file = closedFile

	// Now call loadIndex() - it should fail at the Seek(0, 0) operation
	err = db.loadIndex()

	// Verify we got the expected error
	if err == nil {
		t.Error("expected error from loadIndex with closed file, got nil")
	} else if !strings.Contains(err.Error(), "seek to start") {
		t.Errorf("expected error to contain 'seek to start', got: %v", err)
	}
}

func TestAddReference(t *testing.T) {
	t.Run("success case", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		// Create two transactions with realistic data
		tx1 := &Transaction{
			Timestamp: 1742443164867545494,
			Hash:      "8c3e73b8a8a8996485693057f8d141ac9cf81a01486071e1bbe025d2f0570fa3",
			From:      "DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd",
			Signature: "43ijbQTetb2e6LdzRd6Yfdo1fg3jyAhMWEu44UWw5RcMB459PJPMM68kZ6MsgVkDWmR4KHwoJse3tirzPHr5nW16",
			Changes: []TransactionChange{{
				To:              "DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE",
				Amount:          46653458980,
				InstructionType: "Transfer",
				InstructionData: json.RawMessage(`{"recipients":[{"address":"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE","amount":46653458980}]}`),
			}},
			References: []string{},
			Fee:        10920,
		}

		tx2 := &Transaction{
			Timestamp: 1742443165854655038,
			Hash:      "e99ff77fc4dd8563fcb8862d98a36bab05418d4b876186947af7271b4e5b0e9f",
			From:      "DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd",
			Signature: "2eCFzDh6a1dsmBf4AFVEmUBxSTagSQtqQoMHiq6Ts2D4idhJM3QLTR49ZmMe9Dy8mmxmZoxnRhRCdxRrZZphTNYX",
			Changes: []TransactionChange{{
				To:              "DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE",
				Amount:          3569938312,
				InstructionType: "Transfer",
				InstructionData: json.RawMessage(`{"recipients":[{"address":"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE","amount":3569938312}]}`),
			}},
			References: []string{},
			Fee:        8700,
		}

		// Store both transactions
		if err := db.SetTransaction(tx1); err != nil {
			t.Fatalf("failed to store tx1: %v", err)
		}
		if err := db.SetTransaction(tx2); err != nil {
			t.Fatalf("failed to store tx2: %v", err)
		}

		// Add reference from tx1 to tx2
		err := db.AddReference(tx1.Hash, tx2.Hash)
		if err != nil {
			t.Errorf("AddReference failed: %v", err)
		}

		// Verify forward reference was added
		forwards, err := db.GetForwardRefs(tx1.Hash)
		if err != nil {
			t.Errorf("GetForwardRefs failed: %v", err)
		}
		if len(forwards) != 1 || forwards[0] != tx2.Hash {
			t.Errorf("Expected forward ref from tx1 to tx2, got %v", forwards)
		}

		// Verify backward reference was added
		backwards, err := db.GetBackwardRefs(tx2.Hash)
		if err != nil {
			t.Errorf("GetBackwardRefs failed: %v", err)
		}
		if len(backwards) != 1 || backwards[0] != tx1.Hash {
			t.Errorf("Expected backward ref from tx2 to tx1, got %v", backwards)
		}
	})

	t.Run("multiple references", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		// Create three transactions with realistic data
		tx1 := &Transaction{
			Timestamp: 1742443164867545494,
			Hash:      "8c3e73b8a8a8996485693057f8d141ac9cf81a01486071e1bbe025d2f0570fa3",
			From:      "DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd",
			Signature: "43ijbQTetb2e6LdzRd6Yfdo1fg3jyAhMWEu44UWw5RcMB459PJPMM68kZ6MsgVkDWmR4KHwoJse3tirzPHr5nW16",
			Changes: []TransactionChange{{
				To:              "DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE",
				Amount:          46653458980,
				InstructionType: "Transfer",
				InstructionData: json.RawMessage(`{"recipients":[{"address":"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE","amount":46653458980}]}`),
			}},
			Fee: 10920,
		}

		tx2 := &Transaction{
			Timestamp: 1742443165854655038,
			Hash:      "e99ff77fc4dd8563fcb8862d98a36bab05418d4b876186947af7271b4e5b0e9f",
			From:      "DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd",
			Signature: "2eCFzDh6a1dsmBf4AFVEmUBxSTagSQtqQoMHiq6Ts2D4idhJM3QLTR49ZmMe9Dy8mmxmZoxnRhRCdxRrZZphTNYX",
			Changes: []TransactionChange{{
				To:              "DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE",
				Amount:          3569938312,
				InstructionType: "Transfer",
				InstructionData: json.RawMessage(`{"recipients":[{"address":"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE","amount":3569938312}]}`),
			}},
			Fee: 8700,
		}

		tx3 := &Transaction{
			Timestamp: 1742443166875594385,
			Hash:      "c57ade8fc059ece500e7a01b3a4766187df93ac53edd196b252f8801b75ce067",
			From:      "DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd",
			Signature: "4Ho32ymZ8JM8XQ2NBQUNFK175ooLo4bzzJvkimhbgcPNLopDqc9fL11jehZnCL1joEvcVHyN3kSH4e9vSv31iRZ7",
			Changes: []TransactionChange{{
				To:              "DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE",
				Amount:          14786680653,
				InstructionType: "Transfer",
				InstructionData: json.RawMessage(`{"recipients":[{"address":"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE","amount":14786680653}]}`),
			}},
			Fee: 10880,
		}

		// Store all transactions
		for _, tx := range []*Transaction{tx1, tx2, tx3} {
			if err := db.SetTransaction(tx); err != nil {
				t.Fatalf("failed to store %s: %v", tx.Hash, err)
			}
		}

		// Add multiple references
		if err := db.AddReference(tx1.Hash, tx2.Hash); err != nil {
			t.Errorf("AddReference tx1->tx2 failed: %v", err)
		}
		if err := db.AddReference(tx1.Hash, tx3.Hash); err != nil {
			t.Errorf("AddReference tx1->tx3 failed: %v", err)
		}
		if err := db.AddReference(tx2.Hash, tx3.Hash); err != nil {
			t.Errorf("AddReference tx2->tx3 failed: %v", err)
		}

		// Check forward references from tx1
		forwards, err := db.GetForwardRefs(tx1.Hash)
		if err != nil {
			t.Errorf("GetForwardRefs failed: %v", err)
		}
		if len(forwards) != 2 {
			t.Errorf("Expected 2 forward refs from tx1, got %d", len(forwards))
		}

		// Check backward references to tx3
		backwards, err := db.GetBackwardRefs(tx3.Hash)
		if err != nil {
			t.Errorf("GetBackwardRefs failed: %v", err)
		}
		if len(backwards) != 2 {
			t.Errorf("Expected 2 backward refs to tx3, got %d", len(backwards))
		}

		// Verify reference counts
		db.mu.RLock()
		refCount := db.refCounts[tx3.Hash].Load()
		db.mu.RUnlock()
		if refCount != 2 {
			t.Errorf("Expected refCount of 2 for tx3, got %d", refCount)
		}
	})

	t.Run("non-existent source transaction", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		// Create only the destination transaction
		tx2 := &Transaction{
			Timestamp: 1742443165854655038,
			Hash:      "e99ff77fc4dd8563fcb8862d98a36bab05418d4b876186947af7271b4e5b0e9f",
			From:      "DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd",
			Signature: "2eCFzDh6a1dsmBf4AFVEmUBxSTagSQtqQoMHiq6Ts2D4idhJM3QLTR49ZmMe9Dy8mmxmZoxnRhRCdxRrZZphTNYX",
			Changes: []TransactionChange{{
				To:              "DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE",
				Amount:          3569938312,
				InstructionType: "Transfer",
				InstructionData: json.RawMessage(`{"recipients":[{"address":"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE","amount":3569938312}]}`),
			}},
			Fee: 8700,
		}

		if err := db.SetTransaction(tx2); err != nil {
			t.Fatalf("failed to store tx2: %v", err)
		}

		// Try to add reference from non-existent transaction
		err := db.AddReference("482385ca509ac2a0b4cb0392ca354ba4d00a3a4245c57756e7f07decc43cc641", tx2.Hash)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("non-existent destination transaction", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		// Create only the source transaction
		tx1 := &Transaction{
			Timestamp: 1742443164867545494,
			Hash:      "8c3e73b8a8a8996485693057f8d141ac9cf81a01486071e1bbe025d2f0570fa3",
			From:      "DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd",
			Signature: "43ijbQTetb2e6LdzRd6Yfdo1fg3jyAhMWEu44UWw5RcMB459PJPMM68kZ6MsgVkDWmR4KHwoJse3tirzPHr5nW16",
			Changes: []TransactionChange{{
				To:              "DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE",
				Amount:          46653458980,
				InstructionType: "Transfer",
				InstructionData: json.RawMessage(`{"recipients":[{"address":"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE","amount":46653458980}]}`),
			}},
			Fee: 10920,
		}

		if err := db.SetTransaction(tx1); err != nil {
			t.Fatalf("failed to store tx1: %v", err)
		}

		// Try to add reference to non-existent transaction
		err := db.AddReference(tx1.Hash, "67e0b4967d4d557b47576e2875bca440dea438697d38036441314d2ebc0b6fce")
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("reference count initialization", func(t *testing.T) {
		db, cleanup := createTestDB(t)
		defer cleanup()

		// Create two transactions with realistic data
		tx1 := &Transaction{
			Timestamp: 1742443164867545494,
			Hash:      "8c3e73b8a8a8996485693057f8d141ac9cf81a01486071e1bbe025d2f0570fa3",
			From:      "DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd",
			Signature: "43ijbQTetb2e6LdzRd6Yfdo1fg3jyAhMWEu44UWw5RcMB459PJPMM68kZ6MsgVkDWmR4KHwoJse3tirzPHr5nW16",
			Changes: []TransactionChange{{
				To:              "DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE",
				Amount:          46653458980,
				InstructionType: "Transfer",
				InstructionData: json.RawMessage(`{"recipients":[{"address":"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE","amount":46653458980}]}`),
			}},
			Fee: 10920,
		}

		tx2 := &Transaction{
			Timestamp: 1742443165854655038,
			Hash:      "e99ff77fc4dd8563fcb8862d98a36bab05418d4b876186947af7271b4e5b0e9f",
			From:      "DEVnPuA9Ub24Taqvp9KUJ87yMEck48djHJnzC57owUpd",
			Signature: "2eCFzDh6a1dsmBf4AFVEmUBxSTagSQtqQoMHiq6Ts2D4idhJM3QLTR49ZmMe9Dy8mmxmZoxnRhRCdxRrZZphTNYX",
			Changes: []TransactionChange{{
				To:              "DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE",
				Amount:          3569938312,
				InstructionType: "Transfer",
				InstructionData: json.RawMessage(`{"recipients":[{"address":"DpFLkLKipu9G1miZJKnN33DF5Mw1sfQm8CdCkL933mwE","amount":3569938312}]}`),
			}},
			Fee: 8700,
		}

		// Store both transactions
		if err := db.SetTransaction(tx1); err != nil {
			t.Fatalf("failed to store tx1: %v", err)
		}
		if err := db.SetTransaction(tx2); err != nil {
			t.Fatalf("failed to store tx2: %v", err)
		}

		// Verify refCounts is nil before adding reference
		db.mu.RLock()
		_, exists := db.refCounts[tx2.Hash]
		db.mu.RUnlock()
		if exists {
			t.Error("Expected refCounts to be nil before adding reference")
		}

		// Add reference
		if err := db.AddReference(tx1.Hash, tx2.Hash); err != nil {
			t.Errorf("AddReference failed: %v", err)
		}

		// Verify refCount was initialized and incremented
		db.mu.RLock()
		refCount, exists := db.refCounts[tx2.Hash]
		db.mu.RUnlock()
		if !exists {
			t.Error("Expected refCounts to be initialized after adding reference")
		} else if refCount.Load() != 1 {
			t.Errorf("Expected refCount of 1, got %d", refCount.Load())
		}
	})
}
