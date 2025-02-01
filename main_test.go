package hyyve

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
		FilePath: filepath.Join(dir, "test.db"),
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
			To:     "1111111111111111111111111111111111111111",
			Amount: 1000,
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
