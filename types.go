package hyyve

import (
	"encoding/json"
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
	// HashSize is the size in characters for hex encoded SHA3-256 hashes
	// Example: "bc1291a58dff1d0e8dafb32e9b6a4487aa2ee038eb31d857b1a21e07bfa9f51e"
	HashSize = 64
	// AddressSize is the max characters for base58 encoded Ed25519 public keys
	// Example: "B5LKKpvY6XXpYsDEZSUrzGe58QHWxHReak9tQwzM2R8W"
	AddressSize = 44
	// SignatureSize is the size in characters for base58 encoded Ed25519 signatures
	// Example: "44uivokdp27bP7E8wHECetk2FRU1tq82F1w7ZrgZmeszQmvo72UW55vpsewYPEPjh8m8BidEzXRttmdxhKGJ9jXW"
	SignatureSize = 88

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

const (
	InstructionRequiredTimestamp = int64(1736226247)

	InstructionTransfer            = "Transfer"
	InstructionCreateToken         = "CreateToken"
	InstructionCreateNFT           = "CreateNFT"
	InstructionMintSupply          = "MintSupply"
	InstructionBurnSupply          = "BurnSupply"
	InstructionTransferNFT         = "TransferNFT"
	InstructionFreezeNFT           = "FreezeNFT"
	InstructionThawNFT             = "ThawNFT"
	InstructionFreezeAccount       = "FreezeAccount"
	InstructionThawAccount         = "ThawAccount"
	InstructionFreezeAmount        = "FreezeAmount"
	InstructionThawAmount          = "ThawAmount"
	InstructionSwapToken           = "SwapToken"
	InstructionAddLiquidity        = "AddLiquidity"
	InstructionRemoveLiquidity     = "RemoveLiquidity"
	InstructionCreateLiquidityPool = "CreateLiquidityPool"
	InstructionValidatePool        = "ValidatePool"
	InstructionRewardClaim         = "RewardClaim"
	InstructionGenesis             = "Genesis"
	InstructionTokenGrant          = "TokenGrant"
	InstructionRewardPayment       = "RewardPayment"
)

type CreateTokenInstruction struct {
	Name            string `json:"name"`
	Ticker          string `json:"ticker"`
	Avatar          string `json:"avatar"`
	Decimals        uint8  `json:"decimals"`
	TotalSupply     uint64 `json:"total_supply"`
	Mintable        bool   `json:"mintable"`
	MintAuthority   string `json:"mint_authority"`
	Freezable       bool   `json:"freezable"`
	FreezeAuthority string `json:"freeze_authority"`
	URI             string `json:"uri,omitempty"`
}

type MintSupplyInstruction struct {
	TokenAddress       string `json:"token_address"`
	TokenSupplyCap     uint64 `json:"token_supply_cap"`
	Recipient          string `json:"recipient"`
	ValidatorSignature []byte `json:"validator_signature"`
	ValidatorAddress   string `json:"validator_address"`
}

type CreateNFTInstruction struct {
	Name            string     `json:"name"`
	Ticker          string     `json:"ticker"`
	Mintable        bool       `json:"mintable"`
	MintAuthority   string     `json:"mint_authority"`
	Freezable       bool       `json:"freezable"`
	FreezeAuthority string     `json:"freeze_authority"`
	RoyaltyBPS      uint16     `json:"royalty_bps"`
	BaseURI         string     `json:"base_uri"`         // Base URI for the collection
	MaxSupply       uint64     `json:"max_supply"`       // Max number of NFTs that can exist (0 for unlimited)
	Tokens          []NFTToken `json:"tokens,omitempty"` // Initial tokens to mint
}

type NFTToken struct {
	TokenID    string          `json:"token_id"`
	URI        string          `json:"uri,omitempty"`        // Optional override of BaseURI
	Attributes json.RawMessage `json:"attributes,omitempty"` // Custom attributes for this token
	Recipient  string          `json:"recipient"`            // Address to receive this token
}

// For minting additional tokens later
type MintNFTInstruction struct {
	CollectionAddress string     `json:"collection_address"`
	Tokens            []NFTToken `json:"tokens"`
	MintAuthority     string     `json:"mint_authority"`           // Must match collection's mint authority
	BatchID           string     `json:"batch_id,omitempty"`       // Optional identifier for this mint batch
	Price             uint64     `json:"price,omitempty"`          // Price per token if this is a public mint
	StartTime         int64      `json:"start_time,omitempty"`     // Unix timestamp when minting can begin
	EndTime           int64      `json:"end_time,omitempty"`       // Unix timestamp when minting must end
	AllowList         []string   `json:"allow_list,omitempty"`     // Addresses allowed to mint (empty = public)
	MaxPerWallet      uint64     `json:"max_per_wallet,omitempty"` // Maximum tokens per wallet (0 = unlimited)
}

type BurnSupplyInstruction struct {
	TokenAddress string `json:"token_address"`
	Amount       uint64 `json:"amount"`
}

type TransferNFTInstruction struct {
	NFTAddress  string `json:"nft_address"`
	TokenID     string `json:"token_id"`
	RoyaltyAddr string `json:"royalty_addr"`
}

type FreezeInstruction struct {
	TokenAddress string `json:"token_address"`
	Amount       uint64 `json:"amount,omitempty"`
}

type SwapTokenInstruction struct {
	FromToken    string `json:"from_token"` // Empty string means HYY
	ToToken      string `json:"to_token"`   // Empty string means HYY
	AmountIn     uint64 `json:"amount_in"`
	MinAmountOut uint64 `json:"min_amount_out"`
	// Optional: let users specify exact pools if they want
	SpecificPools []string `json:"specific_pools,omitempty"`
}

type LiquidityInstruction struct {
	PoolAddress  string `json:"pool_address"`
	TokenA       string `json:"token_a"`
	TokenB       string `json:"token_b"`
	AmountA      uint64 `json:"amount_a"`
	AmountB      uint64 `json:"amount_b"`
	MinLiquidity uint64 `json:"min_liquidity,omitempty"`
}

type CreatePoolInstruction struct {
	TokenA   string `json:"token_a"`
	TokenB   string `json:"token_b"`
	InitialA uint64 `json:"initial_a"`
	InitialB uint64 `json:"initial_b"`
	FeeBPS   uint16 `json:"fee_bps"` // Basis points
}

type ValidatePoolInstruction struct {
	PoolAddress        string `json:"pool_address"`
	ValidatorAddress   string `json:"validator_address"`
	ValidatorSignature []byte `json:"validator_signature"`
}

type ThawInstruction struct {
	TokenAddress string `json:"token_address"`
	Amount       uint64 `json:"amount,omitempty"`
}

type FreezeAccountInstruction struct {
	Account string `json:"account"`
}

type ThawAccountInstruction struct {
	Account string `json:"account"`
}

type FreezeNFTInstruction struct {
	NFTAddress string `json:"nft_address"`
	TokenID    string `json:"token_id"`
}

type ThawNFTInstruction struct {
	NFTAddress string `json:"nft_address"`
	TokenID    string `json:"token_id"`
}

type TransferRecipient struct {
	Address string `json:"address"`
	Amount  uint64 `json:"amount"`
}

type TransferInstruction struct {
	TokenAddress string              `json:"token_address,omitempty"` // Optional - if empty, transfers native token
	Recipients   []TransferRecipient `json:"recipients"`              // List of recipients and amounts
	Memo         string              `json:"memo,omitempty"`
}

type FreezeAmountInstruction struct {
	TokenAddress string `json:"token_address"`
	Amount       uint64 `json:"amount"`
	UnfreezeTime int64  `json:"unfreeze_time"` // Unix timestamp when amount can be unfrozen
}

type ThawAmountInstruction struct {
	TokenAddress string `json:"token_address"`
	Amount       uint64 `json:"amount"`
	FreezeID     string `json:"freeze_id"` // ID of the original freeze instruction
}

// RewardPaymentInstruction represents a mining reward payment from a validator
type RewardPaymentInstruction struct {
	Amount             uint64 `json:"amount"`              // Amount of new tokens to mint
	Recipient          string `json:"recipient"`           // Miner's address
	ValidatorAddress   string `json:"validator_address"`   // Validator who verified
	ValidatorSignature []byte `json:"validator_signature"` // Validator's signature
}

// For delegating authority to another address
type DelegateInstruction struct {
	TokenAddress string   `json:"token_address,omitempty"` // Optional - if empty, delegates native token
	DelegateeTo  string   `json:"delegatee"`               // Address receiving delegation
	Amount       uint64   `json:"amount"`                  // Amount being delegated
	Permissions  []string `json:"permissions"`             // What actions the delegate can perform
	ExpiryTime   int64    `json:"expiry_time,omitempty"`   // When delegation expires (0 = no expiry)
}

// For revoking previously granted delegation
type RevokeDelegationInstruction struct {
	TokenAddress string `json:"token_address,omitempty"`
	DelegateeTo  string `json:"delegatee"`
	DelegationID string `json:"delegation_id"` // ID of the original delegation
}

// For updating token/NFT metadata
type UpdateMetadataInstruction struct {
	TokenAddress string          `json:"token_address"`
	URI          string          `json:"uri,omitempty"`
	UpdateMask   []string        `json:"update_mask"`  // Fields that can be updated
	NewMetadata  json.RawMessage `json:"new_metadata"` // New metadata to apply
}

// For closing/deleting a token/NFT
type CloseAccountInstruction struct {
	TokenAddress string `json:"token_address"`
	RefundTo     string `json:"refund_to"` // Address to receive any remaining tokens/rent
}

// For setting new authorities on a token/NFT
type SetAuthorityInstruction struct {
	TokenAddress  string   `json:"token_address"`
	AuthorityType string   `json:"authority_type"` // mint, freeze, etc
	NewAuthority  string   `json:"new_authority"`
	UpdateMask    []string `json:"update_mask"` // Which authorities to update
}

// For creating a vesting schedule
type CreateVestingInstruction struct {
	TokenAddress     string `json:"token_address,omitempty"`
	Recipient        string `json:"recipient"`
	TotalAmount      uint64 `json:"total_amount"`
	StartTime        int64  `json:"start_time"`
	EndTime          int64  `json:"end_time"`
	ReleaseFrequency uint64 `json:"release_frequency"` // In seconds
	Revocable        bool   `json:"revocable"`
}

// For claiming vested tokens
type ClaimVestedInstruction struct {
	VestingID   string `json:"vesting_id"`
	ClaimAmount uint64 `json:"claim_amount"`
}

// For revoking a vesting schedule
type RevokeVestingInstruction struct {
	VestingID string `json:"vesting_id"`
	RefundTo  string `json:"refund_to"`
}

type RewardClaimInstruction struct {
	WinningAddress   string `json:"winning_address"`
	WinningSignature []byte `json:"winning_signature"`
	MinerAddress     string `json:"miner_address"`
	MinerSignature   []byte `json:"miner_signature"`
}

type TokenGrantInstruction struct {
	GrantType string `json:"grant_type"`
	Amount    uint64 `json:"amount"`    // Amount being granted
	Recipient string `json:"recipient"` // Address receiving the grant
}

type GenesisInstruction struct {
	Amount    uint64 `json:"amount"`    // Amount being granted
	Recipient string `json:"recipient"` // Address receiving the grant
}
