package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/pluginapi"
)

// AccountRecord represents the combined account and signer information
type AccountRecord struct {
	// Account information
	AccountID          string    `json:"account_id"`
	Balance            string    `json:"balance"`             // In stroops
	BuyingLiabilities  string    `json:"buying_liabilities"`  // In stroops
	SellingLiabilities string    `json:"selling_liabilities"` // In stroops
	Sequence           uint64    `json:"sequence"`            // Sequence number
	SequenceLedger     uint32    `json:"sequence_ledger"`     // Ledger where sequence was last modified
	SequenceTime       time.Time `json:"sequence_time"`       // Time when sequence was last modified
	NumSubentries      uint32    `json:"num_subentries"`
	InflationDest      string    `json:"inflation_dest,omitempty"`
	Flags              uint32    `json:"flags"` // Account flags
	HomeDomain         string    `json:"home_domain,omitempty"`
	MasterWeight       uint32    `json:"master_weight"`
	LowThreshold       uint32    `json:"low_threshold"`
	MediumThreshold    uint32    `json:"medium_threshold"`
	HighThreshold      uint32    `json:"high_threshold"`
	Sponsor            string    `json:"sponsor,omitempty"` // Account sponsor
	NumSponsored       uint32    `json:"num_sponsored"`     // Number of entries this account sponsors
	NumSponsoring      uint32    `json:"num_sponsoring"`    // Number of entries sponsoring this account
	LastModifiedLedger uint32    `json:"last_modified_ledger"`
	LedgerEntryChange  uint32    `json:"ledger_entry_change"` // Type of ledger entry change
	Deleted            bool      `json:"deleted"`
	ClosedAt           time.Time `json:"closed_at"`       // Time when the ledger closed
	LedgerSequence     uint32    `json:"ledger_sequence"` // Ledger sequence number
	Timestamp          time.Time `json:"timestamp"`       // Processing timestamp

	// Signer information
	Signers []AccountSigner `json:"signers"`
}

// AccountSigner represents a signer for an account
type AccountSigner struct {
	Signer             string    `json:"signer"`
	Weight             int32     `json:"weight"`
	Sponsor            string    `json:"sponsor,omitempty"`
	LastModifiedLedger uint32    `json:"last_modified_ledger"`
	LedgerEntryChange  uint32    `json:"ledger_entry_change"`
	Deleted            bool      `json:"deleted"`
	ClosedAt           time.Time `json:"closed_at"`
	LedgerSequence     uint32    `json:"ledger_sequence"`
}

// AccountsProcessor implements pluginapi.Processor
type AccountsProcessor struct {
	consumers []pluginapi.Consumer
}

// GetSchemaDefinition returns GraphQL type definitions for this plugin
func (ap *AccountsProcessor) GetSchemaDefinition() string {
	return `
type Account {
    accountId: String!
    balance: String!
    buyingLiabilities: String!
    sellingLiabilities: String!
    sequence: String!
    sequenceLedger: Int!
    sequenceTime: String!
    numSubentries: Int!
    inflationDest: String
    flags: Int!
    homeDomain: String
    masterWeight: Int!
    lowThreshold: Int!
    mediumThreshold: Int!
    highThreshold: Int!
    sponsor: String
    numSponsored: Int!
    numSponsoring: Int!
    lastModifiedLedger: Int!
    ledgerEntryChange: Int!
    deleted: Boolean!
    closedAt: String!
    ledgerSequence: Int!
    timestamp: String!
    signers: [AccountSigner!]!
}

type AccountSigner {
    signer: String!
    weight: Int!
    sponsor: String
    lastModifiedLedger: Int!
    ledgerEntryChange: Int!
    deleted: Boolean!
    closedAt: String!
    ledgerSequence: Int!
}
`
}

// GetQueryDefinitions returns GraphQL query definitions for this plugin
func (ap *AccountsProcessor) GetQueryDefinitions() string {
	return `
    account(accountId: String!): Account
    accounts(first: Int, after: String): [Account!]!
    accountsByLedger(ledgerSequence: Int!, first: Int, after: String): [Account!]!
`
}

// Name returns the plugin name
func (ap *AccountsProcessor) Name() string {
	return "flow/processor/accounts"
}

// Version returns the plugin version
func (ap *AccountsProcessor) Version() string {
	return "1.0.0"
}

// Type returns the plugin type
func (ap *AccountsProcessor) Type() pluginapi.PluginType {
	return pluginapi.ProcessorPlugin
}

// Initialize initializes the processor with configuration
func (ap *AccountsProcessor) Initialize(config map[string]interface{}) error {
	// (Optional) Process any configuration here
	log.Printf("AccountsProcessor initialized with config: %+v", config)
	return nil
}

// RegisterConsumer registers a downstream consumer
func (ap *AccountsProcessor) RegisterConsumer(consumer pluginapi.Consumer) {
	ap.consumers = append(ap.consumers, consumer)
}

// Process processes a message containing a LedgerCloseMeta and extracts account changes
func (ap *AccountsProcessor) Process(ctx context.Context, msg pluginapi.Message) error {
	ledgerMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	log.Printf("AccountsProcessor: Processing ledger %d", ledgerMeta.LedgerSequence())

	// Extract account changes from ledgerMeta
	accountChanges := getAccountChanges(ledgerMeta)
	log.Printf("AccountsProcessor: Found %d account changes in ledger %d", len(accountChanges), ledgerMeta.LedgerSequence())

	// Process each account change
	for i, change := range accountChanges {
		log.Printf("AccountsProcessor: Processing account change %d of %d (type: %s)",
			i+1, len(accountChanges), change.Type)

		record, err := processAccountChange(change, ledgerMeta)
		if err != nil {
			log.Printf("Error processing account change: %v", err)
			continue
		}

		log.Printf("AccountsProcessor: Successfully processed account %s (deleted: %t)",
			record.AccountID, record.Deleted)

		// Marshal the record to JSON
		data, err := json.Marshal(record)
		if err != nil {
			log.Printf("Error marshaling account record: %v", err)
			continue
		}

		// Create a new message with the JSON payload
		outMsg := pluginapi.Message{
			Payload:   data,
			Timestamp: time.Now(),
			Metadata: map[string]interface{}{
				"data_type":  "account",
				"account_id": record.AccountID,
				"ledger":     record.LastModifiedLedger,
				"deleted":    record.Deleted,
			},
		}

		// Forward the message to downstream consumers
		log.Printf("AccountsProcessor: Forwarding account %s to %d consumers",
			record.AccountID, len(ap.consumers))

		for _, consumer := range ap.consumers {
			if err := consumer.Process(ctx, outMsg); err != nil {
				log.Printf("Error forwarding account record to consumer %s: %v", consumer.Name(), err)
			}
		}
	}

	return nil
}

// processAccountChange converts a single LedgerEntryChange into an AccountRecord
func processAccountChange(change xdr.LedgerEntryChange, meta xdr.LedgerCloseMeta) (*AccountRecord, error) {
	var entry xdr.LedgerEntry
	var ledgerEntryChangeType uint32

	// For removals, only Pre exists. For state/created/updated, use Post
	deleted := false
	switch change.Type {
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		if change.Removed == nil {
			return nil, fmt.Errorf("removed change has nil Removed")
		}
		// For removed entries, we need to get the entry from the ledger key
		// This is a significant change in how removals are handled
		return nil, fmt.Errorf("removal handling not implemented")
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		if change.Created == nil {
			return nil, fmt.Errorf("created change has nil Created")
		}
		entry = *change.Created
		ledgerEntryChangeType = 1 // Created
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		if change.Updated == nil {
			return nil, fmt.Errorf("updated change has nil Updated")
		}
		entry = *change.Updated
		ledgerEntryChangeType = 2 // Updated
	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		if change.State == nil {
			return nil, fmt.Errorf("state change has nil State")
		}
		entry = *change.State
		ledgerEntryChangeType = 3 // State
	default:
		return nil, fmt.Errorf("unknown change type %s", change.Type)
	}

	// Ensure this is an Account entry
	if entry.Data.Type != xdr.LedgerEntryTypeAccount {
		return nil, fmt.Errorf("non-account entry encountered")
	}

	accountEntry := entry.Data.MustAccount()

	// Convert account ID using strkey
	var accountIdBytes []byte
	if accountEntry.AccountId.Ed25519 != nil {
		accountIdBytes = accountEntry.AccountId.Ed25519[:]
	} else {
		return nil, fmt.Errorf("account ID is nil")
	}

	accountID, err := strkey.Encode(strkey.VersionByteAccountID, accountIdBytes)
	if err != nil {
		return nil, fmt.Errorf("error encoding account ID: %w", err)
	}

	// Get sponsor if available
	var sponsor string
	if entry.Ext.V == 1 && entry.Ext.V1 != nil && entry.Ext.V1.SponsoringId != nil {
		sponsorBytes := entry.Ext.V1.SponsoringId.Ed25519[:]
		sponsor, err = strkey.Encode(strkey.VersionByteAccountID, sponsorBytes)
		if err != nil {
			log.Printf("Warning: Could not encode sponsor ID: %v", err)
		}
	}

	// Get ledger close time
	var closedAt time.Time
	if meta.LedgerSequence() > 0 {
		closedAt = time.Unix(int64(meta.LedgerCloseTime()), 0).UTC()
	} else {
		closedAt = time.Now().UTC()
	}

	// Process signers
	var signers []AccountSigner
	sponsors := accountEntry.SponsorPerSigner()
	for signer, weight := range accountEntry.SignerSummary() {
		var signerSponsor string
		if sponsorDesc, isSponsored := sponsors[signer]; isSponsored {
			signerSponsor = sponsorDesc.Address()
		}

		signers = append(signers, AccountSigner{
			Signer:             signer,
			Weight:             weight,
			Sponsor:            signerSponsor,
			LastModifiedLedger: uint32(entry.LastModifiedLedgerSeq),
			LedgerEntryChange:  ledgerEntryChangeType,
			Deleted:            deleted,
			ClosedAt:           closedAt,
			LedgerSequence:     uint32(meta.LedgerSequence()),
		})
	}

	// Sort signers by weight
	sort.Slice(signers, func(i, j int) bool {
		return signers[i].Weight < signers[j].Weight
	})

	// Build the AccountRecord
	record := &AccountRecord{
		AccountID:          accountID,
		Balance:            fmt.Sprintf("%d", accountEntry.Balance),
		BuyingLiabilities:  fmt.Sprintf("%d", accountEntry.Liabilities().Buying),
		SellingLiabilities: fmt.Sprintf("%d", accountEntry.Liabilities().Selling),
		Sequence:           uint64(accountEntry.SeqNum),
		SequenceLedger:     uint32(entry.LastModifiedLedgerSeq), // Using last modified as a proxy
		SequenceTime:       closedAt,                            // Using ledger close time as a proxy
		NumSubentries:      uint32(accountEntry.NumSubEntries),
		Flags:              uint32(accountEntry.Flags),
		MasterWeight:       uint32(accountEntry.Thresholds[0]),
		LowThreshold:       uint32(accountEntry.Thresholds[1]),
		MediumThreshold:    uint32(accountEntry.Thresholds[2]),
		HighThreshold:      uint32(accountEntry.Thresholds[3]),
		Sponsor:            sponsor,
		NumSponsored:       uint32(accountEntry.NumSponsored()),
		NumSponsoring:      uint32(accountEntry.NumSponsoring()),
		LastModifiedLedger: uint32(entry.LastModifiedLedgerSeq),
		LedgerEntryChange:  ledgerEntryChangeType,
		Deleted:            deleted,
		ClosedAt:           closedAt,
		LedgerSequence:     uint32(meta.LedgerSequence()),
		Timestamp:          time.Now(),
		Signers:            signers,
	}

	// Set InflationDest if present
	if accountEntry.InflationDest != nil {
		var inflationDestBytes []byte
		if accountEntry.InflationDest.Ed25519 != nil {
			inflationDestBytes = accountEntry.InflationDest.Ed25519[:]
			inflationDest, err := strkey.Encode(strkey.VersionByteAccountID, inflationDestBytes)
			if err != nil {
				log.Printf("Warning: Could not encode inflation destination: %v", err)
			} else {
				record.InflationDest = inflationDest
			}
		}
	}

	// Set HomeDomain if present
	homeDomain := string(accountEntry.HomeDomain)
	if homeDomain != "" {
		record.HomeDomain = homeDomain
	}

	return record, nil
}

// getAccountChanges extracts all account-related changes from the LedgerCloseMeta
func getAccountChanges(meta xdr.LedgerCloseMeta) []xdr.LedgerEntryChange {
	var accountChanges []xdr.LedgerEntryChange
	var changes []xdr.LedgerEntryChange

	log.Printf("getAccountChanges: Processing ledger %d (meta version: %d)", meta.LedgerSequence(), meta.V)

	switch meta.V {
	case 0:
		if meta.V0 != nil {
			log.Printf("getAccountChanges: Processing V0 meta")

			// Check if TxSet exists and is not empty
			if meta.V0.TxSet.Txs != nil {
				log.Printf("getAccountChanges: Found %d transactions in TxSet", len(meta.V0.TxSet.Txs))

				// Extract changes from transaction set
				for i := range meta.V0.TxSet.Txs {
					// Use the Operations() method to get operations
					ops := meta.V0.TxSet.Txs[i].Operations()
					log.Printf("getAccountChanges: Transaction %d has %d operations", i+1, len(ops))
				}
			} else {
				log.Printf("getAccountChanges: No transactions in TxSet")
			}

			// Extract changes directly from V0
			if meta.V0.TxProcessing != nil {
				log.Printf("getAccountChanges: Found %d TxProcessing entries", len(meta.V0.TxProcessing))

				for i, txProcessing := range meta.V0.TxProcessing {
					if txProcessing.FeeProcessing != nil {
						log.Printf("getAccountChanges: TxProcessing[%d] has %d FeeProcessing changes",
							i, len(txProcessing.FeeProcessing))
						changes = append(changes, txProcessing.FeeProcessing...)
					}

					log.Printf("getAccountChanges: TxProcessing[%d] TxApplyProcessing version: %d",
						i, txProcessing.TxApplyProcessing.V)

					// Handle TxApplyProcessing based on its version
					switch txProcessing.TxApplyProcessing.V {
					case 1:
						v1Meta := txProcessing.TxApplyProcessing.MustV1()
						if v1Meta.Operations != nil {
							log.Printf("getAccountChanges: V1 meta has %d operations", len(v1Meta.Operations))
							for j, opMeta := range v1Meta.Operations {
								if opMeta.Changes != nil {
									log.Printf("getAccountChanges: V1 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 2:
						v2Meta := txProcessing.TxApplyProcessing.MustV2()
						if v2Meta.Operations != nil {
							log.Printf("getAccountChanges: V2 meta has %d operations", len(v2Meta.Operations))
							for j, opMeta := range v2Meta.Operations {
								if opMeta.Changes != nil {
									log.Printf("getAccountChanges: V2 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 3:
						v3Meta := txProcessing.TxApplyProcessing.MustV3()
						if v3Meta.Operations != nil {
							log.Printf("getAccountChanges: V3 meta has %d operations", len(v3Meta.Operations))
							for j, opMeta := range v3Meta.Operations {
								if opMeta.Changes != nil {
									log.Printf("getAccountChanges: V3 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					default:
						log.Printf("getAccountChanges: Unknown TxApplyProcessing version: %d",
							txProcessing.TxApplyProcessing.V)
					}
				}
			} else {
				log.Printf("getAccountChanges: No TxProcessing entries found")
			}
		}
	case 1:
		if meta.V1 != nil {
			log.Printf("getAccountChanges: Processing V1 meta")

			// Process TxProcessing entries
			if meta.V1.TxProcessing != nil {
				log.Printf("getAccountChanges: Found %d TxProcessing entries in V1 meta", len(meta.V1.TxProcessing))

				for i, txProcessing := range meta.V1.TxProcessing {
					// Process fee changes
					if txProcessing.FeeProcessing != nil {
						log.Printf("getAccountChanges: V1 TxProcessing[%d] has %d FeeProcessing changes",
							i, len(txProcessing.FeeProcessing))
						changes = append(changes, txProcessing.FeeProcessing...)
					}

					// Process transaction metadata based on version
					log.Printf("getAccountChanges: V1 TxProcessing[%d] TxApplyProcessing version: %d",
						i, txProcessing.TxApplyProcessing.V)

					switch txProcessing.TxApplyProcessing.V {
					case 1:
						v1Meta := txProcessing.TxApplyProcessing.MustV1()
						if v1Meta.Operations != nil {
							log.Printf("getAccountChanges: V1 meta has %d operations", len(v1Meta.Operations))
							for j, opMeta := range v1Meta.Operations {
								if opMeta.Changes != nil {
									log.Printf("getAccountChanges: V1 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 2:
						v2Meta := txProcessing.TxApplyProcessing.MustV2()
						if v2Meta.Operations != nil {
							log.Printf("getAccountChanges: V2 meta has %d operations", len(v2Meta.Operations))
							for j, opMeta := range v2Meta.Operations {
								if opMeta.Changes != nil {
									log.Printf("getAccountChanges: V2 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					case 3:
						v3Meta := txProcessing.TxApplyProcessing.MustV3()
						if v3Meta.Operations != nil {
							log.Printf("getAccountChanges: V3 meta has %d operations", len(v3Meta.Operations))
							for j, opMeta := range v3Meta.Operations {
								if opMeta.Changes != nil {
									log.Printf("getAccountChanges: V3 operation %d has %d changes",
										j, len(opMeta.Changes))
									changes = append(changes, opMeta.Changes...)
								}
							}
						}
					default:
						log.Printf("getAccountChanges: Unknown TxApplyProcessing version: %d",
							txProcessing.TxApplyProcessing.V)
					}
				}
			} else {
				log.Printf("getAccountChanges: No TxProcessing entries found in V1 meta")
			}
		}
	default:
		log.Printf("getAccountChanges: Unknown meta version: %d", meta.V)
	}

	log.Printf("getAccountChanges: Found %d total changes", len(changes))

	// Filter for account changes
	for i, change := range changes {
		log.Printf("getAccountChanges: Examining change %d of type %s", i+1, change.Type)

		// Check if this is an account entry
		var entry *xdr.LedgerEntry

		switch change.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
			entry = change.Created
			log.Printf("getAccountChanges: Found Created entry")
		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			entry = change.Updated
			log.Printf("getAccountChanges: Found Updated entry")
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			entry = change.State
			log.Printf("getAccountChanges: Found State entry")
		case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
			log.Printf("getAccountChanges: Found Removed entry (skipping type check)")
			// For removals, we can't check the type directly
			continue
		default:
			log.Printf("getAccountChanges: Unknown change type: %s", change.Type)
			continue
		}

		if entry != nil {
			log.Printf("getAccountChanges: Entry type: %s", entry.Data.Type)
			if entry.Data.Type == xdr.LedgerEntryTypeAccount {
				log.Printf("getAccountChanges: Found account entry!")
				accountChanges = append(accountChanges, change)
			}
		} else {
			log.Printf("getAccountChanges: Entry is nil")
		}
	}

	log.Printf("getAccountChanges: Returning %d account changes", len(accountChanges))
	return accountChanges
}

// Exported New function allows dynamic loading
func New() pluginapi.Plugin {
	return &AccountsProcessor{}
}
