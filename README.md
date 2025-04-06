# Flow Processor: Combined Accounts

A Flow processor plugin for processing Stellar account data.

## Features

- Processes account changes from Stellar ledgers
- Extracts account information including:
  - Account ID
  - Signers array with weights and sponsors
  - Thresholds (low, medium, high, and master weight)
- Provides GraphQL schema for querying account data
- Configurable account filtering

## Configuration

The processor can be configured to process specific accounts or all accounts:

```json
{
  "process_all": false,  // Set to true to process all accounts
  "account_ids": [       // List of specific account IDs to process
    "GABC123...",
    "GDEF456..."
  ]
}
```

### Configuration Options

| Parameter | Required | Type | Default | Description |
|-----------|----------|------|---------|-------------|
| process_all | No | boolean | false | If true, process all accounts. If false, only process accounts listed in account_ids |
| account_ids | No | string[] | [] | List of specific account IDs to process. Required if process_all is false |

Note: At least one of `process_all` or `account_ids` must be specified. If neither is provided, the processor will log a warning but continue running.

## GraphQL Schema

### Types

```graphql
type Account {
    id: String!
    account_id: String!
    master_weight: Int!
    low_threshold: Int!
    medium_threshold: Int!
    high_threshold: Int!
    signers: [AccountSigner!]!
    ledger_sequence: Int!
    created_at: String!
    updated_at: String!
}

type AccountSigner {
    account_id: String!
    signer: String!
    weight: Int!
    sponsor: String
    ledger_sequence: Int!
    created_at: String!
    updated_at: String!
}
```

### Queries

```graphql
query GetAccount($id: String!) {
    account(id: $id) {
        id
        account_id
        master_weight
        low_threshold
        medium_threshold
        high_threshold
        signers {
            signer
            weight
            sponsor
        }
        ledger_sequence
        created_at
        updated_at
    }
}
```

## Building

### Using Go

```bash
go build -buildmode=plugin -o flow-processor-combined-accounts.so .
```

### Using Nix

1. Install Nix if you haven't already:
   ```bash
   curl -L https://nixos.org/nix/install | sh
   ```

2. Enable flakes (if not already enabled):
   ```bash
   mkdir -p ~/.config/nix
   echo "experimental-features = nix-command flakes" >> ~/.config/nix/nix.conf
   ```

3. Build the plugin:
   ```bash
   nix build
   ```
   The plugin will be available at `./result/lib/flow-processor-combined-accounts.so`

4. For development, enter the Nix shell:
   ```bash
   nix develop
   ```
   This will provide a development environment with all necessary tools.

## License

[License information] 