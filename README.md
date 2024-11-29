# Bluesky Starter Pack Tools

A collection of tools for collecting, processing, and searching Bluesky starter packs. These tools help you collect, process, and search through Bluesky's starter packs and their user data.

They also build a MongoDB index that can easily be searched. For a prototype of an index frontend of BlueSky Starterpacks see the demo currently running [on vercel](https://starter-pack-explorer-o13o.vercel.app/), [github repo](https://github.com/CrispStrobe/starter-pack-explorer/).


## Prerequisites

- Node.js (v16 or higher): [Download Node.js](https://nodejs.org/)
- MongoDB (required for data storage): [Download MongoDB](https://www.mongodb.com/try/download/community)
- Git (for cloning the repository)

## Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/CrispStrobe/bluesky-starterpacks-index.git
cd bsky-starter-pack-tools
```

### Step 2: Install Dependencies

```bash
npm install
```

### Step 3: Configuration

1. Create a `.env` file:
```bash
cp .env.example .env
```

2. Edit the `.env` file with your credentials:
```env
BSKY_USERNAME=your.username.bsky.social
BSKY_PASSWORD=your_password
MONGODB_URI=mongodb://localhost:27017/starterpacks
```

## Usage

### Processing Starter Packs

```bash
# Regular processing with MongoDB storage
node data-processor.js

# Process without MongoDB storage (file output only)
node data-processor.js --nomongodb

# Update MongoDB from existing files
node data-processor.js --updatemongodb

# Purge all data and start fresh
node data-processor.js --purge
```

### Searching Starter Packs

The search tool supports both pack-centric and user-centric views, with various data sources:

```bash
# Search in MongoDB (default)
node search-packs.js "search term"

# Show results organized by users
node search-packs.js --users "search term"

# Show results organized by packs (default)
node search-packs.js --packs "search term"

# Search in JSON file
node search-packs.js -s json "search term"

# Search in YAML file
node search-packs.js -s yaml "search term"
```

#### Search Options

- `--source` or `-s`: Specify data source (mongodb, json, yaml)
- `--file` or `-f`: Input file path for JSON/YAML
- `--exact` or `-e`: Perform exact match
- `--case-sensitive` or `-c`: Make search case-sensitive
- `--users`: Show results organized by users
- `--packs`: Show results organized by packs (default)
- `--help`: Show help message

Examples:
```bash
# Exact match search
node search-packs.js --exact "javascript"

# Case-sensitive search with user-centric view
node search-packs.js --users --case-sensitive "JavaScript"

# Search in JSON with pack-centric view
node search-packs.js -s json --packs "javascript"
```

## Output Files

The tools generate several files:

- `starter_packs.json`: JSON output of processed starter packs
- `starter_packs.yaml`: YAML output of processed starter packs
- `checkpoints.json`: Progress tracking and resumption data
- `data_processor.log`: Processing logs

## Features

### Data Processing
- Collects and processes Bluesky starter packs
- Stores data in MongoDB and/or files (JSON/YAML)
- Handles incremental updates
- Tracks processing progress
- Supports resume after interruption

### Search Capabilities
- Searches by handle, display name, pack name, or description
- Supports exact and partial matching
- Case-sensitive and case-insensitive options
- Two view formats: pack-centric and user-centric
- Multiple data source options (MongoDB/JSON/YAML)

### Rate Limiting and Error Handling
- Implements Bluesky's rate limit of 3000 requests per 5 minutes
- Uses exponential backoff for rate limit handling
- Automatically retries failed requests
- Handles network errors gracefully

## MongoDB Schema

### Users Collection
```javascript
{
  did: String,            // User's DID
  handle: String,         // User's handle
  display_name: String,   // User's display name
  pack_ids: [String],     // Array of pack IDs user belongs to
  last_updated: Date,     // Last profile update time
  profile_check_needed: Boolean  // Profile update flag
}
```

### Starter Packs Collection
```javascript
{
  rkey: String,          // Pack's unique key
  name: String,          // Pack name
  creator: String,       // Pack creator's handle
  description: String,   // Pack description
  user_count: Number,    // Number of users in pack
  created_at: Date,      // Creation timestamp
  updated_at: Date       // Last update timestamp
}
```

## Troubleshooting

### Common Issues
- MongoDB connection errors: Verify MongoDB is running and connection string is correct
- Rate limit errors: The script will automatically handle these with backoff
- Network timeouts: Check your internet connection
- Interrupted processing: Will automatically resume from last checkpoint

### Logs
- Check `data_processor.log` for detailed processing information
- Use `--nomongodb` flag to test file processing without database
- MongoDB errors will be logged with full stack traces

## Development

### File Structure
```
bsky-starter-pack-tools/
├── data-processor.js    # Main processing script
├── search-packs.js      # Search interface
├── .env                 # Configuration (not in git)
├── .env.example         # Example configuration
└── package.json         # Project dependencies
```

### Contributing
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
