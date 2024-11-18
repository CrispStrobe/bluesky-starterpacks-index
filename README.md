# Bluesky Starter Pack Tools

A collection of tools for collecting, processing, and searching Bluesky starter packs.

## Prerequisites

- Node.js (v16 or higher): [Download Node.js](https://nodejs.org/)
- MongoDB (optional, for database storage): [Download MongoDB](https://www.mongodb.com/try/download/community)
- Git (for cloning the repository)

## Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/bsky-starter-pack-tools.git
cd bsky-starter-pack-tools
```

### Step 2: Install Dependencies

#### Windows (Command Prompt):
```cmd
npm install
```

#### macOS/Linux (Terminal):
```bash
npm install
```

Note: If you encounter any permission issues on Linux/macOS, you might need to use:
```bash
sudo npm install
```

### Step 3: Configuration

1. Create a `.env` file by copying the example:

#### Windows:
```cmd
copy .env.example .env
```

#### macOS/Linux:
```bash
cp .env.example .env
```

2. Edit the `.env` file with your credentials:
```env
BSKY_USERNAME=your.username.bsky.social
BSKY_PASSWORD=your_password
MONGODB_URI=mongodb://username:password@host:port/database
```

## Usage

The tools can be run using npm scripts or directly with node. Both methods work on all platforms.

### Using npm scripts:

#### 1. Collect starter pack URLs:
```bash
npm run collect
```

#### 2. Process starter packs:
```bash
npm run process
```

#### 3. Search starter packs:
```bash
# Search in MongoDB
npm run search -- --source mongodb "javascript"

# Search in JSON file
npm run search -- --source json --file starter_packs.json "javascript"

# Search in YAML file
npm run search -- --source yaml --file starter_packs.yaml "javascript"
```

### Using node directly:

#### Windows:
```cmd
node url-collector-fast.js
node data-processor.js
node search-packs.js --source mongodb "javascript"
```

#### macOS/Linux:
```bash
node url-collector-fast.js
node data-processor.js
node search-packs.js --source mongodb "javascript"
```

### Search Options

- `--source` or `-s`: Specify data source (mongodb, json, yaml)
- `--file` or `-f`: Input file path for JSON/YAML
- `--exact` or `-e`: Perform exact match
- `--case-sensitive` or `-c`: Make search case-sensitive
- `--help`: Show help message

Examples:
```bash
# Exact match search
npm run search -- --source mongodb --exact "javascript"

# Case-sensitive search in JSON file
npm run search -- --source json --file starter_packs.json --case-sensitive "JavaScript"
```

## Output Files

The tools generate several files that are git-ignored by default:

- `starter_packs.json`: JSON output of processed starter packs
- `starter_packs.yaml`: YAML output of processed starter packs
- `checkpoints.json`: Progress tracking and resumption data
- `data_processor.log`: Processing logs
- `failed_pages.log`: Failed URL collection attempts

## Rate Limiting and Resumption

- Implements Bluesky's rate limit of 3000 requests per 5 minutes
- Automatically saves progress in checkpoints.json
- Resumes from last successful position if interrupted
- Uses exponential backoff for rate limit handling

## Troubleshooting

### Windows-Specific Issues:
- If you get EACCES errors, run Command Prompt as Administrator
- For SSL/certificate errors, ensure your Node.js installation is up to date

### Linux/macOS-Specific Issues:
- Permission denied errors: Use `sudo` for npm install if needed
- If MongoDB service doesn't start: `sudo systemctl start mongodb`

### General Issues:
- Rate limit errors: The script will automatically handle these
- Network timeouts: Check your internet connection
- MongoDB connection errors: Verify your connection string in .env

## Development

### File Structure:
```
bsky-starter-pack-tools/
├── url-collector-fast.js   # Collects starter pack URLs
├── data-processor.js       # Processes and stores starter packs
├── search-packs.js         # Search interface
├── .env                    # Configuration (not in git)
├── .env.example           # Example configuration
└── package.json           # Project dependencies
```

### Contributing:
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request