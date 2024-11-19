import * as dotenv from 'dotenv';
import { BskyAgent } from '@atproto/api';
import { MongoClient } from 'mongodb';
import fs from 'fs';
import yaml from 'js-yaml';
import winston from 'winston';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import path from 'path'; 
import { promises as fsPromises } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config();

const userSchema = {
    did: String,
    handle: String,
    display_name: String,
    last_updated: Date,
    profile_check_needed: Boolean
};

const starterPackSchema = {
    rkey: String,
    name: String,
    creator: String,
    creator_did: String,
    description: String,
    user_count: Number,
    created_at: Date,
    updated_at: Date,
    users: [{ type: String, ref: 'did' }] // References to user DIDs
};

// Configure winston for advanced logging
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => `${timestamp} [${level.toUpperCase()}]: ${message}`)
    ),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'data_processor.log' }),
    ],
});

class FileManager {
    constructor() {
        this.existingPacks = new Map();
        this.jsonStream = null;
        this.yamlStream = null;
        this.isFirstEntry = true;
    }
    
    async safeCleanup() {
        try {
            if (this.jsonStream && !this.jsonStream.closed) {
                await new Promise((resolve, reject) => {
                    this.jsonStream.end('\n]\n', err => {
                        if (err) reject(err);
                        else resolve();
                    });
                });
            }
            
            if (this.yamlStream && !this.yamlStream.closed) {
                await new Promise((resolve, reject) => {
                    this.yamlStream.end('', err => {
                        if (err) reject(err);
                        else resolve();
                    });
                });
            }
        } catch (err) {
            logger.error(`Error in safe cleanup: ${err.stack || err.message}`);
            throw err;
        }
    }

    async cleanupTempFiles() {
        const dir = './';
        try {
            const files = await fs.promises.readdir(dir);
            for (const file of files) {
                if (file.endsWith('.tmp')) {
                    try {
                        await fs.promises.unlink(path.join(dir, file));
                        logger.info(`Cleaned up temporary file: ${file}`);
                    } catch (err) {
                        logger.warn(`Failed to clean up temporary file ${file}: ${err.message}`);
                    }
                }
            }
        } catch (err) {
            logger.error(`Error cleaning up temp files: ${err.message}`);
        }
    }

    async atomicWrite(path, content) {
        const tempPath = `${path}.tmp`;
        try {
            await fs.promises.writeFile(tempPath, content);
            await fs.promises.rename(tempPath, path);
        } catch (err) {
            try {
                await fs.promises.unlink(tempPath).catch(() => {});
            } catch (_) {}
            throw err;
        }
    }

    async validatePaths(paths) {
        try {
            for (const path of paths) {
                const dir = dirname(path);
                await fs.promises.access(dir, fs.constants.R_OK | fs.constants.W_OK);
            }
            return true;
        } catch (err) {
            logger.error(`Path validation failed: ${err.stack || err.message}`);
            return false;
        }
    }

    async init() {
        try {
            // validation for empty paths
            if (!await this.validatePaths(['starter_packs.json', 'starter_packs.yaml'])) {
                throw new Error('Required file paths are not accessible');
            }
            
            // Check if directory exists and is writable
            const dir = './';
            try {
                await fs.promises.access(dir, fs.constants.R_OK | fs.constants.W_OK);
            } catch (err) {
                throw new Error(`Directory ${dir} is not accessible: ${err.message}`);
            }
            
            // Try to load existing files
            const jsonExists = await fs.promises.access('starter_packs.json')
                .then(() => true)
                .catch(() => false);

            if (jsonExists) {
                // Read and parse existing JSON
                const content = await fs.promises.readFile('starter_packs.json', 'utf-8');
                const trimmedContent = content.replace(/\]\s*$/, ''); // Remove trailing ]
                const packs = JSON.parse(trimmedContent + ']');
                
                packs.forEach(pack => {
                    this.existingPacks.set(pack.rkey, pack);
                });
                
                logger.info(`Loaded ${this.existingPacks.size} existing packs`);

                // Backup old files
                const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
                await fs.promises.rename('starter_packs.json', `starter_packs.${timestamp}.json`);
                await fs.promises.rename('starter_packs.yaml', `starter_packs.${timestamp}.yaml`);
            }

            // Create new files
            this.jsonStream = fs.createWriteStream('starter_packs.json');
            this.yamlStream = fs.createWriteStream('starter_packs.yaml');
            

            this.jsonStream.on('error', (err) => {
                logger.error(`JSON Stream Error: ${err.stack || err.message}`);
            });
                
            this.yamlStream.on('error', (err) => {
                logger.error(`YAML Stream Error: ${err.stack || err.message}`);
            });

            // Write initial content including existing packs
            const initialJson = '[' + Array.from(this.existingPacks.values())
                .map(pack => JSON.stringify(pack, null, 2))
                .join(',\n');
            await new Promise(resolve => this.jsonStream.write(initialJson, resolve));
            
            // Write existing packs to YAML
            for (const pack of this.existingPacks.values()) {
                await new Promise(resolve => 
                    this.yamlStream.write('---\n' + yaml.dump(pack), resolve)
                );
            }

            this.isFirstEntry = this.existingPacks.size === 0;
        } catch (err) {
            logger.error(`Error initializing file manager: ${err.stack || err.message}`);
            throw err;
        }
    }

    async getExistingProfile(did) {
        // Look through existing packs for the profile
        for (const pack of this.existingPacks.values()) {
            if (pack.users && Array.isArray(pack.users)) {
                const user = pack.users.find(u => u.did === did);
                
                if (user) {
                    return user;
                }
            }
        }
        return null;
    }
    
    async safeWrite(stream, content) {
        try {
            await new Promise((resolve, reject) => {
                stream.write(content, err => {
                    if (err) reject(err);
                    else resolve();
                });
            });
        } catch (err) {
            logger.error(`Error writing to stream: ${err.stack || err.message}`);
            throw err;
        }
    }

    async handleRemovedProfiles(pack) {
        const existingPack = this.existingPacks.get(pack.rkey);
        if (!existingPack) return;
    
        const currentDids = new Set(pack.users.map(u => u.did));
        const existingDids = new Set(existingPack.users.map(u => u.did));
        
        const removedDids = [...existingDids].filter(did => !currentDids.has(did));
        if (removedDids.length > 0) {
            logger.info(`${removedDids.length} profiles were removed from pack ${pack.rkey}`);
        }
    }

    async writePack(pack) {
        try {
            await this.handleRemovedProfiles(pack);
            this.existingPacks.set(pack.rkey, pack);
            await this.safeWrite(
                this.jsonStream,
                (this.isFirstEntry ? '' : ',\n') + JSON.stringify(pack, null, 2)
            );
            await this.safeWrite(
                this.yamlStream,
                '---\n' + yaml.dump(pack)
            );
            this.isFirstEntry = false;
        } catch (err) {
            logger.error(`Error writing pack ${pack.rkey}: ${err.stack || err.message}`);
            throw err;
        }
    }

    async cleanup() {
        const cleanupPromises = [];
        try {
            await this.cleanupTempFiles();

            // Close JSON array
            if (this.jsonStream) {
                cleanupPromises.push(
                    new Promise((resolve, reject) => {
                        this.jsonStream.write('\n]\n', (err) => {
                            if (err) reject(err);
                            this.jsonStream.end(resolve);
                        });
                    })
                );
            }
            if (this.yamlStream) {
                cleanupPromises.push(
                    new Promise((resolve, reject) => {
                        this.yamlStream.end((err) => {
                            if (err) reject(err);
                            resolve();
                        });
                    })
                );
            }
            await Promise.all(cleanupPromises);
        } catch (err) {
            logger.error(`Error cleaning up file manager: ${err.stack || err.message}`);
            throw err; // Important to throw so the parent knows cleanup failed
        }
    }

    getExistingPack(rkey) {
        return this.existingPacks.get(rkey);
    }
}

async function purgeData() {
    logger.info('Starting purge operation...');
    
    // Delete MongoDB collections if --nomongodb is not set
    if (globalProcessor && !globalProcessor.noMongoDB) {
        try {
            // First drop any existing collections
            await globalProcessor.db.collection('users').drop().catch(err => {
                if (err.code !== 26) { // 26 is collection doesn't exist
                    logger.warn(`Warning dropping users collection: ${err.message}`);
                }
            });
            await globalProcessor.db.collection('starter_packs').drop().catch(err => {
                if (err.code !== 26) {
                    logger.warn(`Warning dropping starter_packs collection: ${err.message}`);
                }
            });
            logger.info('MongoDB collections dropped successfully');

            // Then recreate collections and indexes
            await globalProcessor.setupDatabase();
            logger.info('MongoDB collections and indexes recreated');

        } catch (err) {
            logger.error(`Error during purge operation: ${err.stack || err.message}`);
            throw err;
        }
    }
    
    // Delete JSON and YAML files
    const filesToDelete = ['starter_packs.json', 'starter_packs.yaml'];
    for (const file of filesToDelete) {
        try {
            await fs.promises.unlink(file).catch(err => {
                if (err.code !== 'ENOENT') { // ENOENT means file doesn't exist
                    throw err;
                }
            });
            logger.info(`Deleted ${file}`);
        } catch (err) {
            logger.error(`Error deleting ${file}: ${err.stack || err.message}`);
            throw err;
        }
    }

    // Clear checkpoints
    try {
        const checkpointFiles = ['checkpoints.json', 'checkpoints.json.lock'];
        for (const file of checkpointFiles) {
            await fs.promises.unlink(file).catch(() => {}); // Ignore errors if files don't exist
        }
        logger.info('Checkpoint files cleared');
    } catch (err) {
        logger.warn(`Warning clearing checkpoint files: ${err.message}`);
    }
    
    logger.info('Purge operation completed successfully');
}

// for MongoDB update mode
async function updateMongoDBFromFiles() {
    let mongoClient = null;
    try {
        // First backup existing data
        const backupTimestamp = new Date().toISOString().replace(/[:.]/g, '-');
        await fs.promises.copyFile('starter_packs.json', `starter_packs.${backupTimestamp}.backup.json`);
        
        const content = await fs.promises.readFile('starter_packs.json', 'utf-8');
        const packs = JSON.parse(content);
        
        mongoClient = new MongoClient(process.env.MONGODB_URI);
        await mongoClient.connect();
        const db = mongoClient.db('starterpacks');
        
        // First, get all existing pack_ids
        const existingPacks = await db.collection('users').distinct('pack_id');
        const newPacks = new Set(packs.map(p => p.rkey));
        
        // Remove users from packs that no longer exist
        const removedPacks = existingPacks.filter(id => !newPacks.has(id));
        if (removedPacks.length > 0) {
            await db.collection('users').deleteMany({
                pack_id: { $in: removedPacks }
            });
            logger.info(`Removed users from ${removedPacks.length} deleted packs`);
        }
        
        // Update operations for current packs
        const operations = [];
        for (const pack of packs) {
            // Get current users in this pack
            const currentUsers = await db.collection('users')
                .find({ pack_id: pack.rkey })
                .project({ did: 1 })
                .toArray();
            const currentDids = new Set(currentUsers.map(u => u.did));
            
            // Find removed users
            const packDids = new Set(pack.users.map(u => u.did));
            const removedUsers = [...currentDids].filter(did => !packDids.has(did));
            
            // Delete removed users
            if (removedUsers.length > 0) {
                await db.collection('users').deleteMany({
                    did: { $in: removedUsers },
                    pack_id: pack.rkey
                });
                logger.info(`Removed ${removedUsers.length} users from pack ${pack.rkey}`);
            }
            
            // Update/insert current users
            for (const user of pack.users) {
                operations.push({
                    updateOne: {
                        filter: { did: user.did },
                        update: {
                            $set: {
                                did: user.did,
                                handle: user.handle,
                                display_name: user.display_name || '',
                                last_updated: new Date(),
                                profile_check_needed: false
                            },
                            $addToSet: { pack_ids: pack.rkey }
                        },
                        upsert: true
                    }
                });
            }
        }

        // Process in batches
        const BATCH_SIZE = 1000;
        for (let i = 0; i < operations.length; i += BATCH_SIZE) {
            const batch = operations.slice(i, i + BATCH_SIZE);
            await db.collection('users').bulkWrite(batch);
            logger.info(`Processed ${i + batch.length}/${operations.length} MongoDB operations`);
        }

        logger.info('MongoDB update completed');
        await mongoClient.close();
    } catch (err) {
        logger.error(`Error updating MongoDB from files: ${err.stack || err.message}`);
        throw err;
    } finally {
        if (mongoClient) {
            try {
                await mongoClient.close();
            } catch (err) {
                logger.error(`Error closing MongoDB connection: ${err.stack || err.message}`);
            }
        }
    }
}

class CheckpointManager {
    constructor(filename = 'checkpoints.json') {
        this.filename = filename;
        this.checkpoints = null; // Will be initialized in init()
    }

    async init() {
        this.checkpoints = await this.loadCheckpoints();
    }
    
    async validateCheckpoints() {
        try {
            const data = await fs.promises.readFile(this.filename, 'utf-8');
            JSON.parse(data); // Validate JSON structure
            return true;
        } catch (err) {
            logger.error(`Invalid checkpoints file: ${err.message}`);
            return false;
        }
    }

    async loadCheckpoints() {
        try {
            await fs.promises.access(this.filename);
            const data = JSON.parse(await fs.promises.readFile(this.filename, 'utf-8'));
            if (data.completedPacks) {
                data.completedPacks = new Set(data.completedPacks);
            }
            logger.info(`Loaded checkpoints: ${JSON.stringify({...data, completedPacks: Array.from(data.completedPacks)}, null, 2)}`);
            return data;
        } catch (err) {
            if (err.code === 'ENOENT') {
                logger.info('No checkpoint file found, starting fresh');
            } else {
                logger.error(`Error loading checkpoints: ${err.stack || err.message}`);
            }
            return this.getInitialCheckpoints();
        }
    }

    getInitialCheckpoints() {
        return {
            lastProcessedIndex: -1,
            lastProcessedDate: null,
            dailyStats: {},
            errors: [],
            completedPacks: new Set(),
            rateLimitHits: [],
        };
    }

    async acquireLock() {
        const lockFile = `${this.filename}.lock`;
        try {
            await fsPromises.writeFile(lockFile, String(process.pid), { flag: 'wx' });
            return true;
        } catch (err) {
            if (err.code === 'EEXIST') {
                const pid = await fsPromises.readFile(lockFile, 'utf8');
                logger.warn(`Lock file exists with PID ${pid}`);
            } else {
                // Log unexpected errors
                logger.error(`Unexpected error acquiring lock: ${err.stack || err.message}`);
            }
            return false;
        }
    }

    async releaseLock() {
        const lockFile = `${this.filename}.lock`;
        try {
            await fsPromises.unlink(lockFile);
        } catch (err) {
            logger.warn(`Error releasing lock: ${err.stack || err.message}`);
        }
    }

    async saveCheckpoints() {
        if (await this.acquireLock()) {
            const tempFile = `${this.filename}.temp`;
            try {
                const checkpointsToSave = {
                    ...this.checkpoints,
                    completedPacks: Array.from(this.checkpoints.completedPacks)
                };
                await fs.promises.writeFile(tempFile, JSON.stringify(checkpointsToSave, null, 2));
                await fs.promises.rename(tempFile, this.filename);
            } catch (err) {
                logger.error(`Error saving checkpoints: ${err.stack || err.message}`);
                try {
                    await fs.promises.unlink(tempFile).catch(() => {});
                } catch (_) {}
            } finally {
                await this.releaseLock();
            }
        }
    }

    updateProgress(index, rkey, status = 'success', error = null) {
        const today = new Date().toISOString().split('T')[0];
        
        // Initialize daily stats if needed
        if (!this.checkpoints.dailyStats[today]) {
            this.checkpoints.dailyStats[today] = {
                processed: 0,
                successful: 0,
                errors: 0,
                rateLimitHits: 0
            };
        }

        // Update stats
        this.checkpoints.dailyStats[today].processed++;
        if (status === 'success') {
            this.checkpoints.dailyStats[today].successful++;
            this.checkpoints.completedPacks.add(rkey);
        } else if (status === 'error') {
            this.checkpoints.dailyStats[today].errors++;
            this.checkpoints.errors.push({
                timestamp: new Date().toISOString(),
                index,
                rkey,
                error: error?.message || error
            });
        } else if (status === 'rateLimit') {
            this.checkpoints.dailyStats[today].rateLimitHits++;
            this.checkpoints.rateLimitHits.push({
                timestamp: new Date().toISOString(),
                index,
                rkey
            });
        }

        this.checkpoints.lastProcessedIndex = index;
        this.checkpoints.lastProcessedDate = new Date().toISOString();
        
        // Save after each update
        this.saveCheckpoints();
    }
    
    shouldProcessPack(rkey) {
        return !this.checkpoints.completedPacks.has(rkey);
    }

    getLastProcessedIndex() {
        return this.checkpoints.lastProcessedIndex;
    }

    getDailyStats(date = new Date().toISOString().split('T')[0]) {
        return this.checkpoints.dailyStats[date] || null;
    }
}

class BlueSkyRateLimiter {
    constructor() {
        // Overall rate limit: 3000 requests per 5 minutes
        this.requestWindow = 5 * 60 * 1000; // 5 minutes in milliseconds
        this.maxRequests = 3000;
        
        // Keep track of requests with timestamps
        this.requests = [];
        
        // Add safety margin to stay well below limits
        this.safetyFactor = 0.8; // Use 80% of max rate
        
        // Exponential backoff settings
        this.initialBackoff = 1000; // Start with 1 second
        this.maxBackoff = 60000;    // Max 1 minute
        this.currentBackoff = this.initialBackoff;
        
        // Counter for consecutive 429s
        this.consecutive429s = 0;
    }

    async throttle() {
        const now = Date.now();
        
        // Remove requests older than the window
        this.requests = this.requests.filter(time => now - time < this.requestWindow);
        
        // Calculate effective limits with safety margin
        const effectiveMaxRequests = Math.floor(this.maxRequests * this.safetyFactor);
        
        if (this.requests.length >= effectiveMaxRequests) {
            // Calculate required wait time
            const oldestRequest = this.requests[0];
            const windowEndTime = oldestRequest + this.requestWindow;
            const baseWaitTime = windowEndTime - now;
            
            // Apply exponential backoff if we're getting close to limits
            const backoffWaitTime = this.currentBackoff * (this.requests.length / effectiveMaxRequests);
            
            const waitTime = Math.max(baseWaitTime, backoffWaitTime);
            
            if (waitTime > 0) {
                logger.info(`Rate limit approaching, waiting ${Math.round(waitTime/1000)}s (${this.requests.length}/${effectiveMaxRequests} requests in window)`);
                await new Promise(resolve => setTimeout(resolve, waitTime));
                
                // Increase backoff for next time
                this.currentBackoff = Math.min(this.currentBackoff * 2, this.maxBackoff);
            }
        } else {
            // Reset backoff if we're well below limits
            if (this.requests.length < effectiveMaxRequests * 0.5) {
                this.currentBackoff = this.initialBackoff;
            }
        }
        
        this.requests.push(now);
    }

    async handleResponse(response) {
        // Check for rate limit headers
        const remaining = response?.headers?.['x-ratelimit-remaining'];
        const reset = response?.headers?.['x-ratelimit-reset'];
        
        if (remaining !== undefined) {
            logger.info(`Rate limit remaining: ${remaining}, reset: ${reset}`);
            
            if (remaining < 100) {
                // If we're getting low on remaining requests, add artificial delay
                const delayMs = Math.max(1000, (this.requestWindow / this.maxRequests) * 2);
                logger.info(`Low on remaining requests (${remaining}), adding ${delayMs}ms delay`);
                await new Promise(resolve => setTimeout(resolve, delayMs));
            }
        }
        
        // Handle 429 responses
        if (response?.status === 429) {
            this.consecutive429s++;
            
            // Get retry-after header or use exponential backoff
            const retryAfter = response?.headers?.['retry-after'];
            const waitTime = retryAfter ? 
                (parseInt(retryAfter) * 1000) : 
                (Math.min(this.initialBackoff * Math.pow(2, this.consecutive429s), this.maxBackoff));
            
            logger.warn(`Rate limit exceeded (429). Waiting ${Math.round(waitTime/1000)}s before retry`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
            return true; // Signal that request should be retried
        }
        
        // Reset consecutive 429s on successful response
        this.consecutive429s = 0;
        return false; // Signal that request was successful
    }
}

class StarterPackProcessor {
    constructor() {
        // Get command line arguments
        const args = process.argv.slice(2);
        this.noMongoDB = args.includes('--nomongodb');
        this.updateMongoDB = args.includes('--updatemongodb');
    
        if (!this.updateMongoDB) {
            // Normal mode or --nomongodb mode
            this.agent = new BskyAgent({ service: 'https://bsky.social' });
            if (!this.noMongoDB) {
                this.mongoClient = new MongoClient(process.env.MONGODB_URI);
            }
            this.db = null;
            this.rateLimiter = new BlueSkyRateLimiter();
            this.lastTokenRefresh = Date.now();
            this.tokenRefreshInterval = 45 * 60 * 1000;
            this.checkpointManager = new CheckpointManager();
            this.fileManager = new FileManager(); // New addition
            this.isInitialized = false;
            this.profileCache = new Map();
            this.profileCacheExpiry = 24 * 60 * 60 * 1000;
        }
    }
    
    async checkMongoHealth() {
        if (this.noMongoDB) return true;
        try {
            await this.db.admin().ping();
            return true;
        } catch (err) {
            logger.error(`MongoDB health check failed: ${err.message}`);
            return false;
        }
    }

    async withTransaction(operations) {
        const TRANSACTION_TIMEOUT = 30000;
        const session = this.mongoClient.startSession();
        try {
            await Promise.race([
                session.withTransaction(async () => {
                    await operations(session);
                }, {
                    readPreference: 'primary',
                    readConcern: { level: 'majority' },
                    writeConcern: { w: 'majority' },
                    maxCommitTimeMS: TRANSACTION_TIMEOUT
                }),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Transaction timeout')), TRANSACTION_TIMEOUT)
                )
            ]);
        } finally {
            await session.endSession();
        }
    }

    async refreshTokenIfNeeded(forceRefresh = false) {
        const now = Date.now();
        if (forceRefresh || !this.lastTokenRefresh || (now - this.lastTokenRefresh >= this.tokenRefreshInterval)) {
            const maxRetries = 3;
            let retryCount = 0;
            
            while (retryCount < maxRetries) {
                try {
                    logger.info('Refreshing authentication token...');
                    await this.agent.login({
                        identifier: process.env.BSKY_USERNAME,
                        password: process.env.BSKY_PASSWORD,
                    });
                    this.lastTokenRefresh = now;
                    logger.info('Token refreshed successfully');
                    return;
                } catch (err) {
                    retryCount++;
                    logger.error(`Token refresh attempt ${retryCount}/${maxRetries} failed: $${err.stack || err.message}`);
                    
                    if (retryCount === maxRetries) {
                        throw new Error(`Failed to refresh token after ${maxRetries} attempts: ${err.stack || err.message}`);
                    }
                    
                    // Exponential backoff
                    await new Promise(resolve => setTimeout(resolve, Math.pow(2, retryCount) * 1000));
                }
            }
        }
    }

    async setupDatabase() {
        try {
            await Promise.race([
                this.mongoClient.connect(),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('MongoDB connection timeout')), 30000)
                )
            ]);
            this.db = this.mongoClient.db('starterpacks');    
            
            // Drop existing collections if they exist
            await this.db.collection('users').drop().catch(() => {});
            await this.db.collection('starter_packs').drop().catch(() => {});
            
            // Create collections with schemas
            await this.db.createCollection('users');
            await this.db.createCollection('starter_packs');
            
            // Create indexes in parallel
            await Promise.all([
                this.db.collection('users').createIndex({ did: 1 }, { unique: true }),
                this.db.collection('users').createIndex({ handle: 1 }),
                this.db.collection('users').createIndex({ last_updated: 1 }),
                this.db.collection('users').createIndex({ profile_check_needed: 1 }),
                this.db.collection('starter_packs').createIndex({ rkey: 1 }, { unique: true }),
                this.db.collection('starter_packs').createIndex({ creator_did: 1 })
            ]).catch(err => {
                logger.error(`Error creating indexes: ${err.message}`);
                throw err;
            });
            
            logger.info('Connected to MongoDB and indexes created.');
        } catch (err) {
            logger.error(`Error setting up the database: ${err.stack || err.message}`);
            process.exit(1);
        }
    }

    async setupAgent() {
        try {
            await this.agent.login({
                identifier: process.env.BSKY_USERNAME,
                password: process.env.BSKY_PASSWORD,
            });
            logger.info('Authenticated with BskyAgent.');
        } catch (err) {
            logger.error(`Error authenticating BskyAgent: ${err.stack || err.message}`);
            process.exit(1);
        }
    }

    /**
     * Sanitize handle before resolution
     * @param {string} handle - The handle to sanitize
     * @returns {string} - Sanitized handle
     */
    sanitizeHandle(handle) {
        if (!handle || typeof handle !== 'string') {
            throw new Error('Invalid handle provided');
        }
        
        // Remove any trailing .bsky.social if present
        handle = handle.replace(/\.bsky\.social$/, '');
    
        // Remove any protocol prefixes if present
        handle = handle.replace(/^(http:\/\/|https:\/\/)/, '');
    
        // Remove any trailing periods
        handle = handle.replace(/\.$/, '');
    
        // Remove any whitespace
        handle = handle.trim();
    
        // Validate handle format
        if (!/^[a-zA-Z0-9._-]+$/.test(handle.replace('.bsky.social', ''))) {
            throw new Error('Invalid handle format');
        }
    
        // Ensure the handle format is valid
        if (!handle.includes('.')) {
            handle = `${handle}.bsky.social`;
        }
    
        return handle.toLowerCase();
    }

    async getAllListMembers(uri) {
        let allMembers = [];
        let cursor;
        
        do {
            try {
                const response = await this.getListMembers(uri, cursor);
                if (!response) break;
                
                allMembers = allMembers.concat(response.items);
                cursor = response.cursor;
                
                if (cursor) {
                    logger.info(`Fetched ${allMembers.length} members so far, getting more...`);
                    await this.delay(1000);
                }
            } catch (err) {
                logger.error(`Error fetching list page: ${err.stack || err.message}`);
                break;
            }
        } while (cursor);
        
        return allMembers;
    }
    
    async getListMembers(uri, cursor) {
        await this.refreshTokenIfNeeded();
    
        const maxRetries = 3;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.rateLimiter.throttle();
    
                const response = await this.apiCallWithTimeout(
                    this.agent.api.app.bsky.graph.getList({
                        list: uri,
                        limit: 100,
                        cursor
                    })
                );
                
                if (!response?.data) {
                    throw new Error('Invalid API response structure');
                }

                const shouldRetry = await this.rateLimiter.handleResponse(response);
                if (shouldRetry && attempt < maxRetries - 1) {
                    continue;
                }
    
                if (!response?.data?.items) {
                    logger.error(`No items found in getList response for ${uri}`);
                    return null;
                }
    
                const members = response.data.items;
                logger.info(`Found ${members.length} members in list${cursor ? ' (continuation)' : ''}`);
    
                return {
                    items: members || [],
                    cursor: response.data.cursor
                };
    
            } catch (err) {
                if (err.status === 404) {
                    logger.error(`List not found: ${uri}`);
                    return null;
                }
                
                if (err.message === 'Token has expired' || err.message === 'Authentication Required') {
                    logger.info('Token expired, forcing refresh...');
                    await this.refreshTokenIfNeeded(true);
                    continue;
                }
    
                if (err.status === 429 && attempt < maxRetries - 1) {
                    const shouldRetry = await this.rateLimiter.handleResponse(err);
                    if (shouldRetry) continue;
                }
    
                if (attempt < maxRetries - 1) {
                    const delay = Math.pow(2, attempt) * 1000;
                    logger.info(`Attempt ${attempt + 1} failed, waiting ${delay}ms before retry...`);
                    await this.delay(delay);
                    continue;
                }
    
                logger.error(`Error getting list members for ${uri}: ${err.stack || err.message}`);
                if (err.message.includes('timeout')) {
                    logger.error('Request timed out, will retry');
                    return null;
                }
            }
        }
        return null;
    }

    async getProfile(did, forceUpdate = false) {
        const TEN_DAYS = 10 * 24 * 60 * 60 * 1000;
        
        // Check memory cache first
        const cached = this.profileCache.get(did);
        if (!forceUpdate && cached && Date.now() - cached.timestamp < this.profileCacheExpiry) {
            return cached.profile;
        }
    
        // Check MongoDB if enabled
        if (!this.noMongoDB) {
            try {
                const userDoc = await this.db.collection('users').findOne({ did });
                if (userDoc) {
                    const needsUpdate = Date.now() - userDoc.last_updated.getTime() > TEN_DAYS;
                    
                    if (!needsUpdate && !forceUpdate) {
                        // Map MongoDB fields to API response fields
                        const profile = {
                            did: userDoc.did,
                            handle: userDoc.handle,
                            displayName: userDoc.display_name, // Map display_name to displayName
                            // Add other necessary mappings if required
                        };
                        this.profileCache.set(did, {
                            profile: profile,
                            timestamp: Date.now()
                        });
                        return profile;
                    }
                    
                    // Mark for update if older than 10 days
                    if (needsUpdate) {
                        await this.db.collection('users').updateOne(
                            { did },
                            { $set: { profile_check_needed: true } }
                        );
                    }
                }
            } catch (err) {
                logger.error(`MongoDB profile check error for ${did}: ${err.stack || err.message}`);
            }
        }
    
        // Fetch from API if needed
        const profile = await this.fetchProfileFromAPI(did);
        if (profile) {
            // Create user data with proper display name handling
            const userData = {
                did: did,  // Use the original DID, not profile.did
                handle: profile.handle,
                displayName: profile.displayName,  // Ensure consistency
                last_updated: new Date(),
                profile_check_needed: false
            };
    
            // Update MongoDB
            if (!this.noMongoDB) {
                await this.db.collection('users').updateOne(
                    { did },
                    { $set: userData },
                    { upsert: true }
                );
                
                await this.db.collection('starter_packs').updateMany(
                    { users: did },
                    { $set: { updated_at: new Date() } }
                );
            }
    
            this.profileCache.set(did, {
                profile: userData,
                timestamp: Date.now()
            });
    
            return userData;
        }
        
        return null;
    }

    async getProcessingStatus() {
        try {
            const totalPacks = await this.db.collection('starter_packs').countDocuments();
            const processedPacks = await this.db.collection('starter_packs').countDocuments({ updated_at: { $exists: true } });
            const totalUsers = await this.db.collection('users').countDocuments();
            const needsUpdate = await this.db.collection('users').countDocuments({ profile_check_needed: true });
            
            return {
                total_packs: totalPacks,
                processed_packs: processedPacks,
                completion_percentage: ((processedPacks / totalPacks) * 100).toFixed(2),
                total_users: totalUsers,
                profiles_need_update: needsUpdate
            };
        } catch (err) {
            logger.error(`Error getting processing status: ${err.stack || err.message}`);
            return null;
        }
    }

    async fetchProfileFromAPI(did) {
        await this.refreshTokenIfNeeded();
        const maxRetries = 3;
        
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.rateLimiter.throttle();
                
                const response = await this.apiCallWithTimeout(
                    this.agent.api.app.bsky.actor.getProfile({
                        actor: did
                    })
                );
                
                // debug logging
                //logger.info(`Profile response for ${did}: ${JSON.stringify(response?.data, null, 2)}`);
                
                const shouldRetry = await this.rateLimiter.handleResponse(response);
                if (shouldRetry && attempt < maxRetries - 1) {
                    continue;
                }
                
                if (response?.data) {
                    const profile = response.data;
                    
                    // Check if display name exists and is not just whitespace
                    const displayName = profile.displayName?.trim() || profile.handle;
                    
                    // Create a complete profile object
                    return {
                        did: profile.did,
                        handle: profile.handle,
                        displayName: displayName
                    };
                }
                return null;
            } catch (err) {
                if (err.status === 429 && attempt < maxRetries - 1) {
                    const shouldRetry = await this.rateLimiter.handleResponse(err);
                    if (shouldRetry) continue;
                }
                logger.error(`Error fetching profile for ${did}: ${err.stack || err.message}`);
                
                if (attempt < maxRetries - 1) {
                    await this.delay(Math.pow(2, attempt) * 1000);
                }
            }
        }
        return null;
    }

    // check if we already have processed this profile
    async isProfileProcessed(did, packId) {
        try {
            const result = await this.db.collection('users').findOne({ 
                did: did, 
                pack_id: packId 
            });
            return !!result;
        } catch (err) {
            logger.error(`Error checking if profile is processed: ${err.stack || err.message}`);
            return false;
        }
    }

    async getRecord(uri) {
        await this.refreshTokenIfNeeded();

        const maxRetries = 3;
        const [repo, collection, rkey] = uri.replace('at://', '').split('/');
        
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.rateLimiter.throttle();
                
                const response = await this.agent.api.com.atproto.repo.getRecord({
                    repo,
                    collection,
                    rkey
                });
                
                const shouldRetry = await this.rateLimiter.handleResponse(response);
                if (shouldRetry && attempt < maxRetries - 1) {
                    continue;
                }
                
                if (!response?.data?.value) {
                    logger.error(`No value found in record response for ${uri}`);
                    return null;
                }
                
                return response.data;
            } catch (err) {
                if (err.status === 429 && attempt < maxRetries - 1) {
                    const shouldRetry = await this.rateLimiter.handleResponse(err);
                    if (shouldRetry) continue;
                }
                logger.error(`Error getting ${uri}: ${err.stack || err.message}`);
            }
        }
        return null;
    }
    
    /**
     * Resolves a handle to its DID with retry mechanism
     * @param {string} rawHandle - The handle to resolve
     * @param {number} retries - Current retry count
     * @returns {Promise<string|null>} - The DID or null if failed
     */
    async resolveHandleWithRetry(rawHandle, retries = 0) {
        await this.refreshTokenIfNeeded();
        const MAX_RETRIES = 3;
        const RETRY_DELAY = 2000;
    
        try {
            await this.rateLimiter.throttle();
            const handle = this.sanitizeHandle(rawHandle);
            logger.info(`Attempting to resolve sanitized handle: ${handle}`);
            
            const response = await this.agent.resolveHandle({ handle });
            
            if (response?.data?.did) {
                return response.data.did;
            }
            throw new Error('No DID found in the response.');
        } catch (err) {
            if (err.message === 'Token has expired' || err.message === 'Authentication Required') {
                if (retries < MAX_RETRIES - 1) {
                    logger.info('Token expired, forcing refresh...');
                    await this.refreshTokenIfNeeded(true);
                    return this.resolveHandleWithRetry(rawHandle, retries + 1);
                }
            }
    
            if (err.status === 429 && retries < MAX_RETRIES - 1) {
                const shouldRetry = await this.rateLimiter.handleResponse(err);
                if (shouldRetry) {
                    await this.delay(RETRY_DELAY * (retries + 1));
                    return this.resolveHandleWithRetry(rawHandle, retries + 1);
                }
            }
    
            if (retries < MAX_RETRIES - 1) {
                logger.info(`Retrying handle resolution for ${rawHandle} (${retries + 2}/${MAX_RETRIES})...`);
                await this.delay(RETRY_DELAY * (retries + 1));
                return this.resolveHandleWithRetry(rawHandle, retries + 1);
            }
    
            logger.error(`Failed to resolve handle ${rawHandle} after ${MAX_RETRIES} attempts: ${err.stack || err.message}`);
            return null;
        }
    }

    async ensureDbConnection() {
        const maxRetries = 3;
        let retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                // Check connection
                await this.mongoClient.db().command({ ping: 1 });
                return; // Connection is good
            } catch (err) {
                retryCount++;
                logger.warn(`MongoDB connection attempt ${retryCount}/${maxRetries} failed: ${err.stack || err.message}`);
                
                if (retryCount === maxRetries) {
                    logger.error('Failed to establish MongoDB connection after maximum retries');
                    throw err;
                }
                
                // Close existing connection if it exists
                try {
                    await this.mongoClient.close();
                } catch (closeErr) {
                    logger.warn(`Error closing MongoDB connection: ${closeErr.message}`);
                }
                
                // Create new client instance
                this.mongoClient = new MongoClient(process.env.MONGODB_URI);
                
                try {
                    await this.mongoClient.connect();
                } catch (connectErr) {
                    logger.error(`Error reconnecting to MongoDB: ${connectErr.message}`);
                    // Continue to next retry
                }
                
                // Wait before retry
                await new Promise(resolve => setTimeout(resolve, 5000 * retryCount));
            }
        }
    }

    async apiCallWithTimeout(promise, timeout = 30000) {
        const timeoutPromise = new Promise((_, reject) => {
            const timeoutId = setTimeout(() => {
                clearTimeout(timeoutId);
                reject(new Error('API call timed out'));
            }, timeout);
        });
    
        try {
            return await Promise.race([promise, timeoutPromise]);
        } catch (err) {
            if (err.message.includes('timeout')) {
                logger.warn('API call timed out, will retry');
            }
            throw err;
        }
    }
    
    async processStarterPack(urlLine) {
        let mongoConnection = false;
        try {
            if (!this.noMongoDB) {
                await this.ensureDbConnection();
                mongoConnection = true;
            }
            // Input validation
            if (!urlLine || !urlLine.includes('|')) {
                logger.error(`Invalid URL line format: ${urlLine}`);
                return false;
            }
    
            const [creatorHandle, rkey] = urlLine.trim().split('|').map(s => s.trim());
            if (!creatorHandle || !rkey) {
                logger.error(`Missing handle or rkey in line: ${urlLine}`);
                return false;
            }
    
            logger.info(`\n=== Starting to process pack by ${creatorHandle}: ${rkey} ===`);
    
            // First check if we already have this pack and it's already processed
            const existingPack = this.fileManager.getExistingPack(rkey);
            if (existingPack && !this.checkpointManager.shouldProcessPack(rkey)) {
                logger.info(`Pack ${rkey} already processed, skipping...`);
                return true;
            }
    
            // Resolve creator's DID with retries
            let creatorDID = await this.resolveHandleWithRetry(creatorHandle);
            if (!creatorDID) {
                logger.error(`Failed to resolve creator DID for ${creatorHandle}, skipping pack ${rkey}`);
                return false;
            }
    
            // Fetch starter pack record
            const recordUri = `at://${creatorDID}/app.bsky.graph.starterpack/${rkey}`;
            const record = await this.getRecord(recordUri);
            if (!record || !record.value) {
                logger.error(`Could not fetch starter pack record: ${recordUri}`);
                return false;
            }
    
            const { value } = record;
            logger.info(`Pack name: ${value.name}`);
            logger.info(`Description: ${value.description || 'No description'}`);
            logger.info(`List URI: ${value.list}`);
    
            // Fetch all list members
            const listMembers = await this.getAllListMembers(value.list);
            if (!listMembers || listMembers.length === 0) {
                logger.error(`No members found in the list for pack ${rkey}`);
                return false;
            }
    
            logger.info(`Processing ${listMembers.length} members...`);
    
            // Process user profiles
            const processedUsers = [];
            const mongodbOperations = [];
            const failedProfiles = [];
    
            for (const member of listMembers) {
                const memberDid = member.did || (member.subject ? member.subject.did : null);
                if (!memberDid) {
                    logger.error(`Could not extract DID from member: ${JSON.stringify(member)}`);
                    continue;
                }
    
                try {
                    // First profile fetch attempt
                    let profile = await this.getProfile(memberDid);
                    
                    // Handle empty display names
                    if (profile && (!profile.displayName || profile.displayName.trim() === '')) {
                        logger.warn(`WARNING: Empty display name for DID ${memberDid} (handle: ${profile.handle})`);
                        await this.delay(2000); // Wait before retry
                        
                        // Second attempt with force update
                        profile = await this.getProfile(memberDid, true);
                        
                        if (!profile.displayName || profile.displayName.trim() === '') {
                            logger.warn(`Display name still empty after retry for ${memberDid}`);
                        }
                    }
    
                    if (profile) {
                        const userData = {
                            did: memberDid,
                            handle: profile.handle,
                            display_name: profile.displayName || '', // Keep empty if truly empty
                        };
    
                        processedUsers.push(userData);
    
                        // Prepare MongoDB operations if enabled
                        if (!this.noMongoDB) {
                            mongodbOperations.push({
                                updateOne: {
                                    filter: { did: memberDid },
                                    update: {
                                        $set: {
                                            did: memberDid,
                                            handle: profile.handle,
                                            display_name: profile.displayName || '',
                                            last_updated: new Date(),
                                            profile_check_needed: false
                                        },
                                        $addToSet: { pack_ids: rkey }
                                    },
                                    upsert: true
                                }
                            });
                        }
                    } else {
                        failedProfiles.push({ did: memberDid, reason: 'Profile fetch failed' });
                    }
                } catch (err) {
                    logger.error(`Error processing profile ${memberDid}: ${err.stack || err.message}`);
                    failedProfiles.push({ did: memberDid, reason: err.message });
                    continue; // Continue with next user even if one fails
                }
            }
    
            // Log failed profiles summary if any
            if (failedProfiles.length > 0) {
                logger.warn(`Failed to process ${failedProfiles.length} profiles in pack ${rkey}:`);
                failedProfiles.forEach(f => logger.warn(`- ${f.did}: ${f.reason}`));
            }
    
            // Prepare pack data
            const packData = {
                name: value.name,
                creator: creatorHandle,
                rkey: rkey,
                url: `https://bsky.app/profile/${creatorHandle}/lists/${rkey}`,
                user_count: processedUsers.length,
                total_attempted: listMembers.length,
                failed_count: failedProfiles.length,
                users: processedUsers,
                updated_at: new Date().toISOString()
            };
    
            // MongoDB operations if enabled
            if (!this.noMongoDB && mongodbOperations.length > 0) {
                const MONGODB_OPERATION_TIMEOUT = 30000;
                const BATCH_SIZE = 1000;
                
                try {
                    for (let i = 0; i < mongodbOperations.length; i += BATCH_SIZE) {
                        const batch = mongodbOperations.slice(i, i + BATCH_SIZE);
                        const result = await Promise.race([
                            this.db.collection('users').bulkWrite(batch, { 
                                ordered: false,
                                w: 1,
                                wtimeout: MONGODB_OPERATION_TIMEOUT 
                            }),
                            new Promise((_, reject) => 
                                setTimeout(() => reject(new Error('MongoDB operation timeout')), MONGODB_OPERATION_TIMEOUT)
                            )
                        ]);

                        // Validate result
                        if (!result.ok) {
                            throw new Error(`MongoDB write operation failed: ${result.writeErrors?.join(', ')}`);
                        }

                        logger.info(`Processed batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(mongodbOperations.length/BATCH_SIZE)}`);
                    }
                } catch (err) {
                    if (err.message === 'MongoDB operation timeout') {
                        logger.error('MongoDB batch operation timed out');
                    } else if (err.code === 11000) {
                        logger.warn('Duplicate key error occurred, continuing with next batch');
                    } else {
                        throw err;
                    }
                }
            }
    
            // Always write to files
            try {
                await this.fileManager.writePack(packData);
                logger.info(`File update completed for pack ${rkey}`);
            } catch (err) {
                logger.error(`File writing failed for pack ${rkey}: ${err.stack || err.message}`);
                return false;
            }
    
            logger.info(`=== Successfully processed pack ${rkey} ===`);
            logger.info(`Processed ${processedUsers.length}/${listMembers.length} users (${failedProfiles.length} failed)`);
            
            return true; // Indicate successful processing
    
        } catch (err) {
            logger.error(`Error processing pack: ${err.stack || err.message}`);
            
            if (err.code === 'ECONNRESET' || err.message.includes('socket hang up')) {
                logger.info('Network error detected, waiting before retry...');
                await this.delay(5000);
                return false; // Signal retry needed
            }
            
            // connection recovery attempt
            if (mongoConnection && err.name === 'MongoNetworkError') {
                logger.warn('MongoDB connection lost, will attempt reconnection');
                this.mongoClient = new MongoClient(process.env.MONGODB_URI);
            }

            throw err; // Rethrow other errors for handling by caller
        }
    }

    async processUrls(filename) {
        try {
            await fs.promises.access(filename);  

            const content = await fs.promises.readFile(filename, 'utf-8');
            const urls = content
                .split('\n')
                .map(line => line.trim())
                .filter(line => line && line.includes('|'));
    
            logger.info(`Processing ${urls.length} starter packs...`);
    
            let startIndex = Math.max(0, this.checkpointManager.getLastProcessedIndex() + 1);
            logger.info(`Resuming from index ${startIndex}`);
    
            const todayStats = this.checkpointManager.getDailyStats();
            if (todayStats) {
                logger.info(`Today's progress: ${JSON.stringify(todayStats, null, 2)}`);
            }
            
            const startTime = Date.now();
            let lastStatusReport = startTime;
    
            for (const [index, urlLine] of urls.entries()) {
                
                if (index % 50 === 0 && !this.noMongoDB) {
                    const isHealthy = await this.checkMongoHealth();
                    if (!isHealthy) {
                        logger.warn('MongoDB health check failed, attempting reconnection...');
                        await this.ensureDbConnection();
                    }
                }  
                
                if (index < startIndex) continue;
    
                const [creatorHandle, rkey] = urlLine.trim().split('|').map(s => s.trim());
                
                if (!this.checkpointManager.shouldProcessPack(rkey)) {
                    logger.info(`Skipping already processed pack: ${rkey}`);
                    continue;
                }
    
                let retries = 0;
                const MAX_RETRIES = 3;
                let success = false;
    
                while (!success && retries < MAX_RETRIES) {
                    try {
                        const result = await this.processStarterPack(urlLine.trim());
                        if (result !== false) {  // false indicates retry needed
                            success = true;
                            this.checkpointManager.updateProgress(index, rkey, 'success');
                        } else {
                            retries++;
                            if (retries < MAX_RETRIES) {
                                logger.info(`Retrying pack ${rkey} (${retries}/${MAX_RETRIES})...`);
                                await this.delay(Math.pow(2, retries) * 1000);
                            }
                        }
                    } catch (err) {
                        if (err.status === 429) {
                            logger.warn(`Rate limit reached at index ${index}. Saving progress and exiting...`);
                            this.checkpointManager.updateProgress(index, rkey, 'rateLimit', err);
                            await this.cleanup();
                            process.exit(0);
                        } else if (err.code === 'ECONNRESET' || err.message.includes('socket hang up')) {
                            logger.warn(`Network error at index ${index}, retrying...`);
                            retries++;
                            if (retries < MAX_RETRIES) {
                                await this.delay(Math.pow(2, retries) * 1000);
                                continue;
                            }
                        } else {
                            logger.error(`Error processing pack ${rkey}: ${err.stack || err.message}`);
                            this.checkpointManager.updateProgress(index, rkey, 'error', err);
                            retries++;
                            if (retries < MAX_RETRIES) {
                                logger.info(`Retrying after error (${retries}/${MAX_RETRIES})...`);
                                await this.delay(Math.pow(2, retries) * 1000);
                            }
                        }
                    }
                }
    
                // Add delays for rate limiting
                if (index % 10 === 0 && index !== 0) {
                    logger.info('Adding small delay to prevent rate limiting...');
                    await this.delay(1000);
                }
                if (index % 100 === 0 && index !== 0) {
                    logger.info('Adding longer delay to prevent rate limiting...');
                    await this.delay(5000);
                }

                
                // Status report at the end of the loop
                const now = Date.now();
                if (now - lastStatusReport > 5 * 60 * 1000) {
                    const elapsed = (now - startTime) / 1000;
                    const processed = index - startIndex;
                    const rate = processed / (elapsed / 60);
                    const remaining = urls.length - index;
                    const estimatedTimeLeft = remaining / rate;
                    
                    const stats = this.checkpointManager.getDailyStats();
                    logger.info(`
    Status Report:
    Processed: ${processed}/${urls.length} (${(processed/urls.length*100).toFixed(2)}%)
    Rate: ${rate.toFixed(2)} packs/minute
    Est. time remaining: ${(estimatedTimeLeft/60).toFixed(2)} hours
    Success rate: ${stats ? (stats.successful/stats.processed*100).toFixed(2) : 0}%
    Rate limits hit: ${stats?.rateLimitHits || 0}
    Errors: ${stats?.errors || 0}
                    `);
                    
                    lastStatusReport = now;
                }
            }

            await this.cleanup();
        } catch (err) {
            logger.error(`Error reading file ${filename}: ${err.stack || err.message}`);
            throw err;
        }
    }

    async cleanup() {
        try {
            // Clear profile cache
            this.profileCache.clear();
            
            // Clean up file manager
            if (this.fileManager) {
                await this.fileManager.cleanup();
            }
            
            // Close MongoDB connection if it exists
            if (this.mongoClient) {
                await this.mongoClient.close();
            }
        } catch (err) {
            logger.error(`Error during cleanup: ${err.stack || err.message}`);
        }
    }

    async init() {
        if (this.isInitialized) return;
    
        try {
            // Validate required files exist
            await fs.promises.access('starter_pack_urls.txt')
            .catch(() => {
                throw new Error('Required file starter_pack_urls.txt not found');
            });
            
            if (this.updateMongoDB) {
                if (!process.env.MONGODB_URI) {
                    throw new Error('Missing required environment variable: MONGODB_URI');
                }
                this.mongoClient = new MongoClient(process.env.MONGODB_URI);
                await this.setupDatabase();
            } else {
                const requiredVars = ['BSKY_USERNAME', 'BSKY_PASSWORD'];
                if (!this.noMongoDB) {
                    requiredVars.push('MONGODB_URI');
                }
                
                const missingVars = requiredVars.filter(varName => !process.env[varName]);
                if (missingVars.length > 0) {
                    throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
                }
        
                await this.fileManager.init();
                if (!this.checkpointManager) {
                    throw new Error('Failed to initialize checkpoint manager');
                }
                await this.checkpointManager.init();
                if (!this.noMongoDB) {
                    await this.setupDatabase();
                }
                await this.setupAgent();
            }
            
            this.isInitialized = true;
        } catch (err) {
            logger.error(`Initialization failed: ${err.stack || err.message}`);
            throw err;
        }
    }
    
    async collect() {
        await this.init();
        await this.processUrls('starter_pack_urls.txt');
        await this.cleanup();
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

process.on('unhandledRejection', async (reason, promise) => {
    logger.error('Unhandled Rejection at:', promise, 'reason:', reason.stack || reason.message || reason);
    if (globalProcessor) {
        await globalProcessor.cleanup();
    }
    process.exit(1);
});

process.on('uncaughtException', async (error) => {
    logger.error('Uncaught Exception:', error.stack || error.message || error);
    
    if (globalProcessor) {
        await globalProcessor.cleanup();
    }
    process.exit(1);
});

// Create a global reference
let globalProcessor = null;

// MongoDB update functionality
async function main() {
    globalProcessor = new StarterPackProcessor();
    
    if (process.argv.includes('--purge')) {
        await globalProcessor.init();  // Initialize before purge
        await purgeData();
        process.exit(0);
    }
    
    let isCleaningUp = false;

    async function handleShutdown(signal) {
        if (isCleaningUp) return;
        isCleaningUp = true;
        
        logger.info(`Received ${signal}. Starting cleanup...`);
        try {
            if (globalProcessor) {
                await globalProcessor.cleanup();
            }
        } catch (err) {
            logger.error(`Error during cleanup: ${err.stack || err.message}`);
        } finally {
            process.exit(0);
        }
    }

    process.on('SIGINT', () => handleShutdown('SIGINT'));
    process.on('SIGTERM', () => handleShutdown('SIGTERM'));

    try {
        if (globalProcessor.updateMongoDB) {
            // Update MongoDB from files mode
            await updateMongoDBFromFiles();
        } else {
            // Normal or --nomongodb mode
            await globalProcessor.collect();
        }
    } catch (err) {
        logger.error('Fatal error:', err);
        await globalProcessor.cleanup();
        process.exit(1);
    }
}

main().catch(err => {
    logger.error('Fatal error:', err);
    process.exit(1);
});
