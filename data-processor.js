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
import fetch from 'node-fetch';
import readline from 'readline';
import fsExtra from 'fs-extra';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const DB_CONFIGS = {
    cosmos: {
        ssl: true,
        replicaSet: 'globaldb',
        retryWrites: false,
        maxIdleTimeMS: 120000
    },
    mongodb: {
        retryWrites: true,
        maxIdleTimeMS: 300000
    },
    selfhosted: {
        retryWrites: true,
        maxIdleTimeMS: 300000
    }
};

// defining batch sizes
const BATCH_SIZES = {
    cosmos: 100,
    mongodb: 500,
    selfhosted: 1000
};

// defining supported functions
const DB_INFO = {
    cosmos: {
        supportsCollMod: false,
        isCosmosDb: true
    },
    mongodb: {
        supportsCollMod: true,
        isCosmosDb: false
    },
    selfhosted: {
        supportsCollMod: true,
        isCosmosDb: false
    }
};

// Default to cosmos config if not specified
const DB_TYPE = process.env.DB_TYPE || 'cosmos';
const DB_CONFIG = DB_CONFIGS[DB_TYPE] || DB_CONFIGS.cosmos;
const BATCH_SIZE = BATCH_SIZES[DB_TYPE] || BATCH_SIZES.cosmos;

process.on('exit', () => {
    try {
        // Sync operations because we're exiting
        fs.unlinkSync('checkpoints.json.temp');
        fs.unlinkSync('checkpoints.json.lock');
    } catch (err) {
        // Ignore cleanup errors on exit
    }
});

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

class LRUCache {
    constructor(maxSize = 500) {
        this.cache = new Map();
        this.maxSize = maxSize;
        this.keys = [];
    }

    set(key, value) {
        if (this.cache.size >= this.maxSize * 0.9) { // Clear at 90% capacity
            // Remove oldest 20% of entries
            const removeCount = Math.floor(this.maxSize * 0.2);
            for (let i = 0; i < removeCount; i++) {
                const oldestKey = this.keys.shift();
                if (oldestKey) this.cache.delete(oldestKey);
            }
        }
        
        if (this.cache.has(key)) {
            this.keys = this.keys.filter(k => k !== key);
        }
        
        this.cache.set(key, value);
        this.keys.push(key);
    }

    get(key) {
        const value = this.cache.get(key);
        if (value !== undefined) {
            // Move to most recently used
            this.keys = this.keys.filter(k => k !== key);
            this.keys.push(key);
        }
        return value;
    }

    clear() {
        this.cache.clear();
        this.keys = [];
    }
}


class UserManager {
    constructor(processor) {
        this.processor = processor;
        this.userCache = new Map();       // Cache to store user data
        this.updateBuffer = new Map();    // Buffer to store pending updates
        this.batchSize = 10;              // Flush after n users
        this.flushInterval = 5000;        // Flush every 5 seconds
        this.isFlushing = false;          // Flag to prevent concurrent flushes

        // Bind methods to preserve 'this' context
        this.writeUser = this.writeUser.bind(this);
        this.flushUpdates = this.flushUpdates.bind(this);

        // Start the periodic flush timer
        this.startFlushTimer();
    }

    startFlushTimer() {
        this.flushTimer = setInterval(() => {
            if (this.updateBuffer.size > 0) {
                this.flushUpdates().catch(err => {
                    if (this.processor?.debug) {
                        this.processor.debugLog('Error flushing user data', {
                            error: err.message,
                            stack: err.stack
                        });
                    }
                });
            }
        }, this.flushInterval);
    }

    stopFlushTimer() {
        clearInterval(this.flushTimer);
    }

    async writeUser(user, isUpdate = false) {
        // Debugging logs
        if (this.processor?.debug) {
            this.processor.debugLog('UserManager.writeUser called', { user, isUpdate });
        }

        // Update cache
        this.userCache.set(user.did, user);

        // Add or update the user in the buffer
        this.updateBuffer.set(user.did, {
            ...user,
            last_updated: new Date()
        });

        // If buffer size reaches batchSize, flush
        if (this.updateBuffer.size >= this.batchSize) {
            await this.flushUpdates();
        }

        if (this.processor?.debug) {
            this.processor.debugLog('User data buffered successfully', {
                did: user.did,
                handle: user.handle,
                isUpdate
            });
        }
    }

    async flushUpdates() {
        if (this.isFlushing) return; // Prevent concurrent flushes
        this.isFlushing = true;

        const uniqueId = Date.now() + '-' + Math.random().toString(36).substring(2);
        const tempJsonPath = `users.json.${uniqueId}.tmp`;
        const tempYamlPath = `users.yaml.${uniqueId}.tmp`;
        const backupJsonPath = `users.json.${uniqueId}.bak`;
        const backupYamlPath = `users.yaml.${uniqueId}.bak`;

        try {
            if (this.processor?.debug) {
                this.processor.debugLog('Flushing user data. Loading existing content first...')
            }
            // Read existing content
            let existingJson = [];
            let existingYaml = '';

            try {
                const jsonContent = await fs.promises.readFile('users.json', 'utf-8')
                    .catch(err => err.code === 'ENOENT' ? '[]' : Promise.reject(err));

                try {
                    existingJson = JSON.parse(jsonContent);
                    if (!Array.isArray(existingJson)) {
                        existingJson = [];
                    }
                } catch (parseErr) {
                    logger.warn(`Error parsing users.json: ${parseErr.message}`);
                    existingJson = [];
                }

                existingYaml = await fs.promises.readFile('users.yaml', 'utf-8')
                    .catch(() => '');
            } catch (err) {
                logger.error(`Error reading user files: ${err.message}`);
                existingJson = [];
                existingYaml = '';
            }

            // Apply buffered updates
            if (this.processor?.debug) {
                this.processor.debugLog('Applying updates..')
            }
            
            for (const [did, userData] of this.updateBuffer.entries()) {
                const userIndex = existingJson.findIndex(u => u.did === did);
                if (userIndex >= 0) {
                    existingJson[userIndex] = userData;
                } else {
                    existingJson.push(userData);
                }

                // Update YAML content
                existingYaml = existingYaml +
                    (existingYaml && !existingYaml.endsWith('\n') ? '\n' : '') +
                    '---\n' +
                    yaml.dump(userData);
            }

            // Write to temp files
            if (this.processor?.debug) {
                this.processor.debugLog('Writing to temp files..')
            }
            await fs.promises.writeFile(tempJsonPath, JSON.stringify(existingJson, null, 2));
            await fs.promises.writeFile(tempYamlPath, existingYaml);

            // Windows-specific safe file replacement
            if (process.platform === 'win32') {
                // Create backups
                await fs.promises.copyFile('users.json', backupJsonPath).catch(() => {});
                await fs.promises.copyFile('users.yaml', backupYamlPath).catch(() => {});

                // Add delay after backup
                await new Promise(resolve => setTimeout(resolve, 100));

                try {
                    // Replace original files with temp files
                    await fs.promises.copyFile(tempJsonPath, 'users.json');
                    await fs.promises.copyFile(tempYamlPath, 'users.yaml');

                    // Add delay after copy
                    await new Promise(resolve => setTimeout(resolve, 100));

                    // Clean up temp and backup files
                    await fs.promises.unlink(tempJsonPath).catch(() => {});
                    await fs.promises.unlink(tempYamlPath).catch(() => {});
                    await fs.promises.unlink(backupJsonPath).catch(() => {});
                    await fs.promises.unlink(backupYamlPath).catch(() => {});
                } catch (copyErr) {
                    // Restore from backup if copy fails
                    await fs.promises.copyFile(backupJsonPath, 'users.json').catch(() => {});
                    await fs.promises.copyFile(backupYamlPath, 'users.yaml').catch(() => {});
                    throw copyErr;
                }
            } else {
                // Non-Windows platforms can use rename
                await fs.promises.rename(tempJsonPath, 'users.json');
                await fs.promises.rename(tempYamlPath, 'users.yaml');
            }

            // Clear the buffer after successful flush
            this.updateBuffer.clear();

            if (this.processor?.debug) {
                this.processor.debugLog('Batch user data written successfully', {
                    flushedUsers: this.batchSize
                });
            }
        } catch (err) {
            // Handle errors and ensure the buffer remains intact for retry
            if (this.processor?.debug) {
                this.processor.debugLog('Error flushing user data', {
                    error: err.message,
                    stack: err.stack
                });
            }
            logger.error(`Failed to flush updates: ${err.message}`);
            throw err;
        } finally {
            this.isFlushing = false;
        }
    }

    async shutdown() {
        // Stop the flush timer
        this.stopFlushTimer();

        // Flush any remaining updates
        if (this.updateBuffer.size > 0) {
            await this.flushUpdates();
        }

        if (this.processor?.debug) {
            this.processor.debugLog('UserManager shutdown complete');
        }
    }
}

class AtomicFileWriter {
    constructor(processor = null) {
        this.processor = processor;
        this.activeWriteOperations = new Map();
        this.lockTimeout = 30000; // 30 seconds
        this.maxRetries = 3;
    }
        
    async acquireLock(lockPath) {
        const startTime = Date.now();
        while (true) {
            try {
                await fs.promises.writeFile(lockPath, process.pid.toString(), { flag: 'wx' });
                return true;
            } catch (err) {
                if (Date.now() - startTime > this.lockTimeout) {
                    throw new Error('Lock acquisition timeout');
                }
                await this.delay(1000);
            }
        }
    }
    
    async writeAtomic(filepath, content, options = {}) {
        const {
            mode = 'w',
            tmpPrefix = '.tmp-',
            lockPrefix = '.lock-',
            lockTimeout = 10000,
            maxRetries = 3,
            waitBetweenRetries = 1000
        } = options;

        const operationId = `${filepath}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const tmpPath = `${filepath}${tmpPrefix}${operationId}`;
        const lockPath = `${filepath}${lockPrefix}${operationId}`;

        if (this.processor?.debug) {
            this.processor.debugLog('Starting atomic write', {
                filepath,
                operationId,
                mode,
                tmpPath,
                lockPath
            });
        }

        // Track this operation
        this.activeWriteOperations.set(operationId, {
            filepath,
            tmpPath,
            lockPath,
            startTime: Date.now()
        });

        let lockAcquired = false;
        try {
            // Try to acquire lock with retry
            for (let i = 0; i < maxRetries; i++) {
                try {
                    await fs.promises.writeFile(lockPath, process.pid.toString(), { 
                        flag: 'wx'  // Fail if exists
                    });
                    lockAcquired = true;
                    break;
                } catch (err) {
                    if (err.code === 'EEXIST') {
                        // Check if lock is stale
                        try {
                            const lockStat = await fs.promises.stat(lockPath);
                            if (Date.now() - lockStat.mtimeMs > lockTimeout) {
                                // Try to clean up stale lock
                                await fs.promises.unlink(lockPath).catch(() => {});
                                continue;
                            }
                        } catch (statErr) {
                            if (statErr.code === 'ENOENT') {
                                // Lock disappeared, try again
                                continue;
                            }
                            throw statErr;
                        }

                        if (i < maxRetries - 1) {
                            await new Promise(resolve => 
                                setTimeout(resolve, waitBetweenRetries)
                            );
                            continue;
                        }
                        throw new Error('Could not acquire lock after retries');
                    }
                    throw err;
                }
            }

            if (!lockAcquired) {
                throw new Error('Failed to acquire lock');
            }

            // Write content to temp file
            await fs.promises.writeFile(tmpPath, content, { 
                flag: mode === 'a' ? 'a' : 'w',
                encoding: 'utf8'
            });

            // Verify the write
            const written = await fs.promises.readFile(tmpPath, 'utf8');
            if (written !== content) {
                throw new Error('Content verification failed');
            }

            // Atomic rename
            await fs.promises.rename(tmpPath, filepath);

            if (this.processor?.debug) {
                this.processor.debugLog('Atomic write successful', { 
                    filepath,
                    operationId 
                });
            }

        } catch (err) {
            if (this.processor?.debug) {
                this.processor.debugLog('Atomic write failed', {
                    filepath,
                    operationId,
                    error: err.message,
                    stack: err.stack
                });
            }
            throw err;
        } finally {
            // Cleanup
            try {
                if (lockAcquired) {
                    await fs.promises.unlink(lockPath).catch(() => {});
                }
                await fs.promises.unlink(tmpPath).catch(() => {});
            } catch (cleanupErr) {
                logger.warn(`Cleanup error for ${operationId}: ${cleanupErr.message}`);
            }

            // Remove from tracking
            this.activeWriteOperations.delete(operationId);
        }
    }

    // Helper to clean up any leftover temporary files
    async cleanup() {
        try {
            // Clean up any remaining operations
            for (const [id, op] of this.activeWriteOperations) {
                try {
                    await fs.promises.unlink(op.lockPath).catch(() => {});
                    await fs.promises.unlink(op.tmpPath).catch(() => {});
                } catch (err) {
                    logger.warn(`Failed to cleanup operation ${id}: ${err.message}`);
                }
            }
            
            this.activeWriteOperations.clear();

        } catch (err) {
            logger.error(`Error in AtomicFileWriter cleanup: ${err.message}`);
            throw err;
        }
    }
}

class ResourceManager {
    constructor() {
        this.resources = new Set();
    }
    
    register(resource) {
        this.resources.add(resource);
    }
    
    async cleanup() {
        const errors = [];
        for (const resource of this.resources) {
            try {
                if (typeof resource.cleanup === 'function') {
                    await resource.cleanup();
                } else if (typeof resource.close === 'function') {
                    await resource.close();
                }
            } catch (err) {
                const error = {
                    message: err.message,
                    stack: err.stack,
                    resource: resource.constructor.name
                };
                logger.error(`Resource cleanup error: ${JSON.stringify(error, null, 2)}`);
                errors.push(err);
            }
        }
        if (errors.length > 0) {
            throw new AggregateError(errors, `Multiple cleanup errors occurred (${errors.length} errors)`);
        }
    }
}

class FileManager {
    constructor(processor = null) {
        this.processor = processor;
        this.writer = new AtomicFileWriter(processor);
        this.streams = new Map();  // Important for stream management
        this.existingPacks = new Map();
        this.userCache = new Map();
        this.isFirstEntry = true;
        this.userManager = new UserManager(processor);
    }

    async verifyFiles() {
        const files = ['users.json', 'users.yaml', 'starter_packs.json', 'starter_packs.yaml', 'starter_pack_urls.txt'];
        for (const file of files) {
            try {
                await fs.promises.access(file, fs.constants.F_OK);
                const stats = await fs.promises.stat(file);
                this.debugLog(`File verification`, {
                    file,
                    exists: true,
                    size: stats.size,
                    lastModified: stats.mtime
                });
            } catch (err) {
                this.debugLog(`File missing`, {
                    file,
                    error: err.message
                });
            }
        }
    }

    /**
    * Attempts to move a file with retries on failure.
    * @param {string} oldPath - The current file path.
    * @param {string} newPath - The target file path.
    * @param {number} retries - Number of retry attempts.
    * @param {number} delay - Delay between retries in milliseconds.
    */
    async renameWithRetries(oldPath, newPath, retries = 5, delay = 1000) {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                await fsExtra.move(oldPath, newPath, { overwrite: true });
                return;
            } catch (err) {
                if (attempt === retries) {
                    throw err;
                }
                logger.warn(`Move attempt ${attempt} failed: ${err.message}. Retrying in ${delay}ms...`);
                await new Promise(res => setTimeout(res, delay));
            }
        }
    }

    /**
     * Converts starter_packs.json to NDJSON format if it's not already.
     */
    async convertToNDJSONIfNeeded() {
        const filePath = 'starter_packs.json';
        const backupPath = `starter_packs.json.backup.${Date.now()}`;
        const tempNDJSONPath = `starter_packs.ndjson.tmp`;
        const uniqueId = Date.now() + '-' + Math.random().toString(36).substring(2);
        const backupTempNDJSONPath = `starter_packs.ndjson.backup.${uniqueId}.bak`;

        // Check if starter_packs.json exists
        const fileExists = await fs.promises.access(filePath, fs.constants.F_OK)
            .then(() => true)
            .catch(() => false);

        if (!fileExists) {
            logger.info(`${filePath} does not exist. No conversion needed.`);
            return;
        }

        // Create a backup before any modifications
        try {
            await fs.promises.copyFile(filePath, backupPath);
            logger.info(`Backup created at ${backupPath}`);
        } catch (err) {
            logger.error(`Failed to create backup for ${filePath}: ${err.message}`);
            throw err;
        }

        // Determine if the file is already in NDJSON format
        const isNDJSON = await this.checkIfNDJSON(filePath);
        if (isNDJSON) {
            logger.info(`${filePath} is already in NDJSON format.`);
            return;
        }

        logger.info(`Converting ${filePath} to NDJSON format.`);

        // Initialize read and write streams
        const readStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
        const writeStream = fs.createWriteStream(tempNDJSONPath, { encoding: 'utf-8' });

        const rl = readline.createInterface({
            input: readStream,
            crlfDelay: Infinity
        });

        let buffer = '';
        let packsArray = [];
        let parsedObjects = 0;
        let skippedObjects = 0;

        // Read the entire file into a buffer
        for await (const line of rl) {
            buffer += line + '\n';
        }
        logger.info(`Finished reading the original file into buffer.`);

        // Close read streams
        readStream.close();
        rl.close();
        logger.info(`Closed read streams.`);

        // Attempt to parse as JSON array
        try {
            packsArray = JSON.parse(buffer);
            if (!Array.isArray(packsArray)) {
                throw new Error('JSON is not an array.');
            }
            logger.info(`Parsed ${packsArray.length} JSON objects from the array.`);
        } catch (err) {
            logger.warn(`Failed to parse ${filePath} as JSON array: ${err.message}`);
            // Attempt to extract individual JSON objects from the buffer
            packsArray = this.extractJSONObjects(buffer);
            if (packsArray.length === 0) {
                logger.error(`No valid JSON objects found in ${filePath}.`);
                throw err;
            }
            logger.info(`Extracted ${packsArray.length} JSON objects from the buffer.`);
        }

        // Write each pack as a separate NDJSON line
        for (const pack of packsArray) {
            if (typeof pack === 'object' && pack !== null) {
                try {
                    const jsonLine = JSON.stringify(pack);
                    writeStream.write(jsonLine + '\n');
                    parsedObjects += 1;
                } catch (err) {
                    logger.warn(`Failed to stringify a pack: ${err.message}`);
                    skippedObjects += 1;
                }
            } else {
                skippedObjects += 1;
            }
        }

        logger.info(`Finished writing to temporary NDJSON file.`);

        // Close the write stream and wait for it to finish
        await new Promise((resolve, reject) => {
            writeStream.end();
            writeStream.on('finish', resolve);
            writeStream.on('error', reject);
        });

        logger.info(`Write stream to temporary NDJSON file closed.`);

        // Windows-specific safe file replacement
        if (process.platform === 'win32') {
            try {
                // Create backups of existing files
                await fs.promises.copyFile(filePath, backupPath).catch(() => {});
                await fs.promises.copyFile(tempNDJSONPath, backupTempNDJSONPath).catch(() => {});

                // Add delay after backup
                await new Promise(resolve => setTimeout(resolve, 100));

                // Copy temp NDJSON to original JSON path
                await fs.promises.copyFile(tempNDJSONPath, filePath);

                // Add delay after copy
                await new Promise(resolve => setTimeout(resolve, 100));

                // Clean up temporary and backup files
                await fs.promises.unlink(tempNDJSONPath).catch(() => {});
                await fs.promises.unlink(backupTempNDJSONPath).catch(() => {});
                await fs.promises.unlink(backupPath).catch(() => {});

                logger.info(`Successfully converted ${filePath} to NDJSON format. Parsed objects: ${parsedObjects}, Skipped objects: ${skippedObjects}`);
            } catch (err) {
                logger.error(`Failed to replace original file with NDJSON: ${err.message}`);
                // Attempt to restore from backup
                try {
                    await fs.promises.copyFile(backupPath, filePath);
                    logger.info(`Restored original file from backup.`);
                } catch (restoreErr) {
                    logger.error(`Failed to restore original file from backup: ${restoreErr.message}`);
                }
                throw err;
            }
        } else {
            // Non-Windows platforms can use fs-extra's move with overwrite
            try {
                await fsExtra.move(tempNDJSONPath, filePath, { overwrite: true });
                logger.info(`Successfully converted ${filePath} to NDJSON format. Parsed objects: ${parsedObjects}, Skipped objects: ${skippedObjects}`);
            } catch (err) {
                logger.error(`Failed to replace original file with NDJSON: ${err.message}`);
                // Attempt to restore from backup
                try {
                    await fsExtra.move(backupPath, filePath, { overwrite: true });
                    logger.info(`Restored original file from backup.`);
                } catch (restoreErr) {
                    logger.error(`Failed to restore original file from backup: ${restoreErr.message}`);
                }
                throw err;
            }
        }
    }

    /**
     * Checks if a file is in NDJSON format by attempting to parse each line as JSON.
     * @param {string} filePath 
     * @returns {Promise<boolean>}
     */
    async checkIfNDJSON(filePath) {
        return new Promise((resolve) => {
            const readStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
            const rl = readline.createInterface({
                input: readStream,
                crlfDelay: Infinity
            });

            let isNDJSON = true;

            rl.on('line', (line) => {
                if (line.trim() === '') return; // Skip empty lines
                try {
                    JSON.parse(line);
                } catch (err) {
                    isNDJSON = false;
                    rl.close();
                }
            });

            rl.on('close', () => {
                resolve(isNDJSON);
            });

            rl.on('error', () => {
                resolve(false);
            });
        });
    }

    /**
     * Extracts JSON objects from a string buffer.
     * This is a simplistic implementation and may need enhancements for complex JSON.
     * @param {string} buffer 
     * @returns {Array<Object>}
     */
    extractJSONObjects(buffer) {
        const objects = [];
        let depth = 0;
        let inString = false;
        let escape = false;
        let currentObject = '';

        for (let i = 0; i < buffer.length; i++) {
            const char = buffer[i];

            if (escape) {
                currentObject += char;
                escape = false;
                continue;
            }

            if (char === '\\') {
                currentObject += char;
                escape = true;
                continue;
            }

            if (char === '"' && !escape) {
                inString = !inString;
                currentObject += char;
                continue;
            }

            if (!inString) {
                if (char === '{') {
                    depth += 1;
                } else if (char === '}') {
                    depth -= 1;
                }
            }

            currentObject += char;

            if (depth === 0 && currentObject.trim()) {
                try {
                    const obj = JSON.parse(currentObject);
                    objects.push(obj);
                } catch (err) {
                    logger.warn(`Failed to parse extracted JSON object: ${err.message}`);
                }
                currentObject = '';
            }
        }

        return objects;
    }

    async writePack(pack) {
        try {
            await this.handleRemovedProfiles(pack);
            this.existingPacks.set(pack.rkey, pack);

            // Create backup before writing
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            await this.createBackup('starter_packs.json', timestamp);
            await this.createBackup('starter_packs.yaml', timestamp);

            const jsonContent = JSON.stringify(pack, null, 2) + '\n';
            await this.writeToStream('json', jsonContent);
            await this.writeToStream('yaml', '---\n' + yaml.dump(pack));

            if (this.processor?.debug) {
                this.processor.debugLog('Pack written successfully', { rkey: pack.rkey });
            }
        } catch (err) {
            logger.error(`Error writing pack ${pack.rkey}: ${err.stack || err.message}`);
            await this.cleanup();
            throw err;
        }
    }

    async safeWrite(stream, content) {
        if (!stream || stream.destroyed) {
            throw new Error('Stream is not available');
        }

        return new Promise((resolve, reject) => {
            const writeResult = stream.write(content, err => {
                if (err) reject(err);
                else resolve();
            });

            // Handle backpressure
            if (!writeResult) {
                stream.once('drain', resolve);
            }
        });
    }

    async handleRemovedProfiles(pack) {
        const existingPack = this.existingPacks.get(pack.rkey);
        if (!existingPack) return;

        const currentDids = new Set(pack.users.map(u => u.did));
        const existingDids = new Set(existingPack.users.map(u => u.did));
        
        const removedDids = [...existingDids].filter(did => !currentDids.has(did));
        if (removedDids.length > 0) {
            logger.info(`${removedDids.length} profiles were removed from pack ${pack.rkey}`);
            
            if (this.processor && !this.processor.noMongoDB) {
                try {
                    await this.processor.db.collection('users').updateMany(
                        { did: { $in: removedDids } },
                        { $pull: { pack_ids: pack.rkey } }
                    );
                    
                    const cleanupResults = await this.processor.db.collection('users').deleteMany({
                        did: { $in: removedDids },
                        pack_ids: { $size: 0 }
                    });
                    
                    if (cleanupResults.deletedCount > 0) {
                        logger.info(`Removed ${cleanupResults.deletedCount} users no longer in any packs`);
                    }
                } catch (err) {
                    logger.error(`Error updating removed users in MongoDB: ${err.message}`);
                }
            }
        }
        
        return {
            removed: removedDids,
            added: [...currentDids].filter(did => !existingDids.has(did))
        };
    }

    // Validate file paths
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

    // Initialize streams with proper error handling
    async initStream(name, path, options = {}) {
        if (this.processor?.debug) {
            this.processor.debugLog(`Initializing stream: ${name}`, { path, options });
        }

        const stream = fs.createWriteStream(path, options);
        
        stream.on('error', (err) => {
            logger.error(`Stream error for ${name}: ${err.message}`);
            if (this.processor?.debug) {
                this.processor.debugLog(`Stream error: ${name}`, {
                    error: err.message,
                    stack: err.stack
                });
            }
        });

        await new Promise((resolve, reject) => {
            stream.once('open', resolve);
            stream.once('error', reject);
        });

        this.streams.set(name, stream);
        return stream;
    }

    // Initialize the file manager
    async init() {
        try {
            if (!await this.validatePaths(['starter_packs.json', 'starter_packs.yaml', 'users.json', 'users.yaml'])) {
                throw new Error('Required file paths are not accessible');
            }
            
            // Convert to NDJSON if needed
            await this.convertToNDJSONIfNeeded();

            if (this.processor?.debug) {
                this.processor.debugLog('Loading existing starterpacks');
            }
            await this.loadExistingPacks();

            // Initialize streams with append mode
            await this.initStream('json', 'starter_packs.json', { flags: 'a' });
            await this.initStream('yaml', 'starter_packs.yaml', { flags: 'a' });
            await this.initStream('users-json', 'users.json', { flags: 'a' });
            await this.initStream('users-yaml', 'users.yaml', { flags: 'a' });

            if (this.processor?.debug) {
                this.processor.debugLog('File manager initialized', {
                    existingPacks: this.existingPacks.size,
                    streams: Array.from(this.streams.keys())
                });
            }

        } catch (err) {
            logger.error(`Error initializing file manager: ${err.stack || err.message}`);
            throw err;
        }
    }

    // Load existing packs from NDJSON file
    async loadExistingPacks() {
        const filePath = 'starter_packs.json';
        try {
            const exists = await fs.promises.access(filePath)
                .then(() => true)
                .catch(() => false);
    
            if (exists) {
                if (this.processor?.debug) {
                    this.processor.debugLog(`Opening existing ${filePath}`);
                }

                const readStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
                const rl = readline.createInterface({
                    input: readStream,
                    crlfDelay: Infinity
                });

                for await (const line of rl) {
                    const trimmedLine = line.trim();
                    if (trimmedLine === '') continue; // Skip empty lines
                    try {
                        const pack = JSON.parse(trimmedLine);
                        this.existingPacks.set(pack.rkey, pack);
                    } catch (err) {
                        logger.warn(`Failed to parse a line in ${filePath}: ${err.message}`);
                        // Continue parsing the next lines
                    }
                }

                readStream.close();
                rl.close();
            } else {
                if (this.processor?.debug) {
                    this.processor.debugLog(`${filePath} does not exist. Starting with empty packs.`);
                }
                this.existingPacks = new Map();
            }
        } catch (err) {
            logger.error(`Error loading existing packs: ${err.stack || err.message}`);
            throw err;
        }
    }

    // Write to stream with error handling
    async writeToStream(type, content) {
        const stream = this.streams.get(type);
        if (!stream) throw new Error(`No stream found for type: ${type}`);

        return new Promise((resolve, reject) => {
            const writeResult = stream.write(content, err => {
                if (err) reject(err);
                else resolve();
            });

            // Handle backpressure
            if (!writeResult) {
                stream.once('drain', resolve);
            }
        });
    }

    // Handle atomic writes
    async atomicWrite(path, content, options = {}) {
        return await this.writer.writeAtomic(path, content, options);
    }

    // Write user data
    async writeUser(user, isUpdate = false) { 
        try {
            await this.userManager.writeUser(user, isUpdate);
        } catch (err) {
            logger.error(`Failed to write user ${user.did}: ${err.message}`);
            if (this.processor?.debug) {
                this.processor.debugLog('Error writing user', {
                    did: user.did,
                    error: err.message,
                    stack: err.stack
                });
            }
            throw err;
        }
    }

    async createBackup(filename, timestamp) {
        try {
            const exists = await fs.promises.access(filename)
                .then(() => true)
                .catch(() => false);
            
            if (exists) {
                await fs.promises.copyFile(filename, `${filename}.${timestamp}.backup`);
            }
        } catch (err) {
            logger.warn(`Failed to create backup for ${filename}: ${err.message}`);
        }
    }

    // Clean up temporary files
    async cleanupTempFiles() {
        try {
            const files = await fs.promises.readdir('./');
            for (const file of files) {
                if (file.endsWith('.tmp')) {
                    try {
                        await fs.promises.unlink(path.join('./', file));
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

    // Get existing profile
    getExistingProfile(did) {
        for (const pack of this.existingPacks.values()) {
            if (pack.users && Array.isArray(pack.users)) {
                const user = pack.users.find(u => u.did === did);
                if (user) return user;
            }
        }
        return null;
    }

    // Get user from cache
    getUser(did) {
        return this.userCache.get(did);
    }

    // Get existing pack
    getExistingPack(rkey) {
        return this.existingPacks.get(rkey);
    }

    // Enhanced cleanup with proper stream closing
    async cleanup() {
        if (this.processor?.debug) {
            this.processor.debugLog('Starting file cleanup', {
                streams: Array.from(this.streams.keys())
            });
        }
    
        await this.cleanupTempFiles();
        await this.userManager.shutdown();
    
        const closePromises = Array.from(this.streams.entries()).map(
            async ([name, stream]) => {
                try {
                    if (name === 'json') {
                        // For NDJSON, no need to close array brackets
                        // If you switch back to JSON array, handle accordingly
                    }
                    await new Promise((resolve, reject) => {
                        stream.end(err => {
                            if (err) reject(err);
                            else resolve();
                        });
                    });
    
                    if (this.processor?.debug) {
                        this.processor.debugLog(`Stream closed: ${name}`);
                    }
    
                } catch (err) {
                    logger.error(`Error closing stream ${name}: ${err.message}`);
                    throw err;
                }
            }
        );
    
        await Promise.all(closePromises);
        this.streams.clear();
        this.userCache.clear();
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
    const BATCH_SIZE = 1000;
    
    async function processBatch(db, batch, session) {
        if (!batch.length) return;
        
        await db.collection('users').bulkWrite(batch, {
            ordered: false,
            session,
            writeConcern: { w: 'majority' }
        });
    }

    let mongoClient = null;
    let session = null;
    
    try {
        // Create backup
        const backupTimestamp = new Date().toISOString().replace(/[:.]/g, '-');
        await fs.promises.copyFile('starter_packs.json', `starter_packs.${backupTimestamp}.backup.json`);
        
        // Read and validate input
        const content = await fs.promises.readFile('starter_packs.json', 'utf-8');
        const packs = JSON.parse(content);
        if (!Array.isArray(packs) || !packs.every(p => p.rkey && Array.isArray(p.users))) {
            throw new Error('Invalid pack data structure');
        }
        
        // Initialize MongoDB connection
        mongoClient = new MongoClient(process.env.MONGODB_URI);
        await mongoClient.connect();
        const db = mongoClient.db('starterpacks');
        
        // Start session for transaction
        session = mongoClient.startSession();
        
        await session.withTransaction(async () => {
            // Handle pack removals
            const existingPacks = await db.collection('users').distinct('pack_id', {}, { session });
            const newPacks = new Set(packs.map(p => p.rkey));
            const removedPacks = existingPacks.filter(id => !newPacks.has(id));
            
            if (removedPacks.length > 0) {
                await db.collection('users').deleteMany(
                    { pack_id: { $in: removedPacks } },
                    { session }
                );
            }

            // Process each pack
            for (const pack of packs) {
                // Get current users for this pack
                const currentUsers = await db.collection('users')
                    .find({ pack_id: pack.rkey })
                    .project({ did: 1 })
                    .session(session)
                    .toArray();
                
                // Handle user removals
                const currentDids = new Set(currentUsers.map(u => u.did));
                const packDids = new Set(pack.users.map(u => u.did));
                const removedUsers = [...currentDids].filter(did => !packDids.has(did));
                
                if (removedUsers.length > 0) {
                    await db.collection('users').deleteMany(
                        {
                            did: { $in: removedUsers },
                            pack_id: pack.rkey
                        },
                        { session }
                    );
                }
                
                // Process users in batches
                for (let i = 0; i < pack.users.length; i += BATCH_SIZE) {
                    const batchUsers = pack.users.slice(i, i + BATCH_SIZE);
                    const batchOps = batchUsers.map(user => ({
                        updateOne: {
                            filter: { did: user.did },
                            update: {
                                $set: {
                                    did: user.did,
                                    handle: user.handle,
                                    display_name: user.display_name || '',
                                    last_updated: new Date(),
                                    profile_check_needed: false,
                                    pack_id: pack.rkey
                                }
                            },
                            upsert: true
                        }
                    }));

                    await processBatch(db, batchOps, session);
                    
                    // Memory management and progress logging
                    if (global.gc) {
                        global.gc();
                    }
                    
                    // Allow event loop to process other tasks
                    await new Promise(resolve => setImmediate(resolve));
                    
                    logger.info(`Processed ${i + batchUsers.length}/${pack.users.length} users for pack ${pack.rkey}`);
                }
            }
        }, {
            readConcern: { level: 'majority' },
            writeConcern: { w: 'majority' },
            maxTimeMS: 300000  // 5 minutes timeout
        });

    } catch (err) {
        logger.error(`Error updating MongoDB from files: ${err.stack || err.message}`);
        throw err;
    } finally {
        if (session) {
            try {
                await session.endSession();
            } catch (err) {
                logger.error(`Error ending MongoDB session: ${err.message}`);
            }
        }
        if (mongoClient) {
            try {
                await mongoClient.close();
            } catch (err) {
                logger.error(`Error closing MongoDB connection: ${err.message}`);
            }
        }
    }
}

class CheckpointManager {
    constructor(filename = 'checkpoints.json', processor = null) {
        this.filename = filename;
        this.processor = processor;
        this.writer = new AtomicFileWriter(processor);
        this.checkpoints = null;
        this.dirty = false;
        this.lastSave = 0;
        this.saveInterval = 5000; // 5 seconds

        // Add missing tracking sets
        this.missingProfiles = new Set();
        this.missingPacks = new Set();
        this.completedPacks = new Set();
        this.processedUsers = new Set();
        
        // Setup periodic save
        this.saveIntervalId = setInterval(() => {
            if (this.dirty && Date.now() - this.lastSave > this.saveInterval) {
                this.saveCheckpoints().catch(err => {
                    logger.error(`Periodic checkpoint save failed: ${err.message}`);
                });
            }
        }, this.saveInterval);
    }

    getInitialState() {
        return {
            version: "1.0",
            lastProcessedIndex: -1,
            lastProcessedDate: null,
            dailyStats: {},
            errors: [],
            completedPacks: new Set(),
            rateLimitHits: [],
            missingPacks: new Set(),
            missingProfiles: new Set(),
            processedUsers: new Set(),
            packStats: new Map(),
            lastMemoryUsage: null,
            startTime: Date.now()
        };
    }

    async init() {
        try {
            // Load existing checkpoints
            this.checkpoints = await this.loadCheckpoints();

            // Reconcile tracking sets with loaded data
            this.missingProfiles = new Set(this.checkpoints.missingProfiles);
            this.missingPacks = new Set(this.checkpoints.missingPacks);
            this.completedPacks = new Set(this.checkpoints.completedPacks);
            this.processedUsers = new Set(this.checkpoints.processedUsers);
            
            // Initialize daily stats if needed
            const today = new Date().toISOString().split('T')[0];
            if (!this.checkpoints.dailyStats[today]) {
                this.checkpoints.dailyStats[today] = {
                    processed: 0,
                    successful: 0,
                    errors: 0,
                    rateLimitHits: 0,
                    skipped: 0,
                    usersCounted: 0,
                    usersProcessed: 0,
                    discoveredPacks: 0
                };
            }

            // Convert packStats to Map if it was serialized
            if (!(this.checkpoints.packStats instanceof Map)) {
                this.checkpoints.packStats = new Map(Object.entries(this.checkpoints.packStats || {}));
            }

            if (this.processor?.debug) {
                this.processor.debugLog('Checkpoint manager initialized', {
                    completedPacks: this.completedPacks.size,
                    missingPacks: this.missingPacks.size,
                    missingProfiles: this.missingProfiles.size,
                    processedUsers: this.processedUsers.size
                });
            }

        } catch (err) {
            logger.error(`Error initializing checkpoint manager: ${err.stack || err.message}`);
            throw err;
        }
    }

    async loadCheckpoints() {
        if (this.processor?.debug) {
            this.processor.debugLog('Loading checkpoints', {
                filename: this.filename
            });
        }
    
        try {
            const data = await fs.promises.readFile(this.filename, 'utf-8');
            const loaded = JSON.parse(data);
    
            // Convert serialized collections back to proper types with safety checks
            const state = {
                ...this.getInitialState(),
                ...loaded,
                // Ensure arrays exist before creating Sets
                completedPacks: new Set(Array.isArray(loaded.completedPacks) ? loaded.completedPacks : []),
                missingPacks: new Set(Array.isArray(loaded.missingPacks) ? loaded.missingPacks : []),
                missingProfiles: new Set(Array.isArray(loaded.missingProfiles) ? loaded.missingProfiles : []),
                processedUsers: new Set(Array.isArray(loaded.processedUsers) ? loaded.processedUsers : []),
                // Ensure object exists before creating Map
                packStats: new Map(loaded.packStats ? Object.entries(loaded.packStats || {}) : [])
            };
    
            // Validate and repair any corrupted data
            if (!this.validateCheckpoints(state)) {
                logger.warn('Corrupted checkpoint data detected, creating backup and starting fresh');
                await this.backupCorruptedFile();
                return this.getInitialState();
            }
    
            return state;
    
        } catch (err) {
            if (err.code === 'ENOENT') {
                if (this.processor?.debug) {
                    this.processor.debugLog('No checkpoint file found, starting fresh');
                }
                return this.getInitialState();
            }
    
            if (err instanceof SyntaxError) {
                logger.warn('Checkpoint file corrupted, backing up and starting fresh');
                await this.backupCorruptedFile();
                return this.getInitialState();
            }
    
            throw err;
        }
    }
    
    validateCheckpoints(state) {
        // Basic structure validation with type checking
        const requiredFields = [
            'version',
            'lastProcessedIndex',
            'lastProcessedDate',
            'dailyStats',
            'errors',
            'completedPacks',
            'rateLimitHits',
            'missingPacks',
            'missingProfiles',
            'processedUsers',
            'packStats'
        ];
    
        // Check all required fields exist
        for (const field of requiredFields) {
            if (!(field in state)) {
                logger.error(`Missing required field in checkpoints: ${field}`);
                return false;
            }
        }
    
        // Validate data types
        if (!(state.completedPacks instanceof Set) ||
            !(state.missingPacks instanceof Set) ||
            !(state.missingProfiles instanceof Set) ||
            !(state.processedUsers instanceof Set) ||
            !(state.packStats instanceof Map)) {
            return false;
        }
    
        // Validate dailyStats is an object
        if (typeof state.dailyStats !== 'object' || state.dailyStats === null) {
            return false;
        }
    
        // Validate arrays
        if (!Array.isArray(state.errors) || !Array.isArray(state.rateLimitHits)) {
            return false;
        }
    
        return true;
    }

    async backupCorruptedFile() {
        try {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const backupPath = `${this.filename}.${timestamp}.corrupted`;
            await fs.promises.rename(this.filename, backupPath)
                .catch(() => {}); // Ignore if original doesn't exist
            logger.info(`Created backup of corrupted checkpoint file: ${backupPath}`);
        } catch (err) {
            logger.error(`Error backing up corrupted checkpoint file: ${err.message}`);
        }
    }

    // Existing methods remain the same...
    async addMissingProfile(did, reason) {
        this.checkpoints.missingProfiles.add(did);
        this.dirty = true;
        
        this.checkpoints.errors.push({
            timestamp: new Date().toISOString(),
            type: 'missing_profile',
            did,
            reason
        });

        await this.saveIfNeeded();
    }

    isMissingProfile(did) {
        return this.checkpoints.missingProfiles.has(did);
    }

    async addMissingPack(rkey, reason) {
        this.checkpoints.missingPacks.add(rkey);
        this.dirty = true;
        
        this.checkpoints.errors.push({
            timestamp: new Date().toISOString(),
            type: 'missing_pack',
            rkey,
            reason
        });

        await this.saveIfNeeded();
    }

    isMissingPack(rkey) {
        return this.checkpoints.missingPacks.has(rkey);
    }

    getDailyStats(date = new Date().toISOString().split('T')[0]) {
        return this.checkpoints?.dailyStats[date];
    }

    getLastProcessedIndex() {
        return this.checkpoints?.lastProcessedIndex || -1;
    }

    getProgressStats() {
        const now = Date.now();
        const elapsed = (now - this.checkpoints.startTime) / 1000;
        const processed = this.completedPacks.size;
        const total = processed + this.missingPacks.size;
        
        return {
            processed,
            total,
            percentage: total > 0 ? (processed / total * 100).toFixed(2) : 0,
            elapsedSeconds: elapsed,
            rate: elapsed > 0 ? (processed / (elapsed / 60)).toFixed(2) : 0, // packs per minute
            errors: this.checkpoints.errors.length,
            rateLimitHits: this.checkpoints.rateLimitHits.length
        };
    }

    async updateProgress(index, rkey, status = 'success', error = null) {
        if (!this.checkpoints) await this.init();

        const today = new Date().toISOString().split('T')[0];
        
        // Initialize daily stats if needed
        if (!this.checkpoints.dailyStats[today]) {
            this.checkpoints.dailyStats[today] = {
                processed: 0,
                successful: 0,
                errors: 0,
                rateLimitHits: 0,
                skipped: 0,
                usersCounted: 0,
                usersProcessed: 0,
                discoveredPacks: 0
            };
        }

        // Update pack-specific stats
        if (!this.checkpoints.packStats.has(rkey)) {
            this.checkpoints.packStats.set(rkey, {
                firstSeen: new Date().toISOString(),
                attempts: 0,
                lastAttempt: null,
                status: [],
                errors: []
            });
        }

        const packStats = this.checkpoints.packStats.get(rkey);
        packStats.attempts++;
        packStats.lastAttempt = new Date().toISOString();
        packStats.status.push({ timestamp: new Date().toISOString(), status });

        // Update stats based on status
        const dailyStats = this.checkpoints.dailyStats[today];
        dailyStats.processed++;

        switch (status) {
            case 'success':
                dailyStats.successful++;
                this.checkpoints.completedPacks.add(rkey);
                break;
            case 'error':
                dailyStats.errors++;
                if (error) {
                    const errorInfo = {
                        timestamp: new Date().toISOString(),
                        index,
                        rkey,
                        error: error.message || error,
                        stack: error.stack
                    };
                    this.checkpoints.errors.push(errorInfo);
                    packStats.errors.push(errorInfo);
                }
                break;
            case 'rateLimit':
                dailyStats.rateLimitHits++;
                this.checkpoints.rateLimitHits.push({
                    timestamp: new Date().toISOString(),
                    index,
                    rkey
                });
                break;
            case 'skipped':
                dailyStats.skipped++;
                this.checkpoints.completedPacks.add(rkey);
                break;
        }

        if (index !== null) {
            this.checkpoints.lastProcessedIndex = index;
        }
        this.checkpoints.lastProcessedDate = new Date().toISOString();
        this.checkpoints.lastMemoryUsage = process.memoryUsage();

        this.dirty = true;
        await this.saveIfNeeded();

        // Log progress if debug enabled
        if (this.processor?.debug) {
            const progress = this.getProgressStats();
            this.processor.debugLog('Progress update', {
                index,
                rkey,
                status,
                progress,
                dailyStats: this.getDailyStats()
            });
        }
    }

    shouldProcessPack(rkey, isNewOrUpdate = false) {
        // Always process new packs or those marked for update
        if (isNewOrUpdate) return true;
        
        // Check if pack was already processed successfully
        return !this.checkpoints.completedPacks.has(rkey);
    }

    async saveIfNeeded() {
        if (this.dirty && Date.now() - this.lastSave > this.saveInterval) {
            await this.saveCheckpoints();
        }
    }

    async saveCheckpoints(force = false) {
        if (!this.dirty && !force) return;

        if (this.processor?.debug) {
            this.processor.debugLog('Saving checkpoints', {
                force,
                lastSave: new Date(this.lastSave).toISOString()
            });
        }

        try {
            // Prepare serializable state
            const state = {
                ...this.checkpoints,
                completedPacks: Array.from(this.checkpoints.completedPacks),
                missingPacks: Array.from(this.checkpoints.missingPacks),
                missingProfiles: Array.from(this.checkpoints.missingProfiles),
                processedUsers: Array.from(this.checkpoints.processedUsers),
                packStats: Object.fromEntries(this.checkpoints.packStats),
                lastSave: Date.now()
            };

            await this.writer.writeAtomic(
                this.filename,
                JSON.stringify(state, null, 2)
            );

            this.dirty = false;
            this.lastSave = Date.now();

        } catch (err) {
            logger.error(`Failed to save checkpoints: ${err.message}`);
            if (this.processor?.debug) {
                this.processor.debugLog('Checkpoint save failed', {
                    error: err.message,
                    stack: err.stack
                });
            }
            throw err;
        }
    }

    async cleanup() {
        try {
            // Save any pending changes
            if (this.dirty) {
                await this.saveCheckpoints(true);
            }

            // Clear interval
            if (this.saveIntervalId) {
                clearInterval(this.saveIntervalId);
            }

            // Clear data structures
            this.missingProfiles.clear();
            this.missingPacks.clear();
            this.completedPacks.clear();
            this.processedUsers.clear();

            if (this.processor?.debug) {
                this.processor.debugLog('Checkpoint manager cleaned up');
            }

        } catch (err) {
            logger.error(`Error during checkpoint cleanup: ${err.message}`);
            throw err;
        }
    }
}

class BlueSkyRateLimiter {
    constructor(processor = null) {
        this.processor = processor; // for debug, pass through from constructor

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

        this.rateLimitWindow = new Map();  // Track rate limits per endpoint
        this.maxRequestsPerEndpoint = 100;
        this.endpointWindow = 60000;  // 1 minute
    }

    async throttle(endpoint = 'default') {
        const now = Date.now();
        
        // Cleanup old requests across all endpoints
        this.requests = this.requests.filter(time => now - time < this.requestWindow);
        
        // Handle endpoint-specific tracking
        if (!this.rateLimitWindow) {
            this.rateLimitWindow = new Map();
        }
        const endpointRequests = this.rateLimitWindow.get(endpoint) || [];
        this.rateLimitWindow.set(endpoint, 
            endpointRequests.filter(t => now - t < this.endpointWindow || 60000));
    
        // Calculate effective limits with safety margin
        const effectiveMaxRequests = Math.floor(this.maxRequests * this.safetyFactor);
        const endpointMaxRequests = Math.min(
            this.maxRequestsPerEndpoint || 100,
            Math.floor(effectiveMaxRequests * 0.2)
        );
        
        let waitTime = 0;
        
        // Check global rate limits
        if (this.requests.length >= effectiveMaxRequests) {
            const oldestRequest = this.requests[0];
            const windowEndTime = oldestRequest + this.requestWindow;
            const baseWaitTime = windowEndTime - now;
            const backoffWaitTime = this.currentBackoff * 
                (this.requests.length / effectiveMaxRequests);
            
            waitTime = Math.max(baseWaitTime, backoffWaitTime);
        }
        
        // Check endpoint-specific limits
        const endpointCurrentRequests = this.rateLimitWindow.get(endpoint)?.length || 0;
        if (endpointCurrentRequests >= endpointMaxRequests) {
            const oldestEndpointRequest = this.rateLimitWindow.get(endpoint)[0];
            const endpointWaitTime = (oldestEndpointRequest + this.endpointWindow) - now;
            waitTime = Math.max(waitTime, endpointWaitTime);
        }
    
        if (waitTime > 0) {
            const currentUsage = {
                global: `${this.requests.length}/${effectiveMaxRequests}`,
                endpoint: `${endpointCurrentRequests}/${endpointMaxRequests}`,
                waitTime: `${Math.round(waitTime/1000)}s`
            };
            
            logger.info(
                `Rate limit approaching for ${endpoint}, waiting ${currentUsage.waitTime}` +
                ` (Global: ${currentUsage.global}, Endpoint: ${currentUsage.endpoint})`
            );
    
            try {
                await new Promise((resolve, reject) => {
                    const timeout = setTimeout(resolve, waitTime);
                    timeout.unref(); // Don't keep process alive for this timeout
                    
                    // Add safety timeout
                    const maxWaitTime = Math.min(waitTime * 1.5, 300000); // Max 5 minutes
                    const safetyTimeout = setTimeout(() => {
                        clearTimeout(timeout);
                        reject(new Error('Rate limit wait timeout exceeded'));
                    }, maxWaitTime);
                    safetyTimeout.unref();
                });
            } catch (err) {
                logger.error(`Rate limit wait error: ${err.message}`);
                // Still update backoff on timeout
                this.currentBackoff = Math.min(this.currentBackoff * 2, this.maxBackoff);
                throw err;
            }
    
            // Increase backoff for next time
            this.currentBackoff = Math.min(this.currentBackoff * 2, this.maxBackoff);
        } else {
            // Reset backoff if we're well below limits
            if (this.requests.length < effectiveMaxRequests * 0.5 &&
                endpointCurrentRequests < endpointMaxRequests * 0.5) {
                this.currentBackoff = this.initialBackoff;
            }
        }
        
        // Track both global and endpoint-specific requests
        this.requests.push(now);
        this.rateLimitWindow.set(endpoint, 
            [...(this.rateLimitWindow.get(endpoint) || []), now]);
    
        // Memory cleanup when too many endpoints
        if (this.rateLimitWindow.size > 1000) {
            const oldestEndpoints = [...this.rateLimitWindow.entries()]
                .sort(([,a], [,b]) => 
                    Math.max(...b) - Math.max(...a))
                .slice(500);
            for (const [key] of oldestEndpoints) {
                this.rateLimitWindow.delete(key);
            }
        }
    }

    async handleResponse(response) {
        if (this.processor?.debug) {  
            this.processor.debugLog('Rate limit status', {
                remaining: response?.headers?.['x-ratelimit-remaining'],
                reset: response?.headers?.['x-ratelimit-reset'],
                requestsInWindow: this.requests.length
            });
        }
        
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

// Then define the AdaptiveRateLimiter that extends it
class AdaptiveRateLimiter extends BlueSkyRateLimiter {
    constructor(processor = null) {
        super(processor);
        this.errorRates = new Map(); // Track error rates per endpoint
        this.successiveErrors = 0;
    }

    async handleResponse(response) {
        const endpoint = response?.config?.url || 'unknown';
        if (response?.status >= 400) {
            this.successiveErrors++;
            this.errorRates.set(endpoint, (this.errorRates.get(endpoint) || 0) + 1);
        } else {
            this.successiveErrors = 0;
        }

        // Adjust delay based on error patterns
        if (this.successiveErrors > 2) {
            this.dynamicDelay *= 1.5;
        } else if (this.successiveErrors === 0) {
            this.dynamicDelay = Math.max(1000, this.dynamicDelay * 0.8);
        }

        return super.handleResponse(response);
    }
}

class StarterPackProcessor {
    
    #tokenRefreshPromise = null; // initializing
    
    constructor() {
        const args = process.argv.slice(2);
        this.noMongoDB = args.includes('--nomongodb');
        this.updateMongoDB = args.includes('--updatemongodb');
        this.fromApi = args.includes('--fromapi');
        this.debug = args.includes('--debug');
        this.dbConfig = DB_CONFIG; // flexible mongodb setup
        this.memoryThreshold = 0.9; // 90% heap usage
        this.lastMemoryCheck = Date.now();
        this.memoryCheckInterval = 60000; // 1 minute
        this.apiTimeout = process.env.API_TIMEOUT || 30000;
        this.maxBatchSize = process.env.MAX_BATCH_SIZE || 1000;
        this.maxConcurrentOperations = process.env.MAX_CONCURRENT_OPERATIONS || 5;
        this.activeOperations = new Set();

        this.checkpointManager = new CheckpointManager('checkpoints.json', this);
        
        // Prevent conflicting modes
        if (this.fromApi && this.updateMongoDB) {
            throw new Error('Cannot use --fromapi with --updatemongodb');
        }

        if (!this.updateMongoDB) {
            this.agent = new BskyAgent({ service: 'https://bsky.social' });
            if (!this.noMongoDB) {
                this.mongoClient = new MongoClient(process.env.MONGODB_URI, this.dbConfig);
            }
            this.db = null;
            this.rateLimiter = new AdaptiveRateLimiter(this); // Use the adaptive rate limiter
            this.lastTokenRefresh = Date.now();
            this.tokenRefreshInterval = 45 * 60 * 1000;

            this.fileManager = new FileManager(this);
            
            this.isInitialized = false;
            this.profileCache = new LRUCache(1000); // Use LRU cache with 1000 item limit
            this.profileCacheExpiry = 24 * 60 * 60 * 1000;
            this.resourceManager = new ResourceManager(); // Add resource manager
        }
    }

    async findExistingUser(did) {
        // Already handled by getProfile's cache check
        return await this.getProfile(did, false);
    }

    // Enhanced pack tracking
    async trackNewStarterPack(uri, creator) {
        const rkey = this.extractRkeyFromURI(uri);
        
        if (this.debug) {
            this.debugLog('Checking potential new pack', {
                uri,
                rkey,
                creator: creator.handle,
                timestamp: new Date().toISOString()
            });
        }
        
        // Check existing pack in memory first
        const existingPack = this.fileManager.getExistingPack(rkey);
        if (existingPack) {
            if (this.debug) {
                this.debugLog('Pack already known in memory', { 
                    uri, 
                    rkey,
                    lastUpdated: existingPack.updated_at
                });
            }
            return false;
        }
    
        // Check URLs file
        try {
            const urlsContent = await fs.promises.readFile('starter_pack_urls.txt', 'utf-8');
            if (urlsContent.includes(rkey)) {
                if (this.debug) {
                    this.debugLog('Pack already in URL list', { uri, rkey });
                }
                return false;
            }
    
            // Add to URLs file using atomic write
            const line = `${creator.handle}|${rkey}\n`;
            await this.fileManager.atomicWrite(
                'starter_pack_urls.txt',
                line,
                { flag: 'a' }
            );
    
            if (this.debug) {
                this.debugLog('Added new pack to discovery list', {
                    uri,
                    rkey,
                    creator: creator.handle,
                    timestamp: new Date().toISOString()
                });
            }
    
            // Create minimal pack entry
            const minimalPack = {
                rkey,
                name: creator.name || 'Unknown',
                creator: creator.handle,
                creator_did: creator.did,
                description: creator.description || '',
                user_count: 0,
                created_at: new Date(),
                updated_at: new Date(),
                users: [],
                weekly_joins: 0,
                total_joins: 0
            };
    
            // Write to pack files
            try {
                await this.fileManager.writePack(minimalPack);
                
                if (this.debug) {
                    this.debugLog('Successfully wrote new pack data', {
                        rkey,
                        creator: creator.handle,
                        files: ['starter_packs.json', 'starter_packs.yaml']
                    });
                }
            } catch (writeErr) {
                this.debugLog('Failed to write pack data', {
                    rkey,
                    error: writeErr.message,
                    stack: writeErr.stack
                });
                
                // Try to remove from URLs file if pack write failed
                try {
                    const currentContent = await fs.promises.readFile('starter_pack_urls.txt', 'utf-8');
                    const newContent = currentContent.replace(line, '');
                    await this.fileManager.atomicWrite('starter_pack_urls.txt', newContent);
                } catch (cleanupErr) {
                    this.debugLog('Failed to cleanup URLs file after pack write failure', {
                        rkey,
                        error: cleanupErr.message
                    });
                }
                
                throw writeErr; // Propagate the original error
            }
    
            // Track discovery statistics
            this.stats = this.stats || {};
            this.stats.discoveredPacks = (this.stats.discoveredPacks || 0) + 1;
    
            // Add to current processing batch if appropriate
            if (this.fileManager.existingPacks) {
                this.fileManager.existingPacks.set(rkey, minimalPack);
            }
    
            return true;
    
        } catch (err) {
            if (err.code === 'ENOENT') {
                // First time discovering any packs, create the file
                try {
                    await this.fileManager.atomicWrite(
                        'starter_pack_urls.txt',
                        `${creator.handle}|${rkey}\n`
                    );
                    if (this.debug) {
                        this.debugLog('Created initial URLs file', {
                            rkey,
                            creator: creator.handle
                        });
                    }
                    return await this.trackNewStarterPack(uri, creator); // Retry the whole operation
                } catch (createErr) {
                    this.debugLog('Failed to create URLs file', {
                        error: createErr.message,
                        stack: createErr.stack
                    });
                    return false;
                }
            }
    
            this.debugLog('Error tracking new pack', {
                uri,
                error: err.message,
                stack: err.stack
            });
            return false;
        }
    }

    // Enhanced stat tracking
    async updateDiscoveryStats(profile, newPacks) {
        if (!this.stats) {
            this.stats = {
                discoveredUsers: 0,
                discoveredPacks: 0,
                processedProfiles: 0,
                updatedProfiles: 0
            };
        }

        this.stats.processedProfiles++;
        if (newPacks > 0) {
            this.stats.discoveredPacks += newPacks;
        }

        if (this.debug) {
            this.debugLog('Updated discovery stats', {
                profile: profile.handle,
                newPacks,
                stats: this.stats
            });
        }
    }
    
    async checkMemoryUsage() {
        const used = process.memoryUsage();
        const heapUsage = used.heapUsed / used.heapTotal;
    
        if (heapUsage > 0.85) {
            logger.warn(`High heap usage (${(heapUsage * 100).toFixed(1)}%), performing memory cleanup...`);
            
            // Clear caches
            this.profileCache.clear();
            
            // Force garbage collection
            if (global.gc) {
                global.gc();
                
                // Add a small delay to allow GC to complete
                await new Promise(resolve => setTimeout(resolve, 100));
                
                // Check if GC helped
                const afterGC = process.memoryUsage();
                const heapAfterGC = afterGC.heapUsed / afterGC.heapTotal;
                
                if (heapAfterGC > 0.80) {
                    // If still high after GC, take more aggressive measures
                    logger.warn(`Memory still high after GC (${(heapAfterGC * 100).toFixed(1)}%), performing emergency cleanup`);
                    
                    // Clear all caches
                    this.profileCache = new LRUCache(1000);
                    if (this.fileManager) {
                        this.fileManager.existingPacks.clear();
                        this.fileManager.userCache.clear();
                    }
                    
                    // Force GC again
                    global.gc();
                    await new Promise(resolve => setTimeout(resolve, 200));
                }
            }
        }
    }
    
    async handleDuplicateKeyError(batch, session) {
        logger.warn('Handling duplicate key errors individually');
        for (const op of batch) {
            try {
                await this.db.collection('users').updateOne(
                    op.updateOne.filter,
                    op.updateOne.update,
                    { 
                        session,
                        writeConcern: { w: 'majority' } 
                    }
                );
            } catch (innerErr) {
                if (innerErr.code !== 11000) {
                    throw innerErr;
                }
                logger.warn(`Skipping duplicate key for did: ${op.updateOne.filter.did}`);
            }
        }
    }

    async debugLog(message, data = null) {
        if (this.debug) {
            let logMessage = `[DEBUG] ${message}`;
            if (data !== null) {
                if (typeof data === 'object') {
                    // Handle undefined values and circular references
                    const safeData = JSON.parse(JSON.stringify(data, (key, value) => {
                        if (value === undefined) return 'undefined';
                        if (value === null) return 'null';
                        if (value instanceof Error) {
                            return {
                                message: value.message,
                                stack: value.stack,
                                ...value
                            };
                        }
                        return value;
                    }, 2));
                    logMessage += `\n${JSON.stringify(safeData, null, 2)}`;
                } else {
                    logMessage += ` ${data}`;
                }
            }
            logger.info(logMessage);
        }
    }

    async getProcessStats() {
        const stats = process.memoryUsage();
        return {
            memory: {
                heapUsed: Math.round(stats.heapUsed / 1024 / 1024),
                heapTotal: Math.round(stats.heapTotal / 1024 / 1024),
                external: Math.round(stats.external / 1024 / 1024),
                arrayBuffers: Math.round(stats.arrayBuffers / 1024 / 1024)
            },
            uptime: process.uptime()
        };
    }

    async getActorStarterPacks(did) {
        await this.refreshTokenIfNeeded();
        let allPacks = [];
        let cursor;
        
        try {
            do {
                await this.rateLimiter.throttle();
                
                const response = await this.apiCallWithTimeout(
                    this.agent.api.app.bsky.graph.getActorStarterPacks({
                        actor: did,
                        limit: 100,
                        cursor: cursor
                    })
                );
                
                if (response?.data?.starterPacks) {
                    allPacks = allPacks.concat(response.data.starterPacks);
                    cursor = response.data.cursor;
                } else {
                    break;
                }
                
                if (cursor) {
                    await this.delay(1000);
                }
                
            } while (cursor);
            
            return allPacks;
            
        } catch (err) {
            logger.error(`Error fetching starter packs for actor ${did}: ${err.stack || err.message}`);
            return [];
        }
    }

    async checkMongoHealth() {
        if (this.noMongoDB) return true;
        try {
            this.debugLog('Testing MongoDB connection');
            await this.db.admin().ping();
            this.debugLog('MongoDB connection successful');
            return true;
        } catch (err) {
            logger.error(`MongoDB health check failed: ${err.message}`);
            return false;
        }
    }

    async recover() {
        const backupFiles = await fs.promises.readdir('.')
            .then(files => files.filter(f => f.includes('.backup.')))
            .catch(() => []);
            
        if (backupFiles.length > 0) {
            const latest = backupFiles
                .sort((a, b) => b.localeCompare(a))
                .shift();
                
            await fs.promises.copyFile(latest, 'starter_packs.json');
            logger.info(`Recovered from backup: ${latest}`);
        }
    }

    async withTransaction(operations, maxRetries = 3) {
        let attempt = 0;
        let lastError = null;
        
        while (attempt < maxRetries) {
            let session = null;
            try {
                session = await this.mongoClient.startSession();
                await session.withTransaction(async () => {
                    await operations(session);
                }, {
                    readConcern: { level: 'majority' },
                    writeConcern: { w: 'majority' },
                    maxTimeMS: 30000
                });
                return;  // Success, exit
            } catch (err) {
                lastError = err;
                // Handle retriable errors
                if (err.hasErrorLabel('TransientTransactionError') ||
                    err.hasErrorLabel('UnknownTransactionCommitResult')) {
                    attempt++;
                    if (attempt < maxRetries) {
                        await new Promise(resolve => 
                            setTimeout(resolve, Math.pow(2, attempt) * 1000)
                        );
                        continue;
                    }
                }
                // Non-retriable error
                throw err;
            } finally {
                if (session) {
                    try {
                        await session.endSession();
                    } catch (endErr) {
                        logger.error(`Error ending session: ${endErr.message}`);
                    }
                }
            }
        }
        throw lastError || new Error('Transaction failed after retries');
    }

    async refreshTokenIfNeeded(forceRefresh = false) {
        const now = Date.now();
    
        // Return existing refresh operation if in progress
        if (this.#tokenRefreshPromise) {
            return this.#tokenRefreshPromise;
        }
    
        // Check if refresh needed
        if (!forceRefresh && 
            this.lastTokenRefresh && 
            (now - this.lastTokenRefresh < this.tokenRefreshInterval)) {
            return;
        }
    
        // Create new refresh operation
        this.#tokenRefreshPromise = (async () => {
            const maxRetries = 3;
            let lastError = null;
    
            for (let attempt = 0; attempt < maxRetries; attempt++) {
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
                    lastError = err;
                    logger.error(`Token refresh attempt ${attempt + 1}/${maxRetries} failed: ${err.stack || err.message}`);
                    
                    if (attempt < maxRetries - 1) {
                        await new Promise(resolve => 
                            setTimeout(resolve, Math.pow(2, attempt + 1) * 1000)
                        );
                        continue;
                    }
                }
            }
    
            throw new Error(`Failed to refresh token after ${maxRetries} attempts: ${lastError?.message}`);
        })();
    
        try {
            await this.#tokenRefreshPromise;
        } finally {
            this.#tokenRefreshPromise = null;
        }
    }

    async writeToMongoDB(packData, userOperations) {
        if (this.noMongoDB) return;
    
        try {
            // First update starter pack - without transaction
            await this.db.collection('starter_packs').updateOne(
                { rkey: packData.rkey },
                {
                    $set: {
                        rkey: packData.rkey,
                        name: packData.name,
                        creator: packData.creator,
                        creator_did: packData.creator_did,
                        description: packData.description || '',
                        user_count: parseInt(packData.user_count),
                        created_at: new Date(packData.created_at),
                        updated_at: new Date(packData.updated_at),
                        users: packData.users,
                        weekly_joins: packData.weekly_joins || 0,
                        total_joins: packData.total_joins || 0
                    }
                },
                { 
                    upsert: true,
                    writeConcern: { w: 1 }
                }
            );
    
            // Process users in smaller batches without transaction
            const batchSize = 25; // Even smaller batch size for Cosmos DB
            for (let i = 0; i < userOperations.length; i += batchSize) {
                const batch = userOperations.slice(i, i + batchSize);
                
                for (const op of batch) {
                    try {
                        await this.db.collection('users').updateOne(
                            op.updateOne.filter,
                            {
                                $set: {
                                    did: op.updateOne.update.$set.did,
                                    handle: op.updateOne.update.$set.handle,
                                    display_name: op.updateOne.update.$set.display_name,
                                    followers_count: op.updateOne.update.$set.followers_count,
                                    follows_count: op.updateOne.update.$set.follows_count,
                                    last_updated: op.updateOne.update.$set.last_updated,
                                    profile_check_needed: op.updateOne.update.$set.profile_check_needed
                                },
                                $addToSet: { pack_ids: packData.rkey }
                            },
                            { 
                                upsert: true,
                                writeConcern: { w: 1 }
                            }
                        );
                    } catch (err) {
                        if (err.code === 11000) {
                            // Handle duplicate key error by retrying without upsert
                            await this.db.collection('users').updateOne(
                                op.updateOne.filter,
                                {
                                    $set: op.updateOne.update.$set,
                                    $addToSet: { pack_ids: packData.rkey }
                                },
                                { writeConcern: { w: 1 } }
                            );
                        } else {
                            throw err;
                        }
                    }
                    
                    // Small delay between individual operations
                    await new Promise(resolve => setTimeout(resolve, 50));
                }
    
                // Delay between batches
                if (i + batchSize < userOperations.length) {
                    await new Promise(resolve => setTimeout(resolve, 200));
                    
                    // Force GC if available
                    if (global.gc && i % (batchSize * 4) === 0) {
                        global.gc();
                        await new Promise(resolve => setTimeout(resolve, 100));
                    }
                }
    
                // Log progress
                const progress = ((i + batch.length) / userOperations.length * 100).toFixed(1);
                logger.info(`Processed ${i + batch.length}/${userOperations.length} users (${progress}%)`);
            }
    
        } catch (err) {
            logger.error(`MongoDB write failed: ${err.stack || err.message}`);
            throw err;
        }
    
        // Final cleanup
        if (global.gc) {
            global.gc();
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }
    
    // Helper function to find duplicates
    async findDuplicates(collection, field) {
        const pipeline = [
            {
                $group: {
                    _id: `$${field}`,
                    count: { $sum: 1 },
                    docs: { $push: '$$ROOT' }
                }
            },
            {
                $match: {
                    count: { $gt: 1 }
                }
            }
        ];
    
        return await this.db.collection(collection).aggregate(pipeline).toArray();
    }

    async setupDatabase() {
        const startTime = Date.now();
        this.debugLog('Database setup starting');
        try {
            await this.mongoClient.connect();
            this.db = this.mongoClient.db('starterpacks');
            
            // First, determine if we're running on Cosmos DB
            const isCosmosDb = DB_INFO[DB_TYPE]?.isCosmosDb ?? false;
            logger.info(`Database type: ${DB_TYPE}, isCosmosDb: ${isCosmosDb}`);
    
            // Helper function for retryable operations
            const withRetry = async (operation, name) => {
                const maxRetries = 5;
                for (let attempt = 0; attempt < maxRetries; attempt++) {
                    try {
                        return await operation();
                    } catch (err) {
                        if (err.code === 16500 || err.code === 429 || err.message?.includes('TooManyRequests')) {
                            const retryAfterMs = err.RetryAfterMs || 1000 * Math.pow(2, attempt);
                            logger.warn(`Rate limit hit on ${name}, waiting ${retryAfterMs}ms before retry ${attempt + 1}/${maxRetries}`);
                            await new Promise(resolve => setTimeout(resolve, retryAfterMs));
                            continue;
                        }
                        throw err;
                    }
                }
                throw new Error(`Max retries (${maxRetries}) exceeded for ${name}`);
            };
    
            // 1. Setup collections without validators for Cosmos DB
            const collections = ['starter_packs', 'users'];
            for (const collection of collections) {
                await withRetry(
                    async () => this.db.createCollection(collection).catch(err => {
                        if (err.code !== 48) throw err; // Ignore if exists
                    }),
                    `create collection ${collection}`
                );
            }
    
            // 2. Check existing indexes (with rate limit handling)
            logger.info('Checking existing indexes...');
            const userIndexes = await withRetry(
                async () => this.db.collection('users').indexes(),
                'get users indexes'
            );
            const packIndexes = await withRetry(
                async () => this.db.collection('starter_packs').indexes(),
                'get starter_packs indexes'
            );
    
            logger.info('Existing indexes:', {
                users: userIndexes.map(i => ({
                    name: i.name,
                    key: i.key,
                    unique: i.unique
                })),
                starter_packs: packIndexes.map(i => ({
                    name: i.name,
                    key: i.key,
                    unique: i.unique
                }))
            });
    
            // Map existing indexes
            const existingUserIndexes = new Map(userIndexes.map(idx => [
                JSON.stringify(idx.key),
                idx
            ]));
            const existingPackIndexes = new Map(packIndexes.map(idx => [
                JSON.stringify(idx.key),
                idx
            ]));
    
            // 3. Define required indexes - reduced set for Cosmos DB
            const requiredIndexes = {
                users: [
                    { spec: { handle: 1 }, options: { name: 'handle_1' } },
                    { spec: { pack_ids: 1 }, options: { name: 'pack_ids_1' } },
                    { spec: { last_updated: 1 }, options: { name: 'last_updated_1' } },
                    { spec: { profile_check_needed: 1 }, options: { name: 'profile_check_needed_1' } }
                ],
                starter_packs: [
                    { spec: { creator_did: 1 }, options: { name: 'creator_did_1' } },
                    { spec: { creator: 1 }, options: { name: 'creator_1' } },
                    { spec: { updated_at: 1 }, options: { name: 'updated_at_1' } }
                ]
            };
    
            // Only add unique indexes if not on Cosmos DB
            if (!isCosmosDb) {
                requiredIndexes.users.unshift({ 
                    spec: { did: 1 }, 
                    options: { unique: true, name: 'unique_did' } 
                });
                requiredIndexes.starter_packs.unshift({ 
                    spec: { rkey: 1 }, 
                    options: { unique: true, name: 'unique_rkey' } 
                });
            }
    
            // 4. Create only missing indexes with rate limit handling
            for (const [collection, indexes] of Object.entries(requiredIndexes)) {
                const existingIndexMap = collection === 'users' ? existingUserIndexes : existingPackIndexes;
                
                for (const indexDef of indexes) {
                    const indexKey = JSON.stringify(indexDef.spec);
                    const existing = existingIndexMap.get(indexKey);
    
                    if (!existing) {
                        try {
                            logger.info(`Creating missing index on ${collection}: ${JSON.stringify(indexDef.spec)}`);
                            await withRetry(
                                async () => this.db.collection(collection).createIndex(
                                    indexDef.spec,
                                    {
                                        background: true,
                                        ...indexDef.options
                                    }
                                ),
                                `create index ${indexDef.options.name}`
                            );
                            logger.info(`Successfully created index on ${collection}: ${indexDef.options.name}`);
                        } catch (err) {
                            if (isCosmosDb && err.message?.includes('unique')) {
                                logger.warn(`Skipping unique index creation for Cosmos DB: ${err.message}`);
                            } else {
                                logger.error(`Error creating index on ${collection}: ${err.message}`);
                            }
                        }
                    } else {
                        logger.info(`Index already exists on ${collection}: ${indexKey}`);
                    }
                }
            }
    
            logger.info('MongoDB setup completed successfully');
    
            if (this.debug) {
                const duration = Date.now() - startTime;
                const collections = await withRetry(
                    async () => this.db.listCollections().toArray(),
                    'list collections'
                );
                const finalUserIndexes = await withRetry(
                    async () => this.db.collection('users').indexes(),
                    'get final user indexes'
                );
                const finalPackIndexes = await withRetry(
                    async () => this.db.collection('starter_packs').indexes(),
                    'get final pack indexes'
                );
                
                this.debugLog('Database setup complete', {
                    duration,
                    collections: collections.map(c => c.name),
                    userIndexes: finalUserIndexes.map(i => ({
                        name: i.name,
                        key: i.key,
                        unique: i.unique
                    })),
                    packIndexes: finalPackIndexes.map(i => ({
                        name: i.name,
                        key: i.key,
                        unique: i.unique
                    })),
                    memoryUsage: process.memoryUsage()
                });
            }
    
        } catch (err) {
            logger.error(`Error setting up database: ${err.stack || err.message}`);
            throw err;
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
        let attempts = 0;
        const MAX_ATTEMPTS = 3;
        const MAX_TOTAL_MEMBERS = 1000; // Safety limit
        
        do {
            try {
                const response = await this.getListMembers(uri, cursor);
                if (!response) break;
                
                allMembers = allMembers.concat(response.items);
                cursor = response.cursor;
                
                if (allMembers.length >= MAX_TOTAL_MEMBERS) {
                    logger.warn(`Reached maximum member limit (${MAX_TOTAL_MEMBERS}), stopping pagination`);
                    break;
                }
                
                if (cursor) {
                    logger.info(`Fetched ${allMembers.length} members so far, getting more...`);
                    await this.delay(1000); // Add delay between pages
                }
    
                attempts = 0; // Reset attempts on success
            } catch (err) {
                attempts++;
                logger.error(`Error fetching list page (attempt ${attempts}/${MAX_ATTEMPTS}): ${err.message}`);
                
                if (attempts >= MAX_ATTEMPTS) {
                    logger.error('Max attempts reached, stopping pagination');
                    break;
                }
                
                await this.delay(Math.pow(2, attempts) * 1000); // Exponential backoff
            }
        } while (cursor);
        
        logger.info(`Finished fetching all ${allMembers.length} members`);
        return allMembers;
    }
    
    async getList(uri, cursor = null) {
        const response = await this.makeApiCall('getList',
            this.agent.api.app.bsky.graph.getList({
                list: uri,
                limit: 100,
                cursor
            })
        );
    
        return response?.data?.items || [];
    }
    
    async trackNewStarterPack_old(uri, creator) {
        if (!uri || !creator) return;
    
        try {
            const rkey = uri.split('/').pop();
            const line = `${creator.handle}|${rkey}\n`;
    
            // Add to URLs file for future processing
            await this.fileManager.atomicWrite(
                'starter_pack_urls.txt',
                line,
                { flag: 'a' }
            );
    
            if (this.debug) {
                this.debugLog('Tracked new starter pack', {
                    uri,
                    creator: creator.handle,
                    rkey
                });
            }
    
        } catch (err) {
            if (this.debug) {
                this.debugLog('Failed to track new starter pack', {
                    uri,
                    error: err.message,
                    stack: err.stack
                });
            }
        }
    }

    async getListMembers(uri, cursor) {
        await this.refreshTokenIfNeeded();
        const maxRetries = 2;  // Keep the existing retry count
        
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.rateLimiter.throttle();
    
                const response = await this.apiCallWithTimeout(
                    this.agent.api.app.bsky.graph.getList({
                        list: uri,
                        limit: 100,
                        cursor: cursor || undefined
                    })
                );
                
                if (!response?.data?.items) {
                    throw new Error('No items found in response');
                }
    
                const members = response.data.items;
                logger.info(`Found ${members.length} members in list${cursor ? ' (continuation)' : ''}`);
    
                return {
                    items: members,
                    cursor: response.data.cursor
                };
    
            } catch (err) {
                // Keep exact same error handling as current version
                if (err.status === 404 || err.message.includes('List not found')) {
                    logger.warn(`List not found: ${uri}`);
                    return null;
                }
                
                if (err.message === 'Token has expired' || err.message === 'Authentication Required') {
                    await this.refreshTokenIfNeeded(true);
                    continue;
                }
    
                if (err.status === 429) {
                    const shouldRetry = await this.rateLimiter.handleResponse(err);
                    if (shouldRetry && attempt < maxRetries - 1) {
                        continue;
                    }
                }
    
                if (this.isRetryableError(err) && attempt < maxRetries - 1) {
                    logger.error(`Error fetching list page (attempt ${attempt + 1}/${maxRetries}): ${err.message}`);
                    await this.delay(Math.pow(2, attempt) * 1000);
                    continue;
                }
    
                return null;
            }
        }
        return null;
    }

    async getProfile(did, forceUpdate = false) {
        // 1. Validate Parameters
        if (!did) {
            throw new Error('DID parameter is required');
        }
    
        if (this.debug) {
            this.debugLog('Starting profile fetch', {
                did,
                forceUpdate,
                timestamp: new Date().toISOString()
            });
        }
    
        // 2. Check if profile is known to be missing
        if (!forceUpdate && this.checkpointManager.isMissingProfile(did)) {
            logger.info(`Skipping fetch for known missing profile: ${did}`);
            return null;
        }
    
        const TEN_DAYS = 10 * 24 * 60 * 60 * 1000;
    
        try {
            // 3. Check Memory Cache
            const cached = this.profileCache.get(did);
            if (cached && !forceUpdate && Date.now() - cached.timestamp < this.profileCacheExpiry) {
                if (this.debug) {
                    this.debugLog('Returning cached profile', {
                        did,
                        cacheAge: Date.now() - cached.timestamp
                    });
                }
                return cached.profile;
            }
    
            // 4. Check MongoDB
            if (!this.noMongoDB) {
                try {
                    const userDoc = await this.db.collection('users').findOne({ did });
                    if (userDoc && !forceUpdate) {
                        const needsUpdate = Date.now() - userDoc.last_updated.getTime() > TEN_DAYS;
                        if (!needsUpdate) {
                            const profile = {
                                did: userDoc.did,
                                handle: userDoc.handle,
                                displayName: userDoc.display_name,
                                followersCount: userDoc.followers_count,
                                followsCount: userDoc.follows_count,
                                associated: userDoc.associated || {}
                            };
                            this.profileCache.set(did, {
                                profile,
                                timestamp: Date.now()
                            });
                            if (this.debug) {
                                this.debugLog('Returning MongoDB profile', {
                                    did,
                                    lastUpdated: userDoc.last_updated
                                });
                            }
                            return profile;
                        }
    
                        // Mark for update if older than 10 days
                        if (needsUpdate) {
                            await this.db.collection('users').updateOne(
                                { did },
                                { $set: { profile_check_needed: true } }
                            );
                            if (this.debug) {
                                this.debugLog('Marked profile for update', { did });
                            }
                        }
                    }
                } catch (err) {
                    logger.error(`MongoDB profile check error for ${did}: ${err.stack || err.message}`);
                }
            }
    
            // 5. Fetch from API with fallback strategy
            const endpoints = [
                { url: 'https://api.bsky.app', auth: true },  // Try main API first
                { url: 'https://public.api.bsky.app', auth: false }, // Then public API
                { url: 'https://bsky.social', auth: true }  // Finally social endpoint
            ];
        
            let apiProfile = null;
            let lastError = null;
        
            for (const endpoint of endpoints) {
                try {
                    if (this.debug) {
                        this.debugLog('Attempting profile fetch', {
                            did,
                            endpoint: endpoint.url,
                            requiresAuth: endpoint.auth
                        });
                    }
        
                    if (endpoint.auth) {
                        // Use existing authenticated API call path
                        const response = await this.makeApiCall('getProfile',
                            this.agent.api.app.bsky.actor.getProfile({
                                actor: did
                            })
                        );
                        apiProfile = response?.data;
                    } else {
                        // For public API, use authorized agent but different base URL
                        const response = await this.makeApiCall('getProfile',
                            this.agent.api.app.bsky.actor.getProfile({
                                actor: did
                            }),
                            { 
                                baseUrl: endpoint.url,
                                maxRetries: 2 
                            }
                        );
                        apiProfile = response?.data;
                    }
        
                    if (apiProfile) {
                        if (this.debug) {
                            this.debugLog('Profile fetch successful', {
                                did,
                                endpoint: endpoint.url
                            });
                        }
                        break;
                    }
        
                } catch (err) {
                    lastError = err;
                    if (this.debug) {
                        this.debugLog('Profile fetch failed', {
                            did,
                            endpoint: endpoint.url,
                            error: err.message
                        });
                    }
        
                    // Handle rate limits consistently
                    if (err.status === 429) {
                        const shouldRetry = await this.rateLimiter.handleResponse(err);
                        if (shouldRetry) {
                            await this.delay(2000);
                            continue;
                        }
                    }
        
                    // For 502s and network errors, try next endpoint
                    if (err.status === 502 || this.isNetworkError(err)) {
                        await this.delay(1000);
                        continue;
                    }
        
                    // For auth errors, refresh token and retry
                    if (err.message?.includes('Token has expired') ||
                        err.message?.includes('Authentication Required')) {
                        await this.refreshTokenIfNeeded(true);
                        continue;
                    }
        
                    // For other errors, try next endpoint
                    await this.delay(1000);
                }
            }
    
            if (!apiProfile) {
                if (this.debug) {
                    this.debugLog('Failed to fetch profile from all endpoints', {
                        did,
                        lastError: lastError?.message
                    });
                }
                await this.checkpointManager.addMissingProfile(did, 'api_fetch_failed');
                return null;
            }
    
            // 6. Initialize User Data
            const userData = {
                did: did,
                handle: apiProfile.handle,
                display_name: apiProfile.displayName || '',
                followers_count: parseInt(apiProfile.followersCount, 10) || 0,
                follows_count: parseInt(apiProfile.followsCount, 10) || 0,
                last_updated: new Date(),
                profile_check_needed: false,
                associated: {
                    starterPacks: []
                }
            };
    
            try {
                const needsWrite = forceUpdate || !await this.findExistingUser(did);
                if (needsWrite) {
                    await this.fileManager.writeUser(userData);
                    if (this.debug) {
                        this.debugLog('Buffered write of new user data to files', {
                            did,
                            handle: userData.handle,
                            reason: forceUpdate ? 'forced_update' : 'new_user'
                        });
                    }
                }
            } catch (writeErr) {
                this.debugLog('Failed to buffer write of user data to files', {
                    did,
                    error: writeErr.message,
                    stack: writeErr.stack
                });
                // Continue processing despite write failure
            }

            let starterPacks = [];
    
            // 7. Process Starter Packs if Present
            if (apiProfile.associated?.starterPacks > 0) {
                logger.info(`Profile ${did} has ${apiProfile.associated.starterPacks} starter packs, fetching...`);
                try {
                    const packs = await this.getActorStarterPacks(did);
                    starterPacks = packs.map(pack => pack.uri);
            
                    // Enhanced discovery logging
                    let newPacksFound = 0;
            
                    // Handle new packs discovery
                    for (const pack of packs) {
                        const rkey = this.extractRkeyFromURI(pack.uri);
                        
                        const isNewPack = await this.trackNewStarterPack(pack.uri, {
                            did,
                            handle: apiProfile.handle,
                            name: pack.name,
                            description: pack.description
                        });
            
                        if (isNewPack) {
                            newPacksFound++;
                            if (this.debug) {
                                this.debugLog('Discovered new pack during profile fetch', {
                                    profileDid: did,
                                    packUri: pack.uri,
                                    packName: pack.name
                                });
                            }
                        }
                    }
            
                    // Update discovery stats
                    await this.updateDiscoveryStats(apiProfile, newPacksFound);
            
                    userData.associated.starterPacks = starterPacks;
                } catch (err) {
                    logger.error(`Error fetching starter packs for ${did}: ${err.stack || err.message}`);
                    if (this.debug) {
                        this.debugLog('Starter pack fetch error', {
                            did,
                            error: err.message,
                            stack: err.stack
                        });
                    }
                }
            }
    
            // 8. Update MongoDB
            if (!this.noMongoDB) {
                const session = this.mongoClient.startSession();
                try {
                    await session.withTransaction(async () => {
                        await this.db.collection('users').updateOne(
                            { did },
                            {
                                $set: userData,
                                $addToSet: { 
                                    pack_ids: { 
                                        $each: starterPacks.map(uri => this.extractRkeyFromURI(uri)) 
                                    }
                                }
                            },
                            { upsert: true, session }
                        );
    
                        if (starterPacks.length > 0) {
                            await this.db.collection('starter_packs').updateMany(
                                { users: did },
                                { $set: { updated_at: new Date() } },
                                { session }
                            );
                        }
                    });
                } catch (err) {
                    logger.error(`Error updating user in MongoDB for ${did}: ${err.stack || err.message}`);
                } finally {
                    await session.endSession();
                }
            }
    
            // 9. Create Profile Object for Cache
            const profile = {
                did: userData.did,
                handle: userData.handle,
                displayName: userData.display_name,
                followersCount: userData.followers_count,
                followsCount: userData.follows_count,
                associated: {
                    starterPacks: userData.associated.starterPacks
                }
            };
    
            // 10. Update Memory Cache
            this.profileCache.set(did, {
                profile,
                timestamp: Date.now(),
                discoveryChecked: true  // Flag to indicate we've checked for associated packs
            });
    
            if (this.debug) {
                this.debugLog('Profile fetch complete', {
                    did,
                    handle: profile.handle,
                    starterPackCount: starterPacks.length
                });
            }
    
            return profile;
        } catch (err) {
            logger.error(`Error in getProfile for ${did}: ${err.stack || err.message}`);
            throw err;
        }
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
        const response = await this.makeApiCall('getProfile',
            this.agent.api.app.bsky.actor.getProfile({
                actor: did
            })
        );
    
        if (!response?.data) return null;
    
        return {
            did: response.data.did,
            handle: response.data.handle,
            displayName: response.data.displayName?.trim() || response.data.handle,
            followersCount: response.data.followersCount || 0,
            followsCount: response.data.followsCount || 0,
            associated: response.data.associated
        };
    }
    
    // Base API call handler with consistent debugging and error handling
    async makeApiCall(name, apiCall, options = {}) {
        const {
            maxRetries = 3,
            shouldFallback = false,
            fallbackFn = null,
            baseUrl = null
        } = options;
    
        if (this.debug) {
            this.debugLog(`API call starting: ${name}`, {
                timestamp: new Date().toISOString(),
                options,
                memory: process.memoryUsage(),
                baseUrl: baseUrl || 'default'
            });
        }
    
        await this.refreshTokenIfNeeded();
    
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.rateLimiter.throttle();
    
                const response = await this.apiCallWithTimeout(apiCall);
                const shouldRetry = await this.rateLimiter.handleResponse(response);
    
                // Enhanced response logging
                if (this.debug) {
                    this.debugLog(`API call response: ${name}`, {
                        attempt: attempt + 1,
                        status: response?.status,
                        headers: {
                            rateLimit: {
                                remaining: response?.headers?.['x-ratelimit-remaining'],
                                reset: response?.headers?.['x-ratelimit-reset'],
                                limit: response?.headers?.['x-ratelimit-limit']
                            }
                        },
                        data: response?.data ? {
                            type: typeof response.data,
                            keys: Object.keys(response.data),
                            preview: JSON.stringify(response.data).slice(0, 200) + '...'
                        } : null,
                        shouldRetry
                    });
                }
    
                if (shouldRetry && attempt < maxRetries - 1) {
                    const delay = Math.pow(2, attempt) * 1000;
                    if (this.debug) {
                        this.debugLog(`Rate limit retry: ${name}`, { 
                            delay,
                            attempt,
                            nextAttempt: attempt + 1
                        });
                    }
                    await this.delay(delay);
                    continue;
                }
    
                return response;
    
            } catch (err) {
                // Handle "not found" cases
                const isNotFound = err.status === 404 || 
                                 err.message?.includes('not found') ||
                                 err.message?.includes('Could not locate');
    
                if (isNotFound) {
                    if (this.debug) {
                        this.debugLog(`Resource not found: ${name}`, {
                            error: err.message,
                            attempt: attempt + 1,
                            context: {
                                status: err.status,
                                message: err.message
                            }
                        });
                    }
                    return null;
                }
    
                // Handle rate limiting
                if (err.status === 429) {
                    const shouldRetry = await this.rateLimiter.handleResponse(err);
                    if (shouldRetry && attempt < maxRetries - 1) {
                        if (this.debug) {
                            this.debugLog(`Rate limit hit: ${name}`, {
                                attempt,
                                headers: err.headers,
                                willRetry: true
                            });
                        }
                        continue;
                    }
                    break;
                }
    
                // Handle authentication issues
                if (err.message?.includes('Token has expired') || 
                    err.message?.includes('Authentication Required')) {
                    if (this.debug) {
                        this.debugLog(`Auth refresh needed: ${name}`, {
                            attempt,
                            error: err.message
                        });
                    }
                    await this.refreshTokenIfNeeded(true);
                    if (attempt < maxRetries - 1) continue;
                }
    
                // Enhanced error logging
                if (this.debug) {
                    this.debugLog(`API call error: ${name}`, {
                        error: err.message,
                        status: err.status,
                        attempt: attempt + 1,
                        stack: err.stack,
                        headers: err.headers,
                        context: {
                            isNetworkError: this.isNetworkError(err),
                            willRetry: attempt < maxRetries - 1,
                            remainingAttempts: maxRetries - attempt - 1
                        }
                    });
                }
    
                // Handle fallback for network errors
                if (shouldFallback && fallbackFn && this.isNetworkError(err)) {
                    if (this.debug) {
                        this.debugLog(`Attempting fallback: ${name}`, {
                            error: err.message,
                            attempt
                        });
                    }
                    try {
                        const fallbackResult = await fallbackFn();
                        if (this.debug) {
                            this.debugLog(`Fallback successful: ${name}`, {
                                hasData: !!fallbackResult
                            });
                        }
                        return fallbackResult;
                    } catch (fallbackErr) {
                        if (this.debug) {
                            this.debugLog(`Fallback failed: ${name}`, {
                                error: fallbackErr.message,
                                stack: fallbackErr.stack
                            });
                        }
                    }
                }
    
                // Handle retryable network errors
                if (this.isNetworkError(err) && attempt < maxRetries - 1) {
                    const delay = Math.pow(2, attempt) * 1000;
                    if (this.debug) {
                        this.debugLog(`Network error retry: ${name}`, {
                            delay,
                            attempt,
                            error: err.message
                        });
                    }
                    await this.delay(delay);
                    continue;
                }
    
                // Log final failure
                if (attempt === maxRetries - 1) {
                    logger.error(`Failed API call ${name} after ${maxRetries} attempts: ${err.message}`);
                    if (this.debug) {
                        this.debugLog(`Final failure: ${name}`, {
                            attempts: maxRetries,
                            error: err.message,
                            stack: err.stack
                        });
                    }
                }
    
                throw err; // Propagate the error if we've exhausted all retries
            }
        }
    
        return null;
    }

    // Consistent starter pack fetching
    async getStarterPack(uri) {
        const response = await this.makeApiCall('getStarterPack', 
            this.agent.app.bsky.graph.getStarterPack({ starterPack: uri }),
            {
                shouldFallback: true,
                fallbackFn: async () => {
                    const [repo, collection, rkey] = uri.replace('at://', '').split('/');
                    const recordResponse = await this.makeApiCall('getRecord',
                        this.agent.api.com.atproto.repo.getRecord({
                            repo,
                            collection,
                            rkey
                        })
                    );
                    if (recordResponse?.data) {
                        return {
                            data: {
                                starterPack: {
                                    uri,
                                    record: recordResponse.data.value,
                                    cid: recordResponse.data.cid
                                }
                            }
                        };
                    }
                    return null;
                }
            }
        );

        return response?.data?.starterPack || null;
    }

    async getStarterPackStats(uri) {
        this.debugLog(`Getting stats for pack: ${uri}`);
        await this.refreshTokenIfNeeded();
    
        if (!uri) {
            logger.warn('No URI provided for getStarterPackStats');
            return { weekly_joins: 0, total_joins: 0 };
        }
    
        try {
            await this.rateLimiter.throttle();
            
            const response = await this.apiCallWithTimeout(
                this.agent.app.bsky.graph.getStarterPack({
                    starterPack: uri  // Use full URI
                })
            );
    
            if (response?.data?.starterPack) {
                return {
                    weekly_joins: response.data.starterPack.joinedWeekCount || 0,
                    total_joins: response.data.starterPack.joinedAllTimeCount || 0
                };
            }
        } catch (err) {
            if (err.status === 429) {
                await this.rateLimiter.handleResponse(err);
                return this.getStarterPackStats(uri);
            }
            logger.error(`Error getting stats for pack ${uri}: ${err.message}`);
        }
    
        return { weekly_joins: 0, total_joins: 0 };
    }
    
    // Skeleton Stage: Fetching Starter Pack URIs
    /**
     * Fetches all starter pack URIs by searching across multiple Bluesky servers.
     * If no starter packs are found via API, falls back to reading from a file.
     * 
     * @returns {Promise<Array<string>>} - An array of starter pack URIs.
     */
    
    async fetchStarterPackURIs() {
        logger.info('Starting comprehensive starter pack discovery...');
        const uris = new Set();
        const basePublicUrl = 'https://public.api.bsky.app';
        const maxRetries = 3;
    
        // Helper for safe public API calls
        const safePublicCall = async (endpoint, params = {}) => {
            let attempt = 0;
            while (attempt < maxRetries) {
                try {
                    const queryString = Object.entries(params)
                        .map(([key, value]) => `${key}=${encodeURIComponent(value)}`)
                        .join('&');
                    const url = `${basePublicUrl}/xrpc/${endpoint}${queryString ? '?' + queryString : ''}`;
                    
                    logger.info(`Making public API call to: ${url}`);
                    const response = await fetch(url, {
                        method: 'GET',
                        headers: { 'Accept': 'application/json' }
                    });
    
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
    
                    const data = await response.json();
                    logger.info(`Public API response for ${endpoint}:`, JSON.stringify(data, null, 2));
                    return data;
                } catch (err) {
                    attempt++;
                    logger.warn(`Public API attempt ${attempt}/${maxRetries} failed for ${endpoint}: ${err.message}`);
                    if (attempt < maxRetries) {
                        await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
                    }
                }
            }
            return null;
        };
    
        // Helper for safe authenticated API calls
        const safeAuthCall = async (name, apiCall) => {
            let attempt = 0;
            while (attempt < maxRetries) {
                try {
                    logger.info(`Making authenticated API call: ${name}`);
                    const response = await this.apiCallWithTimeout(apiCall);
                    logger.info(`Auth API response for ${name}:`, JSON.stringify(response?.data, null, 2));
                    return response?.data;
                } catch (err) {
                    attempt++;
                    logger.warn(`Auth API attempt ${attempt}/${maxRetries} failed for ${name}: ${err.message}`);
                    if (attempt < maxRetries) {
                        // Try to refresh auth if it seems to be an auth issue
                        if (err.message.includes('auth') || err.message.includes('401')) {
                            try {
                                await this.refreshTokenIfNeeded(true);
                            } catch (refreshErr) {
                                logger.warn(`Token refresh failed: ${refreshErr.message}`);
                            }
                        }
                        await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
                    }
                }
            }
            return null;
        };
    
        try {
            // Try different methods to discover starter packs
            const methods = [
                // Method 1: Public search API
                async () => {
                    logger.info('\n=== Trying Public Search API ===');
                    const searchData = await safePublicCall('app.bsky.graph.searchStarterPacks', {
                        q: '*',
                        limit: 100
                    });
                    
                    if (searchData?.starterPacks?.length) {
                        searchData.starterPacks.forEach(pack => {
                            if (pack.uri) uris.add(pack.uri);
                        });
                        logger.info(`Found ${searchData.starterPacks.length} packs via public search`);
                    }
                },
    
                // Method 2: Public getStarterPacks API (if we have any URIs)
                async () => {
                    if (uris.size > 0) {
                        logger.info('\n=== Trying Public Get Starter Packs API ===');
                        const batchSize = 25;
                        const urisList = Array.from(uris);
                        
                        for (let i = 0; i < urisList.length; i += batchSize) {
                            const batch = urisList.slice(i, i + batchSize);
                            const batchData = await safePublicCall('app.bsky.graph.getStarterPacks', {
                                uris: JSON.stringify(batch)
                            });
                            
                            if (batchData?.starterPacks?.length) {
                                batchData.starterPacks.forEach(pack => {
                                    if (pack.uri) uris.add(pack.uri);
                                });
                            }
                            
                            await new Promise(resolve => setTimeout(resolve, 1000));
                        }
                    }
                },
    
                // Method 3: Try authenticated search if available
                async () => {
                    if (this.agent?.session) {
                        logger.info('\n=== Trying Authenticated Search API ===');
                        const authSearchData = await safeAuthCall(
                            'searchStarterPacks',
                            this.agent.api.app.bsky.graph.searchStarterPacks({
                                q: '*',
                                limit: 100
                            })
                        );
                        
                        if (authSearchData?.starterPacks?.length) {
                            authSearchData.starterPacks.forEach(pack => {
                                if (pack.uri) uris.add(pack.uri);
                            });
                            logger.info(`Found ${authSearchData.starterPacks.length} packs via authenticated search`);
                        }
                    }
                },
    
                // Method 4: Try skeleton search as last resort
                async () => {
                    if (this.agent?.session) {
                        logger.info('\n=== Trying Skeleton Search API ===');
                        const skeletonData = await safeAuthCall(
                            'searchStarterPacksSkeleton',
                            this.agent.api.app.bsky.unspecced.searchStarterPacksSkeleton({
                                q: '*',
                                limit: 100
                            })
                        );
                        
                        if (skeletonData?.starterPacks?.length) {
                            skeletonData.starterPacks.forEach(pack => {
                                if (pack.uri) uris.add(pack.uri);
                            });
                            logger.info(`Found ${skeletonData.starterPacks.length} packs via skeleton search`);
                        }
                    }
                }
            ];
    
            // Try each method
            for (const method of methods) {
                try {
                    await method();
                } catch (err) {
                    logger.warn(`Method failed: ${err.message}`);
                }
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
    
            const discoveredUris = Array.from(uris);
            logger.info('\n=== Discovery Summary ===');
            logger.info(`Found ${discoveredUris.length} total unique starter packs`);
            discoveredUris.forEach(uri => logger.info(`- ${uri}`));
    
            // Fall back to file if needed
            if (discoveredUris.length === 0) {
                logger.info('\n=== Falling Back to File Mode ===');
                try {
                    const content = await fs.promises.readFile('starter_pack_urls.txt', 'utf-8');
                    const fileUris = content
                        .split('\n')
                        .filter(line => line.trim() && line.includes('|'))
                        .map(line => {
                            const [handle, rkey] = line.trim().split('|');
                            return rkey;
                        });
                    logger.info(`Found ${fileUris.length} packs in file`);
                    return fileUris;
                } catch (fileErr) {
                    logger.warn(`File fallback failed: ${fileErr.message}`);
                    return [];
                }
            }
    
            return discoveredUris;
    
        } catch (err) {
            logger.error(`Fatal error in starter pack discovery: ${err.stack || err.message}`);
            // Even if everything fails, don't crash
            return [];
        }
    }

    /**
     * Hydrates starter pack data by fetching detailed information for each URI.
     * @param {Array<string>} uris - Array of starter pack URIs.
     * @returns {Promise<Array<Object>>} - Array of hydrated starter packs.
     */
    async hydrateStarterPacks(uris) {
        let hydratedPacks = [];

        for (const uri of uris) {
            try {
                logger.info(`Hydrating starter pack: ${uri}`);
                const response = await this.apiCallWithTimeout(
                    this.agent.app.bsky.graph.getStarterPack({
                        uri
                    })
                );

                if (response?.data) {
                    hydratedPacks.push(response.data);
                    logger.info(`Hydrated starter pack: ${uri}`);
                } else {
                    logger.warn(`No data found for starter pack: ${uri}`);
                }

                // Respect rate limits
                await this.delay(500); // 0.5-second delay between requests
            } catch (err) {
                logger.error(`Error hydrating starter pack ${uri}: ${err.stack || err.message}`);

                if (err.status === 429) { // Too Many Requests
                    logger.warn('Rate limit hit during hydration. Waiting before retrying...');
                    const shouldRetry = await this.rateLimiter.handleResponse(err);
                    if (shouldRetry) {
                        await this.delay(2000); // Wait before retrying
                        hydratedPacks.push(uri); // Re-add to the queue
                        continue; // Retry this pack
                    }
                }

                // Decide whether to skip or retry based on error type
                if (this.shouldRetry(err)) { // Implement shouldRetry as a class method
                    await this.delay(2000); // 2-second delay before retry
                    hydratedPacks.push(uri); // Re-add to the queue
                    continue;
                }

                // Skip this pack on irrecoverable errors
                logger.warn(`Skipping starter pack due to error: ${uri}`);
            }
        }

        return hydratedPacks;
    }

    /**
     * Applies business rules to filter out unwanted starter packs.
     * @param {Array<Object>} hydratedPacks - Array of hydrated starter packs.
     * @returns {Promise<Array<Object>>} - Array of filtered starter packs.
     */
    async applyRules(hydratedPacks) {
        // If no rules are applied, return all hydrated packs
        const filteredPacks = hydratedPacks; 

        logger.info(`Filtered starter packs. Original: ${hydratedPacks.length}, After filtering: ${filteredPacks.length}`);
        return filteredPacks;
    }

    /**
     * Structures the filtered starter packs for further processing.
     * @param {Array<Object>} filteredPacks - Array of filtered starter packs.
     * @returns {Promise<Array<Object>>} - Array of structured starter packs.
     */
    async presentStarterPacks(filteredPacks) {
        // Structure the data as per your requirements
        const structuredPacks = filteredPacks.map(pack => ({
            rkey: this.extractRkeyFromURI(pack.uri), // Correctly invoke with 'this'
            name: pack.name,
            description: pack.description || '',
            creator: {
                handle: pack.creator.handle,
                did: pack.creator.did
            },
            usageStats: {
                weeklyJoins: pack.joinedWeek || 0,
                totalJoins: pack.joinedAllTime || 0
            },
            // Add more fields as necessary
        }));

        logger.info(`Structured ${structuredPacks.length} starter packs for processing.`);
        return structuredPacks;
    }

    /**
     * Determines whether a request should be retried based on the error.
     * @param {Object} err - The error object.
     * @returns {boolean} - True if the request should be retried, else false.
     */
    shouldRetry(err) {
        // Define conditions under which the script should retry
        // For example, network errors, transient server errors, etc.
        const retryableStatuses = [500, 502, 503, 504];
        return retryableStatuses.includes(err.status);
    }

    /**
     * Extracts the rkey from a starter pack URI.
     * @param {string} uri - The starter pack URI.
     * @returns {string} - The extracted rkey.
     */
    extractRkeyFromURI(uri) {
        // Assuming URI format: at://<creator_did>/app.bsky.graph.starterpack/<rkey>
        const parts = uri.split('/');
        return parts[parts.length - 1];
    }

    /**
     * Processes all fetched starter packs through the pipeline.
     */
    async collectAllStarterPacks() {
        try {
            // 1. Fetch all starter pack URIs
            const uris = await this.fetchStarterPackURIs();
            logger.info(`Total starter pack URIs fetched: ${uris.length}`);
            
            if (uris.length === 0) {
                logger.warn('No starter pack URIs found. Falling back to file-based mode.');
                await this.processUrls('starter_pack_urls.txt');
                return;
            }

            // 2. Hydrate starter pack data
            const hydratedPacks = await this.hydrateStarterPacks(uris);
            logger.info(`Total starter packs hydrated: ${hydratedPacks.length}`);

            // 3. Apply business rules
            const filteredPacks = await this.applyRules(hydratedPacks);
            logger.info(`Total starter packs after applying rules: ${filteredPacks.length}`);

            // 4. Structure the data for further processing
            const structuredPacks = await this.presentStarterPacks(filteredPacks);

            // 5. Process each structured starter pack
            for (const pack of structuredPacks) {
                const urlLine = `${pack.creator.handle}|${pack.rkey}`;
                await this.processStarterPack(urlLine);
                await this.delay(1000); // Respect rate limits
            }

            logger.info('All starter packs have been processed successfully.');
        } catch (err) {
            logger.error(`Error during starter pack collection: ${err.stack || err.message}`);
            await this.cleanup();
            process.exit(1);
        }
    }

    /**
     * Loads existing users for a pack from MongoDB or cache
     * @param {Object} existingPack - The existing pack data if any
     * @returns {Map} Map of user DIDs to user data
     */
    async loadExistingUsers(existingPack) {
        if (this.processor?.debug) {
            this.processor.debugLog('Loading existing useres');
        }
        
        const existingUsers = new Map();
        
        if (!existingPack) {
            return existingUsers;
        }
    
        try {
            const existingUserDids = new Set(existingPack.users);
            let remainingDids = [];  // Moved outside conditional block
            
            // Try cache first
            for (const did of existingUserDids) {
                const cached = this.profileCache.get(did);
                if (cached && Date.now() - cached.timestamp < this.profileCacheExpiry) {
                    existingUsers.set(did, cached.profile);
                }
            }
    
            // Calculate remaining DIDs regardless of MongoDB status
            remainingDids = Array.from(existingUserDids)
                .filter(did => !existingUsers.has(did));
    
            // Get remaining from MongoDB
            if (!this.noMongoDB && remainingDids.length > 0) {
                const users = await this.db.collection('users')
                    .find({ did: { $in: remainingDids } })
                    .toArray();
                    
                users.forEach(user => {
                    existingUsers.set(user.did, user);
                    // Update cache
                    this.profileCache.set(user.did, {
                        profile: user,
                        timestamp: Date.now()
                    });
                });
            }
    
            if (this.debug) {
                this.debugLog('Loaded existing users', {
                    packUsers: existingUserDids.size,
                    loadedUsers: existingUsers.size,
                    fromCache: existingUsers.size - remainingDids.length
                });
            }
    
            return existingUsers;
    
        } catch (err) {
            logger.error(`Error loading existing users: ${err.stack || err.message}`);
            throw err;
        }
    }

    /**
     * Handles a failed profile fetch/processing
     * @param {string} did - The DID of the failed profile
     * @param {string} reason - The reason for failure
     * @param {Object} changes - The changes tracking object
     */
    async handleFailedProfile(did, reason, changes) {
        try {
            logger.warn(`Failed to process profile ${did}: ${reason}`);
            
            // Track in missing profiles
            await this.checkpointManager.addMissingProfile(did, reason);
            
            // Add to changes tracker
            changes.failed.push({
                did,
                reason,
                timestamp: new Date().toISOString()
            });

            // Remove from cache if exists
            this.profileCache.cache.delete(did);

            if (this.debug) {
                this.debugLog('Profile processing failed', {
                    did,
                    reason,
                    timestamp: new Date().toISOString()
                });
            }

        } catch (err) {
            logger.error(`Error handling failed profile ${did}: ${err.stack || err.message}`);
            // Don't rethrow - we don't want to fail the whole pack for this
        }
    }

    /**
     * Handles users removed from a pack
     * @param {Object} existingPack - The existing pack data
     * @param {Array} processedUsers - Currently processed users
     * @param {Map} existingUsers - Map of existing user data
     * @param {Object} changes - Changes tracking object
     * @param {string} rkey - Pack identifier
     */
    async handleRemovedUsers(existingPack, processedUsers, existingUsers, changes, rkey) {
        try {
            const currentDids = new Set(processedUsers.map(u => u.did));
            const existingDids = new Set(existingPack.users);
    
            // Find removed DIDs
            const removedDids = [...existingDids].filter(did => !currentDids.has(did));
            
            if (removedDids.length === 0) {
                return; // Nothing to do
            }
    
            // Track changes
            changes.removed = removedDids.map(did => ({
                did,
                handle: existingUsers.get(did)?.handle || 'unknown',
                removed_at: new Date().toISOString()
            }));
    
            logger.info(`${removedDids.length} profiles were removed from pack ${rkey}`);
    
            // Update MongoDB if enabled
            if (!this.noMongoDB) {
                try {
                    const batchSize = 20; // Adjust as necessary
                    for (let i = 0; i < removedDids.length; i += batchSize) {
                        const batchDids = removedDids.slice(i, i + batchSize);
    
                        // Remove pack from users' pack_ids and update last_updated
                        await retryOnRateLimit(async () => {
                            await this.db.collection('users').updateMany(
                                { did: { $in: batchDids } },
                                {
                                    $pull: { pack_ids: rkey },
                                    $set: { last_updated: new Date() }
                                }
                            );
                        });
    
                        // Clean up users no longer in any packs
                        await retryOnRateLimit(async () => {
                            const cleanupResults = await this.db.collection('users').deleteMany({
                                did: { $in: batchDids },
                                pack_ids: { $size: 0 }
                            });
                            if (cleanupResults.deletedCount > 0) {
                                logger.info(`Removed ${cleanupResults.deletedCount} users no longer in any packs`);
                            }
                        });
                    }
    
                } catch (err) {
                    logger.error(`Error updating removed users in MongoDB: ${err.stack || err.message}`);
                    // Continue processing - MongoDB updates are not critical
                }
            }
    
            if (this.debug) {
                this.debugLog('Removed users processed', {
                    packRkey: rkey,
                    removedCount: removedDids.length,
                    timestamp: new Date().toISOString()
                });
            }
    
        } catch (err) {
            logger.error(`Error handling removed users: ${err.stack || err.message}`);
            throw err; // This is a critical operation, should fail if it errors
        }
    }
    

    /**
     * Logs changes made during pack processing
     * @param {Object} changes - The changes tracking object
     * @param {string} rkey - Pack identifier
     */
    async logChanges(changes, rkey) {
        const summary = {
            added: changes.added.length,
            removed: changes.removed.length,
            renamed: changes.renamed.length,
            updated: changes.updated.length,
            failed: changes.failed.length
        };

        // Build change report
        const changeReport = [];
        if (summary.added > 0) changeReport.push(`Added: ${summary.added}`);
        if (summary.removed > 0) changeReport.push(`Removed: ${summary.removed}`);
        if (summary.renamed > 0) changeReport.push(`Renamed: ${summary.renamed}`);
        if (summary.updated > 0) changeReport.push(`Updated: ${summary.updated}`);
        if (summary.failed > 0) changeReport.push(`Failed: ${summary.failed}`);

        // Log summary
        logger.info(`=== Pack ${rkey} changes ===`);
        logger.info(`Changes: ${changeReport.join(', ')}`);

        // Log detailed changes if any occurred
        if (summary.renamed > 0) {
            logger.info('Handle changes:', changes.renamed
                .map(u => `${u.oldHandle} -> ${u.newHandle}`)
                .join(', '));
        }

        if (summary.removed > 0) {
            logger.info('Removed users:', changes.removed
                .map(u => u.handle)
                .join(', '));
        }

        if (summary.added > 0) {
            logger.info('New users:', changes.added
                .map(u => u.handle)
                .join(', '));
        }

        if (summary.failed > 0) {
            logger.warn('Failed profiles:', changes.failed
                .map(f => `${f.did}: ${f.reason}`)
                .join(', '));
        }

        // Debug logging if enabled
        if (this.debug) {
            this.debugLog('Pack changes', {
                rkey,
                summary,
                timestamp: new Date().toISOString()
            });
        }

        // Write to checkpoint manager
        if (summary.failed > 0) {
            await this.checkpointManager.updateProgress(null, rkey, 'partial', {
                failed: summary.failed,
                total: Object.values(summary).reduce((a, b) => a + b, 0)
            });
        }
    }

    /**
     * Logs memory usage statistics
     * @param {number} startTime - Operation start timestamp
     */
    async logMemoryUsage(startTime) {
        const duration = Date.now() - startTime;
        const isLongOperation = duration > 30000; // 30 seconds

        if (isLongOperation || this.debug) {
            const used = process.memoryUsage();
            const stats = {
                duration: `${(duration / 1000).toFixed(2)}s`,
                heap: {
                    used: `${Math.round(used.heapUsed / 1024 / 1024)}MB`,
                    total: `${Math.round(used.heapTotal / 1024 / 1024)}MB`,
                    percentage: `${(used.heapUsed / used.heapTotal * 100).toFixed(1)}%`
                },
                external: `${Math.round(used.external / 1024 / 1024)}MB`,
                arrayBuffers: `${Math.round(used.arrayBuffers / 1024 / 1024)}MB`
            };

            logger.info(`Memory usage after${isLongOperation ? ' long' : ''} operation:
                Duration: ${stats.duration}
                Heap Used: ${stats.heap.used} (${stats.heap.percentage} of ${stats.heap.total})
                External: ${stats.external}
                ArrayBuffers: ${stats.arrayBuffers}`);

            if (this.debug) {
                this.debugLog('Memory stats', stats);
            }

            // Warning if heap usage is high
            const heapUsageRatio = used.heapUsed / used.heapTotal;
            if (heapUsageRatio > 0.85) {
                logger.warn(`High heap usage detected: ${(heapUsageRatio * 100).toFixed(1)}%`);
                global.gc?.(); // Request garbage collection if available
            }
        }
    }

    /**
     * Determines if an error is retryable
     * @param {Error} err - The error to check
     * @returns {boolean} - Whether the error is retryable
     */
    isRetryableError(err) {
        // Network related errors
        if (this.isNetworkError(err)) return true;
        
        // MongoDB related errors
        if (this.isMongoError(err)) return true;
        
        // Rate limiting
        if (err.status === 429) return true;
        
        // Authentication issues
        if (err.message.includes('Token has expired') ||
            err.message.includes('Authentication Required')) {
            return true;
        }
        
        // Server errors
        if (err.status >= 500 && err.status < 600) return true;
        
        // Timeout errors
        if (err.message.includes('timeout') ||
            err.message.includes('ETIMEDOUT')) {
            return true;
        }
        
        // Connection errors
        if (err.code === 'ECONNRESET' ||
            err.code === 'ECONNABORTED' ||
            err.code === 'ECONNREFUSED') {
            return true;
        }
        
        return false;
    }

    isNetworkError(err) {
        return (
            err.code === 'ECONNRESET' ||
            err.code === 'ETIMEDOUT' ||
            err.code === 'ECONNREFUSED' ||
            err.message?.includes('socket hang up') ||
            err.message?.includes('network timeout')
        );
    }

    isMongoError(err) {
        return err.name === 'MongoError' || 
               err.name === 'MongoNetworkError' ||
               err.name === 'MongoServerError';
    }

    /**
     * Processes a single starter pack.
     * Maintains existing logic while ensuring robust error handling.
     * @param {string} urlLine - The URL line in the format "handle|rkey".
     * @returns {Promise<boolean>} - True if processed successfully, else false.
     */
    async processStarterPack(urlLine) {
        const startTime = Date.now();
        let mongoConnection = false;
    
        if (this.debug) {
            this.debugLog('Starting to process pack', { 
                urlLine,
                timestamp: new Date().toISOString(),
                memory: process.memoryUsage()
            });
        }
    
        try {
            // Handle memory management at start
            const used = process.memoryUsage();
            const heapUsage = used.heapUsed / used.heapTotal;
            if (heapUsage > 0.9) {
                logger.warn(`High heap usage (${(heapUsage * 100).toFixed(1)}%), requesting garbage collection`);
                if (global.gc) {
                    global.gc();
                    await this.delay(100);
                }
            }
    
            // Initialize MongoDB if needed
            if (!this.noMongoDB) {
                await this.ensureDbConnection();
                mongoConnection = true;
            }
    
            // Basic validation
            if (!urlLine?.includes('|')) {
                logger.error(`Invalid URL line format: ${urlLine}`);
                await this.checkpointManager.updateProgress(null, 'unknown', 'error', new Error('Invalid URL format'));
                return false;
            }
    
            const [creatorHandle, rkey] = urlLine.trim().split('|').map(s => s.trim());
            
            // Early check for missing packs
            if (this.checkpointManager.isMissingPack(rkey)) {
                if (this.debug) {
                    this.debugLog('Skipping known missing pack', { rkey });
                }
                return true; // Skip without retry
            }
    
            // Track changes for reporting
            const changes = {
                renamed: [],   // Users who changed handles
                updated: [],   // Users with updated profiles
                removed: [],   // Users removed from pack
                added: [],     // Users added to pack
                failed: [],    // Failed profile fetches
                discovered: [] // New starter packs found
            };

            if (!this.stats) {
                this.stats = {
                    discoveredUsers: 0,
                    discoveredPacks: 0,
                    processedProfiles: 0,
                    updatedProfiles: 0,
                    fileOperations: {
                        success: 0,
                        failed: 0
                    }
                };
            }
        
            if (this.debug) {
                this.debugLog('Current stats before processing', {
                    stats: this.stats,
                    memory: process.memoryUsage(),
                    timestamp: new Date().toISOString()
                });
            }
    
            // Check existing state
            const existingPack = this.fileManager.getExistingPack(rkey);
            if (existingPack && !this.checkpointManager.shouldProcessPack(rkey)) {
                if (this.debug) {
                    this.debugLog('Pack already processed', { 
                        rkey,
                        lastProcessed: existingPack.updated_at
                    });
                }
                return true;
            }
    
            // Resolve creator's DID
            const creatorDID = await this.resolveHandleWithRetry(creatorHandle);
            if (!creatorDID) {
                logger.error(`Could not resolve handle: ${creatorHandle}`);
                await this.checkpointManager.addMissingPack(rkey, 'creator_not_found');
                return false;
            }
    
            // Fetch the starter pack
            const packUri = `at://${creatorDID}/app.bsky.graph.starterpack/${rkey}`;
            const pack = await this.getStarterPack(packUri);
            
            if (!pack) {
                if (this.debug) {
                    this.debugLog('Pack not found', { uri: packUri });
                }
                await this.checkpointManager.addMissingPack(rkey, 'pack_not_found');
                return false;
            }
    
            const { record } = pack;
            logger.info(`Processing pack: ${record.name} by ${creatorHandle}`);
            if (this.debug) {
                this.debugLog('Pack details', { 
                    name: record.name,
                    creator: creatorHandle,
                    record: record,
                    uri: packUri
                });
            }
    
            // Fetch list members
            const listMembers = await this.getList(record.list);
            if (!listMembers?.length) {
                if (this.debug) {
                    this.debugLog('Empty list', { 
                        uri: record.list,
                        packUri: packUri 
                    });
                }
                await this.checkpointManager.addMissingPack(rkey, 'empty_list');
                return false;
            }
    
            if (this.debug) {
                this.debugLog('Retrieved list members', {
                    count: listMembers.length,
                    listUri: record.list
                });
            }
    
            // Load existing user data for comparison
            const existingUsers = await this.loadExistingUsers(existingPack);
            
            // Process profiles
            const processedUsers = [];
            const mongodbOperations = [];
    
            for (const member of listMembers) {
                const memberDid = member.did || member.subject?.did;
                if (!memberDid) {
                    if (this.debug) {
                        this.debugLog('Invalid member data', { member });
                    }
                    continue;
                }
    
                // Skip known missing profiles
                if (this.checkpointManager.isMissingProfile(memberDid)) {
                    if (this.debug) {
                        this.debugLog('Skipping known missing profile', { did: memberDid });
                    }
                    continue;
                }
    
                try {
                    const profile = await this.getProfile(memberDid);
                    if (!profile) {
                        await this.handleFailedProfile(memberDid, 'Profile fetch failed', changes);
                        continue;
                    }
    
                    // Check if profile has associated starter packs
                    if (profile.associated?.starterPacks > 0) {
                        try {
                            const userPacks = await this.makeApiCall('getActorStarterPacks',
                                this.agent.app.bsky.graph.getActorStarterPacks({ 
                                    actor: memberDid 
                                })
                            );
                            
                            if (this.debug) {
                                this.debugLog('Retrieved associated packs', {
                                    did: memberDid,
                                    handle: profile.handle,
                                    packCount: userPacks?.data?.starterPacks?.length || 0
                                });
                            }
                            
                            if (userPacks?.data?.starterPacks) {
                                let newPacksFound = 0;
                                for (const newPack of userPacks.data.starterPacks) {
                                    try {
                                        const isNewPack = await this.trackNewStarterPack(newPack.uri, {
                                            did: memberDid,
                                            handle: profile.handle,
                                            name: newPack.name || 'Unknown',
                                            description: newPack.description
                                        });
                                        
                                        if (isNewPack) {
                                            newPacksFound++;
                                            changes.discovered.push({
                                                uri: newPack.uri,
                                                creator: profile.handle,
                                                name: newPack.name,
                                                timestamp: new Date().toISOString()
                                            });
                
                                            if (this.debug) {
                                                this.debugLog('New pack discovered and tracked', {
                                                    packUri: newPack.uri,
                                                    creator: profile.handle,
                                                    fromUser: memberDid,
                                                    filesUpdated: true
                                                });
                                            }
                                        }
                                    } catch (packWriteErr) {
                                        this.debugLog('Failed to write new pack', {
                                            uri: newPack.uri,
                                            error: packWriteErr.message,
                                            stack: packWriteErr.stack
                                        });
                                        changes.failed.push({
                                            type: 'pack_write',
                                            uri: newPack.uri,
                                            error: packWriteErr.message,
                                            timestamp: new Date().toISOString()
                                        });
                                    }
                                }
                                
                                if (newPacksFound > 0) {
                                    await this.updateDiscoveryStats(profile, newPacksFound);
                                    if (this.debug) {
                                        this.debugLog('Pack discovery batch complete', {
                                            profileDid: memberDid,
                                            newPacks: newPacksFound,
                                            totalAssociated: userPacks.data.starterPacks.length,
                                            stats: this.stats
                                        });
                                    }
                                }
                            }
                        } catch (packErr) {
                            this.debugLog('Failed to fetch user packs', {
                                did: memberDid,
                                error: packErr.message,
                                stack: packErr.stack,
                                impact: 'pack_discovery_incomplete'
                            });
                        }
                    }
    
                    // Track changes
                    const existingUser = await this.findExistingUser(memberDid);
                    if (existingUser) {
                        if (existingUser.handle !== profile.handle) {
                            changes.renamed.push({
                                did: memberDid,
                                oldHandle: existingUser.handle,
                                newHandle: profile.handle,
                                timestamp: new Date().toISOString()
                            });
                        }
                        if (existingUser.display_name !== profile.displayName) {
                            changes.updated.push({
                                did: memberDid,
                                fields: ['display_name'],
                                old: { display_name: existingUser.display_name },
                                new: { display_name: profile.displayName },
                                timestamp: new Date().toISOString()
                            });
                        }
                    } else {
                        changes.added.push({
                            did: memberDid,
                            handle: profile.handle,
                            timestamp: new Date().toISOString()
                        });
                        this.stats.discoveredUsers++;
                    }
    
                    const userData = {
                        did: memberDid,
                        handle: profile.handle,
                        display_name: profile.displayName || '',
                        followers_count: parseInt(profile.followersCount) || 0,
                        follows_count: parseInt(profile.followsCount) || 0,
                        last_updated: new Date(),
                        profile_check_needed: false
                    };
    
                    if (!existingUser || existingUser.handle !== profile.handle || 
                        existingUser.display_name !== profile.displayName) {
                        try {
                            await this.fileManager.writeUser(userData);
                            if (this.debug) {
                                this.debugLog('Buffered user data to write to files', {
                                    did: memberDid,
                                    handle: profile.handle,
                                    isNew: !existingUser,
                                    changes: existingUser ? 
                                        changes.renamed.concat(changes.updated)
                                            .filter(c => c.did === memberDid) : 
                                        ['new_user']
                                });
                            }
                        } catch (writeErr) {
                            this.debugLog('Failed to buffer user data to write to files', {
                                did: memberDid,
                                error: writeErr.message,
                                stack: writeErr.stack
                            });
                            changes.failed.push({
                                type: 'user_write',
                                did: memberDid,
                                handle: profile.handle,
                                error: writeErr.message,
                                timestamp: new Date().toISOString()
                            });
                        }
                    }

                    processedUsers.push(userData);
    
                    // Prepare MongoDB operations
                    if (!this.noMongoDB) {
                        mongodbOperations.push({
                            updateOne: {
                                filter: { did: memberDid },
                                update: {
                                    $set: userData,
                                    $addToSet: { pack_ids: rkey }
                                },
                                upsert: true
                            }
                        });
                    }
    
                } catch (err) {
                    await this.handleFailedProfile(memberDid, err.message, changes);
                }
    
                // Memory check and cleanup during processing
                if (processedUsers.length % 100 === 0) {
                    const currentHeapUsage = process.memoryUsage().heapUsed / process.memoryUsage().heapTotal;
                    if (currentHeapUsage > 0.85) {
                        if (this.debug) {
                            this.debugLog('Performing mid-processing memory cleanup', {
                                heapUsage: `${(currentHeapUsage * 100).toFixed(1)}%`,
                                processedCount: processedUsers.length
                            });
                        }
                        if (global.gc) {
                            global.gc();
                            await this.delay(100);
                        }
                    }
                }
            }
    
            // Handle removed users
            if (existingPack) {
                await this.handleRemovedUsers(
                    existingPack,
                    processedUsers,
                    existingUsers,
                    changes,
                    rkey
                );
            }
    
            // Prepare updated pack data
            const packData = {
                rkey,
                name: record.name,
                creator: creatorHandle,
                creator_did: creatorDID,
                description: record.description || '',
                user_count: processedUsers.length,
                created_at: existingPack ? existingPack.created_at : new Date(),
                updated_at: new Date(),
                users: processedUsers.map(u => u.did),
                weekly_joins: pack.joinedWeekCount || 0,
                total_joins: pack.joinedAllTimeCount || 0
            };
    
            // Update storage
            if (!this.noMongoDB && mongodbOperations.length > 0) {
                try {
                    await this.writeToMongoDB(packData, mongodbOperations);
                    if (this.debug) {
                        this.debugLog('MongoDB update complete', {
                            rkey,
                            operations: mongodbOperations.length,
                            packSize: processedUsers.length,
                            stats: {
                                newUsers: changes.added.length,
                                updatedUsers: changes.updated.length + changes.renamed.length,
                                discoveredPacks: changes.discovered.length
                            },
                            timing: {
                                duration: Date.now() - startTime,
                                timestamp: new Date().toISOString()
                            }
                        });
                    }
                } catch (err) {
                    logger.error(`MongoDB write failed for pack ${rkey}: ${err.message}`);
                    throw err;
                }
            }
    
            await this.fileManager.writePack(packData);
    
            // Log all changes
            await this.logChanges(changes, rkey);
    
            // Print memory usage in debug mode
            if (this.debug) {
                const endTime = Date.now();
                const duration = endTime - startTime;
                const endMemory = process.memoryUsage();
                this.debugLog('Processing complete', {
                    rkey,
                    duration: `${(duration / 1000).toFixed(2)}s`,
                    changes: {
                        added: changes.added.length,
                        removed: changes.removed.length,
                        renamed: changes.renamed.length,
                        updated: changes.updated.length,
                        failed: changes.failed.length,
                        discovered: changes.discovered.length
                    },
                    memory: {
                        heapUsed: `${Math.round(endMemory.heapUsed / 1024 / 1024)}MB`,
                        heapTotal: `${Math.round(endMemory.heapTotal / 1024 / 1024)}MB`,
                        external: `${Math.round(endMemory.external / 1024 / 1024)}MB`,
                        arrayBuffers: `${Math.round(endMemory.arrayBuffers / 1024 / 1024)}MB`
                    }
                });
            }
            
            if (this.debug) {
                this.debugLog('Discovery statistics', {
                    rkey,
                    stats: this.stats,
                    changes: {
                        discoveredUsers: changes.added.length,
                        discoveredPacks: changes.discovered.length,
                        updatedUsers: changes.updated.length + changes.renamed.length
                    },
                    totals: {
                        processedUsers: processedUsers.length,
                        existingUsers: existingUsers.size,
                        mongoOperations: mongodbOperations.length
                    }
                });
            }

            return true;
    
        } catch (err) {
            const errorDuration = Date.now() - startTime;
            logger.error(`Error processing pack (${errorDuration}ms): ${err.message}`);
            
            if (this.debug) {
                this.debugLog('Processing error', {
                    error: err.message,
                    stack: err.stack,
                    duration: errorDuration
                });
            }
    
            if (this.isRetryableError(err)) {
                logger.info('Retryable error detected, will retry after delay...');
                await this.delay(2000);
                return false;
            }
    
            throw err;
    
        } finally {
            await this.logMemoryUsage(startTime);
        }
    }
    
    isRetryableError(err) {
        // Network related errors
        if (err.code === 'ECONNRESET' || 
            err.code === 'ETIMEDOUT' ||
            err.code === 'ECONNREFUSED' ||
            err.message.includes('socket hang up') ||
            err.message.includes('network timeout')) {
            return true;
        }
        
        // Rate limiting
        if (err.status === 429) return true;
        
        // Authentication issues
        if (err.message.includes('Token has expired') ||
            err.message.includes('Authentication Required')) {
            return true;
        }
        
        // Server errors
        if (err.status >= 500 && err.status < 600) return true;
        
        // MongoDB errors
        if (err.name === 'MongoNetworkError' ||
            err.name === 'MongoServerError') {
            return true;
        }
        
        return false;
    }
    
    // Helper for processing individual user profiles
    async processUserProfile(profile, did, existingUsers, changes, processedUsers, mongodbOperations, rkey) {
        if (this.debug) {
            this.debugLog('Processing user profile', {
                did,
                handle: profile.handle,
                isExisting: existingUsers.has(did)
            });
        }
    
        // Check against existing users
        const existingUser = existingUsers.get(did);
        
        if (existingUser) {
            if (existingUser.handle !== profile.handle) {
                changes.renamed.push({
                    did,
                    oldHandle: existingUser.handle,
                    newHandle: profile.handle,
                    timestamp: new Date().toISOString()
                });
                if (this.debug) {
                    this.debugLog('User handle changed', {
                        did,
                        oldHandle: existingUser.handle,
                        newHandle: profile.handle
                    });
                }
            }
            if (existingUser.display_name !== profile.displayName) {
                changes.updated.push({
                    did,
                    fields: ['display_name'],
                    old: { display_name: existingUser.display_name },
                    new: { display_name: profile.displayName },
                    timestamp: new Date().toISOString()
                });
                if (this.debug) {
                    this.debugLog('User display name updated', {
                        did,
                        handle: profile.handle,
                        oldName: existingUser.display_name,
                        newName: profile.displayName
                    });
                }
            }
        } else {
            changes.added.push({ 
                did, 
                handle: profile.handle,
                timestamp: new Date().toISOString()
            });
            if (this.debug) {
                this.debugLog('New user discovered', {
                    did,
                    handle: profile.handle
                });
            }
        }
    
        // Prepare user data
        const userData = {
            did,
            handle: profile.handle,
            display_name: profile.displayName || '',
            followers_count: parseInt(profile.followersCount, 10) || 0,
            follows_count: parseInt(profile.followsCount, 10) || 0,
            last_updated: new Date(),
            profile_check_needed: false
        };
    
        // Add to processed users list
        processedUsers.push(userData);
    
        // Prepare MongoDB operation if enabled
        if (!this.noMongoDB) {
            mongodbOperations.push({
                updateOne: {
                    filter: { did },
                    update: {
                        $set: userData,
                        $addToSet: { pack_ids: rkey }
                    },
                    upsert: true
                }
            });
        }
    
        // Write to local files
        try {
            await this.fileManager.writeUser(userData);
            if (this.debug) {
                this.debugLog('Wrote user data to files', {
                    did,
                    handle: profile.handle,
                    files: ['users.json', 'users.yaml']
                });
            }
        } catch (err) {
            this.debugLog('Failed to write user data to files', {
                did,
                handle: profile.handle,
                error: err.message,
                stack: err.stack
            });
            // Don't throw - we can continue even if file write fails
            // But do log it as a change
            changes.failed.push({
                type: 'file_write',
                did,
                handle: profile.handle,
                error: err.message,
                timestamp: new Date().toISOString()
            });
        }
    
        // Update discovery stats if this is a new user
        if (!existingUser) {
            this.stats = this.stats || {
                discoveredUsers: 0,
                processedProfiles: 0
            };
            this.stats.discoveredUsers++;
            this.stats.processedProfiles++;
        }
    
        return userData;
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
    
        // Properly parse AT URI format
        const matches = uri.match(/at:\/\/(did:[^/]+)\/([^/]+)\/(.+)/);
        if (!matches) {
            logger.error(`Invalid AT URI format: ${uri}`);
            return null;
        }
    
        const [_, repo, collection, rkey] = matches;
    
        if (this.debug) {
            this.debugLog('Fetching record', {
                uri,
                parsed: { repo, collection, rkey }
            });
        }
    
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.rateLimiter.throttle();
    
                // Try first with app.bsky.graph API
                try {
                    const response = await this.apiCallWithTimeout(
                        this.agent.app.bsky.graph.getStarterPack({
                            starterPack: uri
                        })
                    );
    
                    if (response?.data?.starterPack) {
                        return {
                            uri: response.data.starterPack.uri,
                            value: response.data.starterPack.record,
                            cid: response.data.starterPack.cid
                        };
                    }
                } catch (graphErr) {
                    if (this.debug) {
                        this.debugLog('Graph API failed, falling back to repo API', {
                            error: graphErr.message
                        });
                    }
                }
    
                // Fallback to repo.getRecord
                const response = await this.apiCallWithTimeout(
                    this.agent.api.com.atproto.repo.getRecord({
                        repo,
                        collection,
                        rkey
                    })
                );
    
                const shouldRetry = await this.rateLimiter.handleResponse(response);
                if (shouldRetry && attempt < maxRetries - 1) {
                    await this.delay(Math.pow(2, attempt) * 1000);
                    continue;
                }
    
                if (!response?.data?.value) {
                    throw new Error('No value in response');
                }
    
                return response.data;
    
            } catch (err) {
                // Handle rate limits
                if (err.status === 429) {
                    const shouldRetry = await this.rateLimiter.handleResponse(err);
                    if (shouldRetry && attempt < maxRetries - 1) {
                        continue;
                    }
                    break;
                }
    
                // Handle auth issues
                if (err.message?.includes('Token has expired') || 
                    err.message?.includes('Authentication Required')) {
                    await this.refreshTokenIfNeeded(true);
                    continue;
                }
    
                // Only retry on network errors or rate limits
                if (this.isNetworkError(err)) {
                    if (attempt < maxRetries - 1) {
                        await this.delay(Math.pow(2, attempt) * 1000);
                        continue;
                    }
                }
    
                // Log error on final attempt
                if (attempt === maxRetries - 1) {
                    logger.error(`Failed to fetch record after ${maxRetries} attempts: ${uri}\nError: ${err.message}`);
                }
            }
        }
    
        return null;
    }
    
    /**
     * Resolves a handle to its DID without retrying.
     * @param {string} rawHandle - The handle to resolve.
     * @returns {Promise<string|null>} - The DID if resolved successfully; otherwise, null.
     */
    async resolveHandle(rawHandle) {
        await this.refreshTokenIfNeeded();

        try {
            if (!rawHandle) {
                throw new Error('No handle provided');
            }

            // Check if it's already a DID
            if (rawHandle.startsWith('did:')) {
                return rawHandle;
            }

            // Sanitize the handle
            const handle = this.sanitizeHandle(rawHandle);
            logger.info(`Attempting to resolve sanitized handle: ${handle}`);

            // Attempt to resolve the handle
            const response = await this.agent.resolveHandle({ handle });

            if (response?.data?.did) {
                logger.info(`Successfully resolved handle ${handle} to DID ${response.data.did}`);
                return response.data.did;
            } else {
                logger.error(`No DID found in the response for handle: ${handle}`);
                return null;
            }
        } catch (err) {
            if (err.status === 404) {
                logger.error(`Handle not found: ${rawHandle}`);
            } else if (err.message === 'Token has expired' || err.message === 'Authentication Required') {
                logger.error(`Authentication error while resolving handle ${rawHandle}: ${err.message}`);
                await this.refreshTokenIfNeeded(true); // Force token refresh
            } else {
                logger.error(`Error resolving handle ${rawHandle}: ${err.stack || err.message}`);
            }
            return null; // Indicate that the handle could not be resolved
        }
    }

    /**
     * Resolves a handle to its DID with retry mechanism
     * @param {string} rawHandle - The handle to resolve
     * @param {number} retries - Current retry count
     * @returns {Promise<string|null>} - The DID or null if failed
     */
    async resolveHandleWithRetry(rawHandle, retries = 0) {
        const MAX_RETRIES = 2;
    
        // Skip if already a DID
        if (rawHandle.startsWith('did:')) {
            return rawHandle;
        }
    
        const handle = this.sanitizeHandle(rawHandle);
        if (this.debug) {
            this.debugLog('Resolving handle', { handle, retries });
        }
    
        const response = await this.makeApiCall('resolveHandle',
            this.agent.resolveHandle({ handle }),
            { maxRetries: MAX_RETRIES }
        );
    
        return response?.data?.did || null;
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
                this.mongoClient = new MongoClient(process.env.MONGODB_URI, {
                    ...this.dbConfig,
                    maxPoolSize: 10,
                    minPoolSize: 5,
                    maxIdleTimeMS: 120000,
                    waitQueueTimeoutMS: 30000
                });
                
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
        const startTime = Date.now();
        
        if (this.debug) {
            this.debugLog('API timeout wrapper', {
                timeout,
                startTime: new Date(startTime).toISOString()
            });
        }
    
        let timeoutId;
        let isCompleted = false;
        
        try {
            const timeoutPromise = new Promise((_, reject) => {
                timeoutId = setTimeout(() => {
                    const duration = Date.now() - startTime;
                    if (this.debug) {
                        this.debugLog('API call timeout', { 
                            duration,
                            timeout 
                        });
                    }
                    reject(new Error(`API call timed out after ${duration}ms`));
                }, timeout);
            });
    
            const result = await Promise.race([promise, timeoutPromise]);
            isCompleted = true;
            
            if (this.debug) {
                this.debugLog('API call complete', {
                    duration: Date.now() - startTime
                });
            }
    
            return result;
    
        } catch (err) {
            if (this.debug) {
                this.debugLog('API call failed', {
                    error: err.message,
                    duration: Date.now() - startTime,
                    stack: err.stack
                });
            }
            throw err;
        } finally {
            if (timeoutId) clearTimeout(timeoutId);
            if (!isCompleted && this.debug) {
                this.debugLog('API call cleaned up', {
                    duration: Date.now() - startTime
                });
            }
        }
    }

    async processUrls(filename) {
        try {
            logger.info('Starting URL processing...');
            const startTime = Date.now();
            let lastStatusReport = startTime;
            
            // Initialize processing state
            let startIndex = Math.max(0, this.checkpointManager.getLastProcessedIndex() + 1);
            const todayStats = this.checkpointManager.getDailyStats();
            logger.info(`Today's progress: ${JSON.stringify(todayStats, null, 2)}`);
    
            // Read and process file in smaller chunks
            logger.info(`Reading ${filename}...`);
            let fileContent = await fs.promises.readFile(filename, 'utf-8');
            
            // Split into lines and filter valid ones
            logger.info('Processing file content...');
            const lines = fileContent
                .split('\n')
                .map(line => line.trim())
                .filter(line => line && line.includes('|'));
            
            // Clear the original content to free memory
            fileContent = null;
            if (global.gc) global.gc();
    
            const totalUrls = lines.length;
            logger.info(`Found ${totalUrls} total URLs to process`);
            logger.info(`Starting from index ${startIndex}`);
    
            // Process in smaller chunks
            const CHUNK_SIZE = 50; // Reduced chunk size
            let processedCount = 0;
            let currentChunk = [];
    
            for (let i = startIndex; i < lines.length; i++) {
                // Memory check and cleanup
                if (i % 10 === 0) {
                    const memUsage = process.memoryUsage();
                    const heapUsage = memUsage.heapUsed / memUsage.heapTotal;
                    
                    if (heapUsage > 0.85) {
                        logger.info(`Memory cleanup at ${(heapUsage * 100).toFixed(1)}% heap usage`);
                        this.profileCache.clear();
                        if (global.gc) {
                            global.gc();
                            await this.delay(100);
                        }
                    }
                }
    
                const line = lines[i];
                const [_, rkey] = line.split('|').map(s => s.trim());
    
                if (!this.checkpointManager.shouldProcessPack(rkey)) {
                    processedCount++;
                    continue;
                }
    
                currentChunk.push({ index: i, line });
    
                if (currentChunk.length >= CHUNK_SIZE) {
                    await this.processUrlChunk(currentChunk, totalUrls, processedCount);
                    processedCount += currentChunk.length;
                    currentChunk = [];
    
                    // Force cleanup after each chunk
                    this.profileCache.clear();
                    if (global.gc) {
                        global.gc();
                        await this.delay(200);
                    }
    
                    // Status report
                    const now = Date.now();
                    if (now - lastStatusReport > 300000) { // Every 5 minutes
                        await this.reportProcessingStatus(startTime, processedCount, totalUrls, todayStats);
                        lastStatusReport = now;
                    }
                }
            }
    
            // Process remaining items
            if (currentChunk.length > 0) {
                await this.processUrlChunk(currentChunk, totalUrls, processedCount);
            }
    
            logger.info('URL processing completed successfully');
    
        } catch (err) {
            logger.error(`Error processing URLs: ${err.stack || err.message}`);
            throw err;
        }
    }
    
    async processUrlChunk(chunk, totalUrls, processedSoFar) {
        logger.info(`Processing chunk of ${chunk.length} items (${processedSoFar + 1}-${processedSoFar + chunk.length}/${totalUrls})`);
    
        for (const {index, line} of chunk) {
            const [handle, rkey] = line.split('|').map(s => s.trim());
            
            try {
                const success = await this.processStarterPack(line);
                if (success !== false) {
                    await this.checkpointManager.updateProgress(index, rkey, 'success');
                } else {
                    await this.checkpointManager.updateProgress(index, rkey, 'skipped');
                }
    
                // Rate limiting delays
                if ((processedSoFar + chunk.indexOf({index, line})) % 10 === 0) {
                    await this.delay(1000);
                }
            } catch (err) {
                if (err.status === 429) {
                    logger.warn(`Rate limit reached at index ${index}. Saving progress...`);
                    await this.checkpointManager.updateProgress(index, rkey, 'rateLimit', err);
                    throw err; // Let the main process handle rate limits
                }
                
                logger.error(`Error processing ${rkey}: ${err.message}`);
                await this.checkpointManager.updateProgress(index, rkey, 'error', err);
                await this.delay(2000); // Add delay after errors
            }
        }
    }
    
    async reportProcessingStatus(startTime, processed, total, todayStats) {
        const now = Date.now();
        const elapsed = (now - startTime) / 1000;
        const rate = processed / (elapsed / 60);
        const remaining = total - processed;
        const estimatedTimeLeft = remaining / rate;
        const memUsage = process.memoryUsage();
        
        const statusReport = `
    Processing Status:
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    Progress   : ${processed}/${total} (${(processed/total*100).toFixed(2)}%)
    Rate       : ${rate.toFixed(2)} packs/minute
    Time Left  : ${(estimatedTimeLeft/60).toFixed(2)} hours
    Success    : ${todayStats ? (todayStats.successful/todayStats.processed*100).toFixed(2) : 0}%
    Rate Limits: ${todayStats?.rateLimitHits || 0}
    Errors     : ${todayStats?.errors || 0}
    Memory     : ${(memUsage.heapUsed/1024/1024).toFixed(1)}MB / ${(memUsage.heapTotal/1024/1024).toFixed(1)}MB
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`;
    
        logger.info(statusReport);
    }

    async cleanup() {
        const errors = [];
        let cleanupTimeout;
        
        try {
            logger.info('Starting cleanup process...');
            
            // Add timeout for cleanup
            cleanupTimeout = setTimeout(() => {
                logger.error('Cleanup timeout - forcing exit');
                process.exit(1);
            }, 120000); // 120 second timeout
    
            // First save any pending checkpoints
            if (this.checkpointManager) {
                try {
                    await this.checkpointManager.saveCheckpoints(true)
                        .catch(err => {
                            logger.error(`Error saving checkpoints: ${err.message}`);
                            errors.push(['checkpoint_save', err]);
                        });
                    
                    await this.checkpointManager.cleanup()
                        .catch(err => {
                            logger.error(`Error cleaning up checkpoints: ${err.message}`);
                            errors.push(['checkpoint_cleanup', err]);
                        });
                } catch (err) {
                    errors.push(['checkpoint', err]);
                }
            }
    
            // Clear profile cache
            try {
                if (this.profileCache) {
                    this.profileCache.clear();
                }
            } catch (err) {
                errors.push(['cache', err]);
            }
    
            // Clean up file manager and its resources
            if (this.fileManager) {
                try {
                    await this.fileManager.cleanup()
                        .catch(err => {
                            logger.error(`Error cleaning up file manager: ${err.message}`);
                            errors.push(['fileManager', err]);
                        });
                } catch (err) {
                    errors.push(['fileManager', err]);
                }
            }
    
            // Clean up MongoDB resources
            if (this.mongoClient) {
                try {
                    // Ensure all pending operations are complete
                    if (this.db && !this.noMongoDB) {
                        try {
                            await this.db.command({ fsync: 1 })
                                .catch(err => logger.warn(`Fsync warning: ${err.message}`));
                        } catch (fsyncErr) {
                            logger.warn(`Fsync error: ${fsyncErr.message}`);
                        }
                    }
    
                    // Close MongoDB connection
                    try {
                        await this.mongoClient.close(true) // Force close
                            .catch(err => {
                                logger.error(`Error closing MongoDB connection: ${err.message}`);
                                errors.push(['mongodb_close', err]);
                            });
                    } catch (closeErr) {
                        errors.push(['mongodb', closeErr]);
                    }
                } catch (err) {
                    errors.push(['mongodb', err]);
                }
            }
    
            // Clear any remaining timeouts
            if (this.rateLimiter?.saveIntervalId) {
                clearInterval(this.rateLimiter.saveIntervalId);
            }
    
            // Force garbage collection if available
            if (global.gc) {
                try {
                    global.gc();
                } catch (gcErr) {
                    logger.warn(`GC error: ${gcErr.message}`);
                }
            }
    
            // Clean up temporary files
            try {
                const tempFiles = await fs.promises.readdir('.')
                    .then(files => files.filter(f => 
                        f.endsWith('.tmp') || 
                        f.endsWith('.temp') || 
                        f.endsWith('.lock')
                    ))
                    .catch(() => []);
                    
                for (const file of tempFiles) {
                    await fs.promises.unlink(file)
                        .catch(err => logger.warn(`Error deleting temp file ${file}: ${err.message}`));
                }
            } catch (tempErr) {
                logger.error(`Error cleaning temporary files: ${tempErr.message}`);
                errors.push(['tempfiles', tempErr]);
            }
    
            // Handle any errors that occurred during cleanup
            if (errors.length > 0) {
                const errorMsg = errors
                    .map(([type, err]) => `${type}: ${err.message}`)
                    .join('; ');
                
                logger.error(`Cleanup completed with errors: ${errorMsg}`);
                
                // Only throw if we have errors other than fsync warnings
                const criticalErrors = errors.filter(([type]) => 
                    !['fsync_warning'].includes(type));
                
                if (criticalErrors.length > 0) {
                    throw new AggregateError(
                        errors.map(([,err]) => err),
                        'Multiple cleanup errors occurred'
                    );
                }
            }
    
            logger.info('Cleanup completed successfully');
    
        } catch (err) {
            logger.error(`Critical error during cleanup: ${err.stack || err.message}`);
            throw err;
        } finally {
            // Clear timeout
            if (cleanupTimeout) {
                clearTimeout(cleanupTimeout);
            }
    
            // Final emergency cleanup of file handles
            try {
                const emergencyFiles = await fs.promises.readdir('.')
                    .then(files => files.filter(f => 
                        f.endsWith('.tmp') || 
                        f.endsWith('.temp') || 
                        f.endsWith('.lock')
                    ))
                    .catch(() => []);
                    
                for (const file of emergencyFiles) {
                    await fs.promises.unlink(file).catch(() => {});
                }
            } catch (err) {
                logger.error(`Emergency cleanup failed: ${err.message}`);
            }
    
            // Clear all class-level references
            this.db = null;
            this.profileCache = null;
            this.rateLimiter = null;
        }
    }

    async processUserQuick(identifier) {
        try {
            await this.initMinimal();
            
            // Handle different identifier types
            let did;
            if (identifier.startsWith('did:')) {
                did = identifier;
            } else if (identifier.startsWith('at://')) {
                did = identifier.split('/')[2];
            } else {
                did = await this.resolveHandleWithRetry(identifier);
            }
    
            if (!did) {
                throw new Error(`Could not resolve identifier: ${identifier}`);
            }
    
            logger.info(`Processing user: ${did}`);
            
            // Get user profile
            const profile = await this.getProfile(did, true); // Force update
            if (!profile) {
                throw new Error(`Could not fetch profile for ${did}`);
            }
    
            // Process profile
            const userData = {
                did: profile.did,
                handle: profile.handle,
                display_name: profile.displayName || '',
                followers_count: parseInt(profile.followersCount, 10) || 0,
                follows_count: parseInt(profile.followsCount, 10) || 0,
                last_updated: new Date(),
                profile_check_needed: false
            };
    
            // Update MongoDB if enabled
            if (!this.noMongoDB) {
                await this.db.collection('users').updateOne(
                    { did: userData.did },
                    { $set: userData },
                    { upsert: true }
                );
            }
    
            // Write to files
            await this.fileManager.writeUser(userData);
    
            logger.info(`Successfully processed user ${profile.handle} (${did})`);
            return userData;
    
        } catch (err) {
            logger.error(`Error processing user ${identifier}: ${err.stack || err.message}`);
            throw err;
        } finally {
            await this.cleanup();
        }
    }
    
    async processStarterPackQuick(identifier) {
        try {
            await this.initMinimal();
    
            // Handle different identifier formats
            let uri;
            if (identifier.startsWith('at://')) {
                uri = identifier;
            } else {
                // Try to construct URI from rkey
                const creatorDID = await this.resolveHandleWithRetry(process.env.BSKY_USERNAME);
                if (!creatorDID) {
                    throw new Error('Could not resolve agent DID');
                }
                uri = `at://${creatorDID}/app.bsky.graph.starterpack/${identifier}`;
            }
    
            logger.info(`Processing starter pack: ${uri}`);
    
            // Get pack data
            const pack = await this.getStarterPack(uri);
            if (!pack) {
                throw new Error(`Could not fetch starter pack: ${uri}`);
            }
    
            const { record } = pack;
            const rkey = uri.split('/').pop();
    
            // Get creator details
            const creatorDID = uri.split('/')[2];
            const creatorProfile = await this.getProfile(creatorDID);
            if (!creatorProfile) {
                throw new Error(`Could not fetch creator profile: ${creatorDID}`);
            }
    
            // Get pack stats
            const stats = await this.getStarterPackStats(uri);
    
            // Get members
            const members = await this.getListMembers(record.list);
            if (!members) {
                throw new Error(`Could not fetch list members: ${record.list}`);
            }
    
            // Process members
            const processedUsers = [];
            for (const member of members.items) {
                try {
                    const profile = await this.getProfile(member.did);
                    if (profile) {
                        processedUsers.push(profile.did);
                    }
                } catch (err) {
                    logger.warn(`Could not process member ${member.did}: ${err.message}`);
                }
            }
    
            // Prepare pack data
            const packData = {
                rkey,
                name: record.name,
                creator: creatorProfile.handle,
                creator_did: creatorDID,
                description: record.description || '',
                user_count: processedUsers.length,
                created_at: new Date(),
                updated_at: new Date(),
                users: processedUsers,
                weekly_joins: stats.weekly_joins,
                total_joins: stats.total_joins
            };
    
            // Update MongoDB if enabled
            if (!this.noMongoDB) {
                await this.writeToMongoDB(packData, processedUsers.map(did => ({
                    updateOne: {
                        filter: { did },
                        update: { $addToSet: { pack_ids: rkey } },
                        upsert: true
                    }
                })));
            }
    
            // Write to files
            await this.fileManager.writePack(packData);
    
            logger.info(`Successfully processed starter pack ${record.name} (${rkey})`);
            return packData;
    
        } catch (err) {
            logger.error(`Error processing starter pack ${identifier}: ${err.stack || err.message}`);
            throw err;
        } finally {
            await this.cleanup();
        }
    }

    async initMinimal() {
        if (this.isInitialized) return;
    
        try {
            // We always need these for API access
            const requiredVars = ['BSKY_USERNAME', 'BSKY_PASSWORD'];
            if (!this.noMongoDB) {
                requiredVars.push('MONGODB_URI');
            }
            
            const missingVars = requiredVars.filter(varName => !process.env[varName]);
            if (missingVars.length > 0) {
                throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
            }
    
            // Setup minimal MongoDB if needed
            if (!this.noMongoDB) {
                await this.setupDatabase();
            }
    
            // Initialize only essential managers
            this.fileManager = new FileManager(this);
            await this.fileManager.init();
    
            // Setup Bluesky agent
            await this.setupAgent();
    
            this.isInitialized = true;
            
            // Register cleanup handlers
            if (!this.noMongoDB && this.mongoClient) {
                this.resourceManager.register(this.mongoClient);
            }
            this.resourceManager.register(this.fileManager);
    
        } catch (err) {
            logger.error(`Minimal initialization failed: ${err.stack || err.message}`);
            throw err;
        }
    }
    
    async init() {
        if (this.isInitialized) return;
    
        try {
            // Validate required files exist only if not in --fromapi mode
            if (!this.fromApi) {
                await fs.promises.access('starter_pack_urls.txt')
                    .catch(() => {
                        throw new Error('Required file starter_pack_urls.txt not found');
                    });
            }
            
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
    
                // **Rearranged Order: Establish MongoDB Connection First**
                if (!this.noMongoDB) {
                    await this.setupDatabase(); // Connect and set this.db
                }
    
                await this.fileManager.init(); // Now, this.db is set
    
                if (!this.checkpointManager) {
                    throw new Error('Failed to initialize checkpoint manager');
                }
                await this.checkpointManager.init();
    
                await this.setupAgent();
            }
            
            this.isInitialized = true;
    
            if (false) {
                const memoryInterval = setInterval(() => {
                    const used = process.memoryUsage();
                    this.debugLog('Memory usage', {
                        heapUsed: `${Math.round(used.heapUsed / 1024 / 1024)}MB`,
                        heapTotal: `${Math.round(used.heapTotal / 1024 / 1024)}MB`,
                        external: `${Math.round(used.external / 1024 / 1024)}MB`,
                        arrayBuffers: `${Math.round(used.arrayBuffers / 1024 / 1024)}MB`
                    });
                }, 5 * 60 * 1000);  // Every 5 minutes
                
                // Register for cleanup
                this.resourceManager.register({ 
                    cleanup: () => clearInterval(memoryInterval) 
                });
            }

            // Register MongoDB connection
            if (!this.noMongoDB && this.mongoClient) {
                this.resourceManager.register(this.mongoClient);
            }

            // Register FileManager streams
            this.resourceManager.register(this.fileManager.jsonStream);
            this.resourceManager.register(this.fileManager.yamlStream);
    
        } catch (err) {
            logger.error(`Initialization failed: ${err.stack || err.message}`);
            throw err;
        }
    }
    
    /**
     * Overrides the existing `collect` method to utilize the new pipeline when `--fromapi` is enabled.
     */
    async collect() {
        await this.init();
        
        if (this.fromApi) {
            try {
                logger.info('Starting collection from API...');
                await this.collectAllStarterPacks();
            } catch (err) {
                logger.error(`API collection error: ${err.message}`);
                throw err;
            }
        } else {
            // Original URL-based collection
            await this.processUrls('starter_pack_urls.txt');
        }
        
        await this.cleanup();
    }

    async delay(ms) {
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                clearTimeout(timeout);
                resolve();
            }, ms);
            
            // Handle potential errors
            timeout.unref(); // Don't keep process alive just for this timeout
        });
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

async function withRetry(operation, maxRetries = 3) {
    let lastError;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            return await operation();
        } catch (err) {
            lastError = err;
            if (err.hasErrorLabel('TransientTransactionError') ||
                err.hasErrorLabel('UnknownTransactionCommitResult')) {
                await new Promise(resolve => 
                    setTimeout(resolve, Math.pow(2, attempt) * 1000));
                continue;
            }
            throw err;
        }
    }
    throw lastError;
}

async function main() {
    globalProcessor = new StarterPackProcessor();
    
    const args = process.argv.slice(2);
    
    // Handle quick processing modes
    if (args.includes('--adduser')) {
        const index = args.indexOf('--adduser');
        const identifier = args[index + 1];
        if (!identifier) {
            console.error('Error: --adduser requires an identifier (handle, DID, or URI)');
            process.exit(1);
        }
        try {
            await globalProcessor.processUserQuick(identifier);
            process.exit(0);
        } catch (err) {
            logger.error('Error processing user:', err);
            process.exit(1);
        }
    }
    
    if (args.includes('--addstarterpack')) {
        const index = args.indexOf('--addstarterpack');
        const identifier = args[index + 1];
        if (!identifier) {
            console.error('Error: --addstarterpack requires an identifier (URI or rkey)');
            process.exit(1);
        }
        try {
            await globalProcessor.processStarterPackQuick(identifier);
            process.exit(0);
        } catch (err) {
            logger.error('Error processing starter pack:', err);
            process.exit(1);
        }
    }
    
    // Handle purge before any initialization
    if (args.includes('--purge')) {
        await globalProcessor.init();
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
            await updateMongoDBFromFiles();
        } else {
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
