import * as dotenv from 'dotenv';
import { BskyAgent } from '@atproto/api';
import { MongoClient } from 'mongodb';
import fs from 'fs';
import yaml from 'js-yaml';
import winston from 'winston';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { promises as fsPromises } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config();

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

class CheckpointManager {
    constructor(filename = 'checkpoints.json') {
        this.filename = filename;
        this.checkpoints = null; // Will be initialized in init()
    }

    async init() {
        this.checkpoints = await this.loadCheckpoints();
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
                logger.error(`Error loading checkpoints: ${err.message}`);
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
            }
            return false;
        }
    }

    async releaseLock() {
        const lockFile = `${this.filename}.lock`;
        try {
            await fsPromises.unlink(lockFile);
        } catch (err) {
            logger.warn(`Error releasing lock: ${err.message}`);
        }
    }

    async saveCheckpoints() {
        if (await this.acquireLock()) {
            try {
                const checkpointsToSave = {
                    ...this.checkpoints,
                    completedPacks: Array.from(this.checkpoints.completedPacks)
                };
                await fs.promises.writeFile(
                    this.filename, 
                    JSON.stringify(checkpointsToSave, null, 2)
                );
                logger.info('Checkpoints saved successfully');
            } catch (err) {
                logger.error(`Error saving checkpoints: ${err.message}`);
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
        this.agent = new BskyAgent({ service: 'https://bsky.social' });
        this.mongoClient = new MongoClient(process.env.MONGODB_URI);
        this.db = null;
    
        this.rateLimiter = new BlueSkyRateLimiter();
        this.lastTokenRefresh = Date.now();
        this.tokenRefreshInterval = 45 * 60 * 1000;
        this.checkpointManager = new CheckpointManager();
        this.isInitialized = false;
    
        // First setup streams
        this.setupStreams();
    }

    async setupStreams() {
        try {
            this.jsonStream = fs.createWriteStream('starter_packs.json', { flags: 'w' });
            this.yamlStream = fs.createWriteStream('starter_packs.yaml', { flags: 'w' });
            this.isFirstJsonEntry = true;
        
            // Set up error handlers immediately
            this.jsonStream.on('error', async (err) => {
                logger.error(`JSON stream error: ${err.message}`);
                await this.cleanup();
            });
        
            this.yamlStream.on('error', async (err) => {
                logger.error(`YAML stream error: ${err.message}`);
                await this.cleanup();
            });
        
            // Write initial JSON array opening bracket
            await new Promise((resolve, reject) => {
                this.jsonStream.write('[\n', (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
        } catch (err) {
            // Clean up streams if initialization fails
            if (this.jsonStream) this.jsonStream.destroy();
            if (this.yamlStream) this.yamlStream.destroy();
            logger.error(`Error setting up streams: ${err.message}`);
            throw err;
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
                    logger.error(`Token refresh attempt ${retryCount}/${maxRetries} failed: ${err.message}`);
                    
                    if (retryCount === maxRetries) {
                        throw new Error(`Failed to refresh token after ${maxRetries} attempts: ${err.message}`);
                    }
                    
                    // Exponential backoff
                    await new Promise(resolve => setTimeout(resolve, Math.pow(2, retryCount) * 1000));
                }
            }
        }
    }

    async setupDatabase() {
        try {
            await this.mongoClient.connect();
            this.db = this.mongoClient.db('starterpacks');
            await this.db.collection('users').createIndex({ did: 1, pack_id: 1 }, { unique: true });
            await this.db.collection('users').createIndex({ handle: 1 });
            await this.db.collection('users').createIndex({ pack_id: 1 });
            logger.info('Connected to MongoDB and indexes created.');
        } catch (err) {
            logger.error(`Error setting up the database: ${err.message}`);
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
            logger.error(`Error authenticating BskyAgent: ${err.message}`);
            process.exit(1);
        }
    }

    /**
     * Sanitize handle before resolution
     * @param {string} handle - The handle to sanitize
     * @returns {string} - Sanitized handle
     */
    sanitizeHandle(handle) {
        // Remove any trailing .bsky.social if present
        handle = handle.replace(/\.bsky\.social$/, '');

        // Remove any protocol prefixes if present
        handle = handle.replace(/^(http:\/\/|https:\/\/)/, '');

        // Remove any trailing periods
        handle = handle.replace(/\.$/, '');

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
                logger.error(`Error fetching list page: ${err.message}`);
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
                    items: members,
                    cursor: response.data.cursor
                };
    
            } catch (err) {
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
    
                logger.error(`Error getting list members for ${uri}: ${err.message}`);
                if (err.message.includes('timeout')) {
                    logger.error('Request timed out, will retry');
                    return null;
                }
            }
        }
        return null;
    }

    async getProfile(did) {
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
                
                const shouldRetry = await this.rateLimiter.handleResponse(response);
                if (shouldRetry && attempt < maxRetries - 1) {
                    continue;
                }
                
                if (response?.data) {
                    logger.info(`Profile found for ${did}: ${response.data.handle}`);
                    return response.data;
                }
            } catch (err) {
                if (err.status === 429 && attempt < maxRetries - 1) {
                    const shouldRetry = await this.rateLimiter.handleResponse(err);
                    if (shouldRetry) continue;
                }
                logger.error(`Error getting profile for ${did}: ${err.message}`);
            }
        }
        return null;
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
                logger.error(`Error getting ${uri}: ${err.message}`);
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
    
            logger.error(`Failed to resolve handle ${rawHandle} after ${MAX_RETRIES} attempts: ${err.message}`);
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
                logger.warn(`MongoDB connection attempt ${retryCount}/${maxRetries} failed: ${err.message}`);
                
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
        await this.ensureDbConnection();
        try {
            // Validate and clean the input line
            if (!urlLine || !urlLine.includes('|')) {
                logger.error(`Invalid URL line format: ${urlLine}`);
                return;
            }

            const [creatorHandle, rkey] = urlLine.trim().split('|').map(s => s.trim());
            if (!creatorHandle || !rkey) {
                logger.error(`Missing handle or rkey in line: ${urlLine}`);
                return;
            }

            logger.info(`Processing pack by ${creatorHandle}: ${rkey}`);

            // Resolve creatorHandle to DID with retry
            let creatorDID = await this.resolveHandleWithRetry(creatorHandle);
            if (!creatorDID) {
                logger.error(`Skipping pack ${rkey} due to unresolved DID.`);
                return;
            }

            const recordUri = `at://${creatorDID}/app.bsky.graph.starterpack/${rkey}`;
            const record = await this.getRecord(recordUri);
            if (!record) {
                logger.error(`Could not fetch starter pack record: ${recordUri}`);
                return;
            }

            const { value } = record;
            logger.info(`Pack name: ${value.name}`);
            logger.info(`Description: ${value.description}`);
            logger.info(`List URI: ${value.list}`);

            // Get list members using our method
            const listMembers = await this.getAllListMembers(value.list);  // Changed from getListMembers
            
            if (!listMembers || listMembers.length === 0) {
                logger.error(`No members found in the list for pack ${rkey}`);
                return;
            }

            logger.info(`Processing ${listMembers.length} users...`);
            const usersCollection = this.db.collection('users');

            const profilePromises = listMembers.map(async (member) => {
                // Debug log the member structure
                logger.info(`Processing member: ${JSON.stringify(member, null, 2)}`);

                // Extract the DID from the member object
                const memberDid = member.did || (member.subject ? member.subject.did : null);
                
                if (!memberDid) {
                    logger.error(`Could not extract DID from member: ${JSON.stringify(member)}`);
                    return null;
                }

                const profile = await this.getProfile(memberDid);
                if (profile) {
                    return {
                        updateOne: {
                            filter: { did: memberDid, pack_id: rkey },
                            update: {
                                $set: {
                                    did: memberDid,
                                    handle: profile.handle,
                                    display_name: profile.displayName,
                                    pack_id: rkey,
                                    pack_name: value.name,
                                    pack_creator: creatorHandle,
                                    added_at: new Date()
                                }
                            },
                            upsert: true
                        }
                    };
                }
                return null;
            });

            try {
                const operations = (await Promise.all(profilePromises)).filter(op => op !== null);
                
                if (operations.length > 0) {
                    const result = await usersCollection.bulkWrite(operations);
                    logger.info(`Saved ${operations.length} users for pack ${rkey}`);
                    logger.info(`MongoDB result: ${JSON.stringify(result.result || result, null, 2)}`);

                    const packData = await usersCollection.aggregate([
                        { $match: { pack_id: rkey } },
                        {
                            $group: {
                                _id: { pack_id: "$pack_id", pack_name: "$pack_name", pack_creator: "$pack_creator" },
                                user_count: { $sum: 1 },
                                users: { 
                                    $push: { 
                                        handle: "$handle", 
                                        display_name: "$display_name",
                                        did: "$did"
                                    }
                                }
                            }
                        }
                    ]).toArray();

                    if (packData.length > 0) {
                        const pack = packData[0];
                        const outputEntry = {
                            name: pack._id.pack_name,
                            creator: pack._id.pack_creator,
                            rkey: pack._id.pack_id,
                            url: `https://bsky.app/profile/${creatorHandle}/lists/${rkey}`,
                            user_count: pack.user_count,
                            users: pack.users.map(user => ({
                                handle: user.handle,
                                display_name: user.display_name || '',
                                did: user.did
                            }))
                        };

                        // Write to JSON stream
                        if (!this.isFirstJsonEntry) {
                            this.jsonStream.write(',\n');
                        } else {
                            this.isFirstJsonEntry = false;
                        }
                        this.jsonStream.write(JSON.stringify(outputEntry, null, 2));

                        // Write to YAML stream
                        this.yamlStream.write('---\n');
                        this.yamlStream.write(yaml.dump(outputEntry));
                    }
                } else {
                    logger.error(`No valid operations generated for pack ${rkey}`);
                }
            } catch (err) {
                logger.error(`Error saving users for pack ${rkey}: ${err.message}`);
            }
        } catch (err) {
            logger.error(`Error processing pack: ${err.message}`);
            if (err.code === 'ECONNRESET' || err.message.includes('socket hang up')) {
                logger.info('Network error detected, waiting before retry...');
                await this.delay(5000);
                return false; // Signal retry needed
            }
            throw err;
        }
    }

    async writeToStreams(outputEntry) {
        try {
            // Wait for JSON stream to be ready
            await this.jsonStreamReady;
    
            // Write to streams
            await Promise.all([
                new Promise((resolve, reject) => {
                    this.jsonStream.write(
                        this.isFirstJsonEntry ? 
                            JSON.stringify(outputEntry, null, 2) :
                            ',\n' + JSON.stringify(outputEntry, null, 2),
                        err => err ? reject(err) : resolve()
                    );
                }),
                new Promise((resolve, reject) => {
                    this.yamlStream.write(
                        '---\n' + yaml.dump(outputEntry),
                        err => err ? reject(err) : resolve()
                    );
                })
            ]);
    
            if (this.isFirstJsonEntry) {
                this.isFirstJsonEntry = false;
            }
        } catch (err) {
            logger.error(`Error writing to streams: ${err.message}`);
            throw err;
        }
    }

    async exportData(format = 'json') {
        try {
            if (format === 'json') {
                await new Promise(resolve => this.jsonStream.write('\n]\n', resolve));
                await new Promise(resolve => this.jsonStream.end(resolve));
            } else if (format === 'yaml') {
                await new Promise(resolve => this.yamlStream.end(resolve));
            }
            logger.info(`Exported data to starter_packs.${format}`);
        } catch (err) {
            logger.error(`Error exporting ${format} data: ${err.message}`);
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
                        } else {
                            logger.error(`Error processing pack ${rkey}: ${err.message}`);
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
            logger.error(`Error reading file ${filename}: ${err.message}`);
            throw err;
        }
    }

    async cleanup() {
        try {
            await this.exportData('json');
            await this.exportData('yaml');
            if (this.mongoClient) {
                await this.mongoClient.close();
            }
        } catch (err) {
            logger.error(`Error during cleanup: ${err.message}`);
        }
    }

    async init() {
        if (!this.isInitialized) {
            // Check required environment variables
            const requiredEnvVars = [
                'MONGODB_URI',
                'BSKY_USERNAME',
                'BSKY_PASSWORD'
            ];
            
            const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);
            if (missingVars.length > 0) {
                throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
            }
    
            await this.checkpointManager.init();
            await this.setupDatabase();
            await this.setupAgent();
            this.isInitialized = true;
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
    logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
    if (globalProcessor) {
        await globalProcessor.cleanup();
    }
    process.exit(1);
});

process.on('uncaughtException', async (error) => {
    logger.error('Uncaught Exception:', error);
    if (globalProcessor) {
        await globalProcessor.cleanup();
    }
    process.exit(1);
});

// Create a global reference
let globalProcessor = null;

async function main() {
    globalProcessor = new StarterPackProcessor();
    
    process.on('SIGINT', async () => {
        logger.info('Received SIGINT. Cleaning up...');
        await globalProcessor.cleanup();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        logger.info('Received SIGTERM. Cleaning up...');
        await globalProcessor.cleanup();
        process.exit(0);
    });

    try {
        await globalProcessor.collect();
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
