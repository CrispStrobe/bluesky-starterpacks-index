require('dotenv').config();
const { BskyAgent } = require('@atproto/api');
const { MongoClient } = require('mongodb');
const fs = require('fs');
const yaml = require('js-yaml');
const winston = require('winston');

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
        this.checkpoints = this.loadCheckpoints();
    }

    loadCheckpoints() {
        try {
            if (fs.existsSync(this.filename)) {
                const data = JSON.parse(fs.readFileSync(this.filename, 'utf-8'));
                logger.info(`Loaded checkpoints: ${JSON.stringify(data, null, 2)}`);
                return data;
            }
        } catch (err) {
            logger.error(`Error loading checkpoints: ${err.message}`);
        }
        return {
            lastProcessedIndex: -1,
            lastProcessedDate: null,
            dailyStats: {},
            errors: [],
            completedPacks: new Set(),
            rateLimitHits: [],
        };
    }

    saveCheckpoints() {
        try {
            // Convert Set to Array for JSON serialization
            const checkpointsToSave = {
                ...this.checkpoints,
                completedPacks: Array.from(this.checkpoints.completedPacks)
            };
            fs.writeFileSync(this.filename, JSON.stringify(checkpointsToSave, null, 2));
            logger.info('Checkpoints saved successfully');
        } catch (err) {
            logger.error(`Error saving checkpoints: ${err.message}`);
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

        // Initialize write streams for incremental writing
        this.jsonStream = fs.createWriteStream('starter_packs.json', { flags: 'w' });
        this.yamlStream = fs.createWriteStream('starter_packs.yaml', { flags: 'w' });

        // Initialize JSON array
        this.jsonStream.write('[\n');
        this.isFirstJsonEntry = true;

        this.rateLimiter = new BlueSkyRateLimiter();
        this.checkpointManager = new CheckpointManager();
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

    async getListMembers(uri) {
        const maxRetries = 3;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.rateLimiter.throttle();
                
                const response = await this.agent.api.app.bsky.graph.getList({
                    list: uri,
                    limit: 100
                });
                
                // Handle rate limits and check if we need to retry
                const shouldRetry = await this.rateLimiter.handleResponse(response);
                if (shouldRetry && attempt < maxRetries - 1) {
                    continue;
                }

                logger.info(`Raw API response: ${JSON.stringify(response, null, 2)}`);

                if (!response?.data?.items) {
                    logger.error(`No items found in getList response for ${uri}`);
                    return [];
                }

                const members = response.data.items;
                logger.info(`Found ${members.length} members in list`);
                if (members.length > 0) {
                    logger.info(`Sample member structure: ${JSON.stringify(members[0], null, 2)}`);
                }
                
                return members;
            } catch (err) {
                if (err.status === 429 && attempt < maxRetries - 1) {
                    // Let the rate limiter handle the 429
                    const shouldRetry = await this.rateLimiter.handleResponse(err);
                    if (shouldRetry) continue;
                }
                logger.error(`Error getting list members for ${uri}: ${err.message}`);
                return [];
            }
        }
    }

    async getProfile(did) {
        const maxRetries = 3;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.rateLimiter.throttle();
                
                const response = await this.agent.api.app.bsky.actor.getProfile({
                    actor: did
                });
                
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
        const MAX_RETRIES = 3;
        const RETRY_DELAY = 2000; // 2 seconds

        try {
            await this.rateLimiter.throttle();
            const handle = this.sanitizeHandle(rawHandle);
            logger.info(`Attempting to resolve sanitized handle: ${handle}`);
            
            const response = await this.agent.resolveHandle({ handle });
            
            // Handle rate limits
            const shouldRetry = await this.rateLimiter.handleResponse(response);
            if (shouldRetry && retries < MAX_RETRIES - 1) {
                logger.info(`Retrying handle resolution for ${rawHandle} (${retries + 2}/${MAX_RETRIES})...`);
                await this.delay(RETRY_DELAY * (retries + 1)); // Exponential backoff
                return await this.resolveHandleWithRetry(rawHandle, retries + 1);
            }

            if (response?.data?.did) {
                return response.data.did;
            } else {
                throw new Error('No DID found in the response.');
            }
        } catch (err) {
            if (err.status === 429 && retries < MAX_RETRIES - 1) {
                const shouldRetry = await this.rateLimiter.handleResponse(err);
                if (shouldRetry) {
                    logger.info(`Retrying handle resolution for ${rawHandle} (${retries + 2}/${MAX_RETRIES})...`);
                    await this.delay(RETRY_DELAY * (retries + 1));
                    return await this.resolveHandleWithRetry(rawHandle, retries + 1);
                }
            }
            logger.error(`Error resolving handle ${rawHandle}: ${err.message}`);
            if (retries < MAX_RETRIES - 1) {
                logger.info(`Retrying handle resolution for ${rawHandle} (${retries + 2}/${MAX_RETRIES})...`);
                await this.delay(RETRY_DELAY * (retries + 1)); // Exponential backoff
                return await this.resolveHandleWithRetry(rawHandle, retries + 1);
            } else {
                logger.error(`Failed to resolve handle ${rawHandle} after ${MAX_RETRIES} attempts.`);
                return null;
            }
        }
    }


    async processStarterPack(urlLine) {
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
        const listMembers = await this.getListMembers(value.list);
        
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
    }

    async exportData(format = 'json') {
        // Since we're writing incrementally, finalize the JSON array
        if (format === 'json') {
            this.jsonStream.write('\n]\n');
            this.jsonStream.end();
        }

        // For YAML, just end the stream
        if (format === 'yaml') {
            this.yamlStream.end();
        }

        logger.info(`Exported data to starter_packs.${format}`);
    }

    async processUrls(filename) {
        if (!fs.existsSync(filename)) {
            logger.error(`Input file ${filename} not found`);
            return;
        }

        const urls = fs.readFileSync(filename, 'utf-8')
            .split('\n')
            .map(line => line.trim())
            .filter(line => line && line.includes('|'));

        logger.info(`Processing ${urls.length} starter packs...`);

        // Load last processed index
        let startIndex = Math.max(0, this.checkpointManager.getLastProcessedIndex() + 1);
        logger.info(`Resuming from index ${startIndex}`);

        // Show daily stats if available
        const todayStats = this.checkpointManager.getDailyStats();
        if (todayStats) {
            logger.info(`Today's progress: ${JSON.stringify(todayStats, null, 2)}`);
        }

        for (const [index, urlLine] of urls.entries()) {
            if (index < startIndex) continue; // Skip already processed

            const [creatorHandle, rkey] = urlLine.trim().split('|').map(s => s.trim());
            
            // Skip if already processed successfully
            if (!this.checkpointManager.shouldProcessPack(rkey)) {
                logger.info(`Skipping already processed pack: ${rkey}`);
                continue;
            }

            try {
                await this.processStarterPack(urlLine.trim());
                this.checkpointManager.updateProgress(index, rkey, 'success');
            } catch (err) {
                if (err.status === 429) {
                    logger.warn(`Rate limit reached at index ${index}. Saving progress and exiting...`);
                    this.checkpointManager.updateProgress(index, rkey, 'rateLimit', err);
                    // Gracefully exit if we hit rate limits
                    await this.cleanup();
                    process.exit(0);
                } else {
                    logger.error(`Error processing pack ${rkey}: ${err.message}`);
                    this.checkpointManager.updateProgress(index, rkey, 'error', err);
                }
            }

            // Rate limiting protection with progress saving
            if (index % 10 === 0 && index !== 0) {
                logger.info('Adding small delay to prevent rate limiting...');
                await this.delay(1000);
            }

            // Longer pause every 100 items with progress saving
            if (index % 100 === 0 && index !== 0) {
                logger.info('Adding longer delay to prevent rate limiting...');
                await this.delay(5000);
            }
        }

        // Finalize the output files
        await this.cleanup();
    }

    async cleanup() {
        await this.exportData('json');
        await this.exportData('yaml');
        await this.mongoClient.close();
    }

    async collect() {
        await this.setupDatabase();
        await this.setupAgent();
        await this.processUrls('starter_pack_urls.txt');
        await this.mongoClient.close();
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

async function main() {
    const processor = new StarterPackProcessor();
    
    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        logger.info('Received SIGINT. Cleaning up...');
        await processor.cleanup();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        logger.info('Received SIGTERM. Cleaning up...');
        await processor.cleanup();
        process.exit(0);
    });

    try {
        await processor.collect();
    } catch (err) {
        logger.error('Fatal error:', err);
        await processor.cleanup();
        process.exit(1);
    }
}

main().catch(err => {
    logger.error('Fatal error:', err);
    process.exit(1);
});