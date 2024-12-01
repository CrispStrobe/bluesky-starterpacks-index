#!/usr/bin/env node

import * as dotenv from 'dotenv';
import { BskyAgent } from '@atproto/api';
import { MongoClient } from 'mongodb';
import fs from 'fs/promises';
import winston from 'winston';
import yaml from 'js-yaml';
import path from 'path';

// Load environment variables
dotenv.config();

// Database configurations
const DB_CONFIGS = {
    cosmos: {
        ssl: true,
        replicaSet: 'globaldb',
        retryWrites: false,
        maxIdleTimeMS: 120000,
        connectTimeoutMS: 30000,
        socketTimeoutMS: 30000
    },
    mongodb: {
        retryWrites: true,
        maxIdleTimeMS: 300000
    }
};

const BATCH_SIZES = {
    cosmos: 10,
    mongodb: 100
};

const DB_INFO = {
    cosmos: {
        supportsCollMod: false,
        isCosmosDb: true
    },
    mongodb: {
        supportsCollMod: true,
        isCosmosDb: false
    }
};

// API configurations
const API_CONFIG = {
    defaultTimeout: 30000,
    maxRetries: 3,
    baseURLs: {
        primary: 'https://bsky.social',
        public: 'https://public.api.bsky.app'
    }
};

// Cache configurations
const CACHE_CONFIG = {
    maxProfileCacheSize: 1000,
    profileCacheTTL: 24 * 60 * 60 * 1000, // 24 hours
    maxPackCacheSize: 500,
    packCacheTTL: 12 * 60 * 60 * 1000 // 12 hours
};

// File paths
const FILE_PATHS = {
    urls: process.env.URLS_PATH || 'starter_pack_urls.txt',
    users: process.env.USERS_PATH || 'users.json',
    packs: process.env.PACKS_PATH || 'starter_packs.json',
    usersBackup: process.env.USERS_BACKUP_PATH || 'users.yaml',
    packsBackup: process.env.PACKS_BACKUP_PATH || 'starter_packs.yaml',
    errorLog: process.env.ERROR_LOG_PATH || 'processor-error.log',
    infoLog: process.env.INFO_LOG_PATH || 'processor.log',
    checkpoints: process.env.CHECKPOINTS_PATH || 'checkpoint.json',
    checkpointsBackup: process.env.CHECKPOINTS_BACKUP_PATH || 'checkpoint.json.tmp'
}; 

// Determine database type from environment
const DB_TYPE = process.env.DB_TYPE || 'mongodb';
const DB_CONFIG = DB_CONFIGS[DB_TYPE] || DB_CONFIGS.mongodb;
const BATCH_SIZE = BATCH_SIZES[DB_TYPE] || BATCH_SIZES.mongodb;

const MAX_PACK_DEPTH = process.env.MAX_PACK_DEPTH || 2 // Controls how deep to go when discovering associated packs

// Error types
const ERROR_TYPES = {
    RATE_LIMIT: 'RATE_LIMIT',
    AUTH_ERROR: 'AUTH_ERROR',
    NOT_FOUND: 'NOT_FOUND',
    NETWORK_ERROR: 'NETWORK_ERROR',
    DB_ERROR: 'DB_ERROR',
    VALIDATION_ERROR: 'VALIDATION_ERROR'
};

// Validation schemas
const VALIDATION_SCHEMAS = {
    user: {
        required: ['did', 'handle', 'last_updated'],
        optional: ['displayName', 'description', 'avatar']
    },
    pack: {
        required: ['rkey', 'name', 'creator', 'creator_did', 'updated_at'],
        optional: ['description', 'avatar', 'user_count']
    }
};

// Initialize global logger
const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.splat(),
        winston.format.json()
    ),
    defaultMeta: { service: 'starter-pack-processor' },
    transports: [
        new winston.transports.Console({
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.simple(),
                winston.format.printf(({ timestamp, level, message, ...meta }) => {
                    let metaStr = Object.keys(meta).length ? 
                        '\n' + JSON.stringify(meta, null, 2) : '';
                    return `${timestamp} [${level}]: ${message}${metaStr}`;
                })
            )
        }),
        new winston.transports.File({ 
            filename: FILE_PATHS.errorLog,
            level: 'error',
            maxsize: 10485760, // 10MB
            maxFiles: 5
        }),
        new winston.transports.File({ 
            filename: FILE_PATHS.infoLog,
            maxsize: 10485760,
            maxFiles: 5
        })
    ]
});

// Export constants and logger
export {
    DB_CONFIG,
    BATCH_SIZE,
    API_CONFIG,
    CACHE_CONFIG,
    FILE_PATHS,
    ERROR_TYPES,
    VALIDATION_SCHEMAS,
    logger
};

class MetricsCollector {
    constructor() {
        this.metrics = {
            startTime: Date.now(),
            api: {
                calls: 0,
                errors: 0,
                rateLimits: 0,
                totalLatency: 0,
                byEndpoint: new Map()
            },
            database: {
                operations: 0,
                errors: 0,
                totalLatency: 0,
                byOperation: new Map()
            },
            processing: {
                packsProcessed: 0,
                packsSkipped: 0,
                packsFailed: 0,
                usersProcessed: 0,
                usersFailed: 0,
                totalProcessingTime: 0
            },
            memory: {
                peaks: [],
                collections: 0,
                lastUsage: null
            },
            cache: {
                hits: 0,
                misses: 0,
                evictions: 0
            },
            fileOperations: {  // Initialize fileOperations here
                calls: 0,
                errors: 0,
                success: 0,
                totalLatency: 0,
                byOperation: new Map()
            },
            associatedPacks: {
                discovered: 0,
                queued: 0,
                skipped: 0,
                failed: 0,
                relationships: new Map()
            }

        };

        // Track memory usage periodically
        this.memoryInterval = setInterval(() => {
            this.trackMemoryUsage();
        }, 60000).unref(); // Don't keep process alive for metrics
    }

    // API Metrics
    recordApiCall(endpoint, latency, success = true) {
        this.metrics.api.calls++;
        this.metrics.api.totalLatency += latency;

        if (!this.metrics.api.byEndpoint.has(endpoint)) {
            this.metrics.api.byEndpoint.set(endpoint, {
                calls: 0,
                errors: 0,
                totalLatency: 0
            });
        }

        const endpointStats = this.metrics.api.byEndpoint.get(endpoint);
        endpointStats.calls++;
        endpointStats.totalLatency += latency;
        if (!success) endpointStats.errors++;
    }

    recordAssociatedPacksMetrics(results) {
        const { discovered, queued, skipped, failed } = results;
        this.metrics.associatedPacks.discovered += discovered;
        this.metrics.associatedPacks.queued += queued;
        this.metrics.associatedPacks.skipped += skipped;
        this.metrics.associatedPacks.failed += failed;
    }

    recordRateLimit(endpoint) {
        this.metrics.api.rateLimits++;
        const endpointStats = this.metrics.api.byEndpoint.get(endpoint);
        if (endpointStats) {
            endpointStats.rateLimits = (endpointStats.rateLimits || 0) + 1;
        }
    }

    // Database Metrics
    recordDbOperation(operation, latency, success = true) {
        this.metrics.database.operations++;
        this.metrics.database.totalLatency += latency;

        if (!this.metrics.database.byOperation.has(operation)) {
            this.metrics.database.byOperation.set(operation, {
                count: 0,
                errors: 0,
                totalLatency: 0
            });
        }

        const opStats = this.metrics.database.byOperation.get(operation);
        opStats.count++;
        opStats.totalLatency += latency;
        if (!success) {
            opStats.errors++;
            this.metrics.database.errors++;
        }
    }

    /**
     * Records a file operation metric.
     * @param {string} operation - The name of the file operation (e.g., 'verify', 'repair').
     * @param {number} latency - The time taken for the operation in milliseconds.
     * @param {boolean} success - Indicates if the operation was successful.
     */
    recordFileOperation(operation, latency, success) {
        // Initialize file metrics if not present
        if (!this.metrics.files) {
            this.metrics.files = {
                operations: 0,
                successes: 0,
                failures: 0,
                totalLatency: 0
            };
        }

        this.metrics.files.operations++;
        this.metrics.files.totalLatency += latency;

        if (success) {
            this.metrics.files.successes++;
        } else {
            this.metrics.files.failures++;
        }

        // Log the file operation
        logger.debug(`File Operation - ${operation}: ${success ? 'Success' : 'Failed'}`);
    }

    // Processing Metrics
    recordPackProcessing(success = true, timeSpent = 0) {
        if (success) {
            this.metrics.processing.packsProcessed++;
        } else {
            this.metrics.processing.packsFailed++;
        }
        this.metrics.processing.totalProcessingTime += timeSpent;
    }

    recordPackSkipped() {
        this.metrics.processing.packsSkipped++;
    }

    recordUserProcessing(success = true) {
        if (success) {
            this.metrics.processing.usersProcessed++;
        } else {
            this.metrics.processing.usersFailed++;
        }
    }

    recordError(operation, err) {
        if (!this.metrics.errors) {
            this.metrics.errors = new Map();
        }
        
        if (!this.metrics.errors.has(operation)) {
            this.metrics.errors.set(operation, []);
        }
        
        this.metrics.errors.get(operation).push({
            timestamp: new Date().toISOString(),
            message: err.message,
            code: err.code || err.status
        });
    }
    
    recordStartup() {
        this.metrics.startupTime = Date.now();
    }
    
    recordFatalError(err) {
        if (!this.metrics.fatalErrors) {
            this.metrics.fatalErrors = [];
        }
        
        this.metrics.fatalErrors.push({
            timestamp: new Date().toISOString(),
            message: err.message,
            stack: err.stack
        });
    }

    // Cache Metrics
    recordCacheHit() {
        this.metrics.cache.hits++;
    }

    recordCacheMiss() {
        this.metrics.cache.misses++;
    }

    recordCacheEviction() {
        this.metrics.cache.evictions++;
    }

    // Memory Tracking
    trackMemoryUsage() {
        const usage = process.memoryUsage();
        this.metrics.memory.lastUsage = {
            timestamp: Date.now(),
            heapUsed: usage.heapUsed,
            heapTotal: usage.heapTotal,
            external: usage.external,
            arrayBuffers: usage.arrayBuffers
        };

        // Track peak memory usage
        if (this.metrics.memory.peaks.length === 0 || 
            usage.heapUsed > this.metrics.memory.peaks[this.metrics.memory.peaks.length - 1].heapUsed) {
            this.metrics.memory.peaks.push({
                timestamp: Date.now(),
                heapUsed: usage.heapUsed,
                heapTotal: usage.heapTotal
            });

            // Keep only last 10 peaks
            if (this.metrics.memory.peaks.length > 10) {
                this.metrics.memory.peaks.shift();
            }
        }
    }

    recordGC() {
        this.metrics.memory.collections++;
    }

    // Metrics Reporting
    // Inside MetricsCollector class
    getMetrics() {
        const now = Date.now();
        const runtime = now - this.metrics.startTime;

        return {
            runtime: {
                seconds: Math.floor(runtime / 1000),
                minutes: Math.floor(runtime / 60000)
            },
            api: {
                totalCalls: this.metrics.api.calls,
                averageLatency: this.metrics.api.calls ? 
                    this.metrics.api.totalLatency / this.metrics.api.calls : 0,
                errorRate: this.metrics.api.calls ? 
                    (this.metrics.api.errors / this.metrics.api.calls) * 100 : 0,
                rateLimits: this.metrics.api.rateLimits,
                byEndpoint: Object.fromEntries(this.metrics.api.byEndpoint)
            },
            database: {
                totalOperations: this.metrics.database.operations,
                averageLatency: this.metrics.database.operations ? 
                    this.metrics.database.totalLatency / this.metrics.database.operations : 0,
                errorRate: this.metrics.database.operations ? 
                    (this.metrics.database.errors / this.metrics.database.operations) * 100 : 0,
                byOperation: Object.fromEntries(this.metrics.database.byOperation)
            },
            processing: {
                packsProcessed: this.metrics.processing.packsProcessed,
                packsSkipped: this.metrics.processing.packsSkipped,
                packsFailed: this.metrics.processing.packsFailed,
                usersProcessed: this.metrics.processing.usersProcessed,
                usersFailed: this.metrics.processing.usersFailed,
                averagePackProcessingTime: this.metrics.processing.packsProcessed ? 
                    this.metrics.processing.totalProcessingTime / this.metrics.processing.packsProcessed : 0
            },
            cache: {
                hitRate: (this.metrics.cache.hits + this.metrics.cache.misses) ? 
                    (this.metrics.cache.hits / (this.metrics.cache.hits + this.metrics.cache.misses)) * 100 : 0,
                evictions: this.metrics.cache.evictions
            },
            memory: {
                current: this.metrics.memory.lastUsage,
                peaks: this.metrics.memory.peaks,
                gcCollections: this.metrics.memory.collections
            },
            fileOperations: {
                totalCalls: this.metrics.fileOperations.calls,
                averageLatency: this.metrics.fileOperations.calls ? 
                    this.metrics.fileOperations.totalLatency / this.metrics.fileOperations.calls : 0,
                errorRate: this.metrics.fileOperations.calls ? 
                    (this.metrics.fileOperations.errors / this.metrics.fileOperations.calls) * 100 : 0,
                byOperation: Object.fromEntries(this.metrics.fileOperations.byOperation)
            },
            associatedPacks: {
                discovered: this.metrics.associatedPacks.discovered,
                queued: this.metrics.associatedPacks.queued,
                skipped: this.metrics.associatedPacks.skipped,
                failed: this.metrics.associatedPacks.failed,
                successRate: this.metrics.associatedPacks.queued / 
                    (this.metrics.associatedPacks.discovered || 1) * 100
            }
        };
    }


    // Get progress metrics
    getProgress() {
        const total = this.metrics.processing.packsProcessed + 
                     this.metrics.processing.packsSkipped + 
                     this.metrics.processing.packsFailed;
        
        return {
            total,
            processed: this.metrics.processing.packsProcessed,
            skipped: this.metrics.processing.packsSkipped,
            failed: this.metrics.processing.packsFailed,
            successRate: total ? 
                (this.metrics.processing.packsProcessed / total) * 100 : 0
        };
    }

    // Get performance metrics
    getPerformanceMetrics() {
        return {
            api: {
                averageLatency: this.metrics.api.calls ? 
                    this.metrics.api.totalLatency / this.metrics.api.calls : 0,
                rateLimitsPerHour: this.metrics.api.rateLimits / 
                    (((Date.now() - this.metrics.startTime) / 1000 / 60 / 60) || 1)
            },
            database: {
                averageLatency: this.metrics.database.operations ? 
                    this.metrics.database.totalLatency / this.metrics.database.operations : 0,
                operationsPerSecond: this.metrics.database.operations / 
                    (((Date.now() - this.metrics.startTime) / 1000) || 1)
            },
            memory: {
                currentUsage: this.metrics.memory.lastUsage,
                averagePeak: this.metrics.memory.peaks.reduce((sum, peak) => sum + peak.heapUsed, 0) / 
                    (this.metrics.memory.peaks.length || 1)
            },
            cache: {
                hitRate: (this.metrics.cache.hits + this.metrics.cache.misses) ? 
                    (this.metrics.cache.hits / (this.metrics.cache.hits + this.metrics.cache.misses)) * 100 : 0
            }
        };
    }

    recordOperation(name, duration) {
        if (!this.metrics.operations) {
            this.metrics.operations = new Map();
        }
        if (!this.metrics.operations.has(name)) {
            this.metrics.operations.set(name, {
                count: 0,
                totalDuration: 0
            });
        }
        const op = this.metrics.operations.get(name);
        op.count++;
        op.totalDuration += duration;
    }

    getOperationTime(name) {
        return this.metrics.operations?.get(name)?.totalDuration || 0;
    }

    recordProfileProcessing(duration) {
        if (!this.metrics.profiles) {
            this.metrics.profiles = {
                processed: 0,
                totalDuration: 0,
                avgDuration: 0
            };
        }
        this.metrics.profiles.processed++;
        this.metrics.profiles.totalDuration += duration;
        this.metrics.profiles.avgDuration = 
            this.metrics.profiles.totalDuration / this.metrics.profiles.processed;
    }

    cleanup() {
        logger.debug('Starting metrics collector cleanup...');
        
        try {
            // Clear memory tracking interval
            if (this.memoryInterval) {
                clearInterval(this.memoryInterval);
                this.memoryInterval = null;
            }
    
            // Save final metrics if needed
            const finalMetrics = this.getMetrics();
            logger.debug('Final metrics:', finalMetrics);
    
            // Clear metrics data
            this.metrics = {
                api: { calls: 0, errors: 0, rateLimits: 0, totalLatency: 0, byEndpoint: new Map() },
                database: { operations: 0, errors: 0, totalLatency: 0, byOperation: new Map() },
                processing: { packsProcessed: 0, packsSkipped: 0, packsFailed: 0, usersProcessed: 0, usersFailed: 0, totalProcessingTime: 0 },
                memory: { peaks: [], collections: 0, lastUsage: null },
                cache: { hits: 0, misses: 0, evictions: 0 },
                fileOperations: { calls: 0, errors: 0, success: 0, totalLatency: 0, byOperation: new Map() }
            };
    
            logger.debug('Metrics collector cleanup completed');
        } catch (err) {
            logger.error('Error during metrics collector cleanup:', err);
        }
    }
}

// Export a singleton instance
export const metrics = new MetricsCollector();

class DebugManager {
    constructor(options = {}) {
        this.enabled = options.debug || false;
        this.verbosity = options.verbosity || 'info';
        this.logTimings = options.logTimings || true;
        this.logMemory = options.logMemory || true;
        this.metricsInterval = null;

        if (this.enabled) {
            this.setupDebugMode();
        }
    }

    setupDebugMode() {
        // Enhanced console logging
        logger.level = 'debug';
        
        // Start periodic metrics logging
        this.metricsInterval = setInterval(() => {
            this.logPerformanceMetrics();
        }, 60000).unref(); // Don't keep process alive

        // Memory tracking
        if (this.logMemory) {
            this.trackMemoryUsage();
        }
    }

    debug(message, context = {}) {
        if (!this.enabled) return;

        const debugContext = {
            ...context,
            timestamp: new Date().toISOString()
        };

        if (this.logTimings && context.startTime) {
            debugContext.duration = Date.now() - context.startTime;
        }

        if (this.logMemory) {
            const memUsage = process.memoryUsage();
            debugContext.memory = {
                heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024) + 'MB',
                heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024) + 'MB'
            };
        }

        logger.debug(message, debugContext);
    }

    trackMemoryUsage() {
        const memoryLog = [];
        const gcLog = [];

        // Track GC if available
        if (global.gc) {
            const originalGc = global.gc;
            global.gc = (...args) => {
                const before = process.memoryUsage().heapUsed;
                originalGc(...args);
                const after = process.memoryUsage().heapUsed;
                gcLog.push({
                    timestamp: new Date(),
                    freed: (before - after) / 1024 / 1024
                });
                this.debug('Garbage collection completed', {
                    freedMB: Math.round((before - after) / 1024 / 1024)
                });
            };
        }

        // Periodic memory snapshots
        setInterval(() => {
            const usage = process.memoryUsage();
            memoryLog.push({
                timestamp: new Date(),
                heapUsed: usage.heapUsed,
                heapTotal: usage.heapTotal
            });

            // Keep only last hour of data
            const hourAgo = Date.now() - 3600000;
            while (memoryLog[0]?.timestamp < hourAgo) {
                memoryLog.shift();
            }
            while (gcLog[0]?.timestamp < hourAgo) {
                gcLog.shift();
            }
        }, 60000).unref();
    }

    async logPerformanceMetrics() {
        if (!this.enabled) return;

        const currentMetrics = metrics.getMetrics();
        const performanceMetrics = metrics.getPerformanceMetrics();
        
        this.debug('Performance metrics', {
            api: {
                callsPerMinute: currentMetrics.api.totalCalls / 
                    (currentMetrics.runtime.minutes || 1),
                averageLatency: performanceMetrics.api.averageLatency,
                errorRate: currentMetrics.api.errorRate
            },
            processing: {
                packsPerMinute: currentMetrics.processing.packsProcessed / 
                    (currentMetrics.runtime.minutes || 1),
                successRate: (currentMetrics.processing.packsProcessed /
                    (currentMetrics.processing.packsProcessed + 
                     currentMetrics.processing.packsFailed || 1)) * 100
            },
            cache: {
                hitRate: performanceMetrics.cache.hitRate,
                size: currentMetrics.cache.size
            },
            memory: {
                usage: process.memoryUsage(),
                gcCollections: currentMetrics.memory.gcCollections
            }
        });
    }

    cleanup() {
        if (this.metricsInterval) {
            clearInterval(this.metricsInterval);
        }
    }
}

class ErrorVerificationHandler {
    constructor(options = {}) {
        if (!options.apiHandler) {
            throw new Error('API handler is required for ErrorVerificationHandler');
        }
        
        this.debugManager = options.debugManager;
        this.metrics = options.metrics;
        this.maxRetries = options.maxRetries || 3;
        this.verificationResults = new Map();
        this.fileHandler = options.fileHandler;
        this.dbManager = options.dbManager;
        this.apiHandler = options.apiHandler;
    }

    async verifySystemState() {
        logger.debug('Verify system state');

        const results = {
            status: 'ok',
            files: await this.verifyFiles(),
            // database: await this.verifyDatabase(),
            relationships: await this.verifyRelationships()
        };

        return results;
    }

    async verifyRelationships() {
        // Verify pack relationships
        const relationshipResults = {
            total: this.packRelationships?.size || 0,
            invalid: 0,
            repaired: 0
        };

        if (this.packRelationships) {
            for (const [rkey, discoverers] of this.packRelationships) {
                // Check if the pack exists
                const pack = await this.fileHandler.getPack(rkey);
                if (!pack) {
                    this.packRelationships.delete(rkey);
                    relationshipResults.invalid++;
                    relationshipResults.repaired++;
                    continue;
                }

                // Check if discoverers exist
                for (const did of discoverers) {
                    const user = await this.fileHandler.getUser(did);
                    if (!user) {
                        discoverers.delete(did);
                        relationshipResults.invalid++;
                        relationshipResults.repaired++;
                    }
                }
            }
        }

        return relationshipResults;
    }

    async verifyFiles() {
        const results = {
            verified: [],
            repaired: [],
            failed: []
        };
    
        for (const [key, path] of Object.entries(FILE_PATHS)) {
            try {
                const startTime = Date.now();
                const fileState = await this.fileHandler.verifyFileIntegrity(path);
    
                if (fileState.needsRepair) {
                    const backup = await this.backupFile(path);
                    results.repaired.push(path);
    
                    // Log the repair operation
                    if (this.metrics) {
                        this.metrics.recordFileOperation('repair', Date.now() - startTime, true);
                    }
    
                    logger.info(`Repaired and backed up file: ${path}`);
                } else {
                    results.verified.push(path);
    
                    // Log the verification success
                    if (this.metrics) {
                        this.metrics.recordFileOperation('verify', Date.now() - startTime, true);
                    }
    
                    logger.debug(`Verified file integrity: ${path}`);
                }
            } catch (err) {
                if (err.code === 'ENOENT') {
                    // File doesn't exist - create empty
                    await this.fileHandler.createEmptyFile({ path, type: 'json' }); // Adjust type as needed
                    results.repaired.push(path);
    
                    // Log the creation of the empty file
                    if (this.metrics) {
                        this.metrics.recordFileOperation('create_empty', 0, true);
                    }
    
                    logger.warn(`File not found. Created empty file: ${path}`);
                } else {
                    results.failed.push({
                        path,
                        error: err.message,
                        timestamp: new Date().toISOString()
                    });
    
                    // Log the verification failure
                    if (this.metrics) {
                        this.metrics.recordFileOperation('verify', 0, false);
                    }
    
                    logger.error(`File verification failed for ${path}:`, err);
                }
            }
        }
    
        return results;
    }

    async backupFile(filePath) {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const backupPath = `${filePath}.${timestamp}.bak`;
        
        try {
            await fs.copyFile(filePath, backupPath);
            return {
                original: filePath,
                backup: backupPath,
                timestamp,
                success: true
            };
        } catch (err) {
            logger.error(`Backup failed for ${filePath}:`, err);
            return {
                original: filePath,
                error: err.message,
                timestamp,
                success: false
            };
        }
    }

    getVerificationHistory() {
        return Array.from(this.verificationResults.entries())
            .sort((a, b) => b[1].timestamp - a[1].timestamp);
    }

    cleanup() {
        this.verificationResults.clear();
    }
}

class ValidationHelper {
    constructor(schemas) {
        this.schemas = schemas;
    }

    validateUser(user) {
        try {
            const schema = this.schemas.user;
            const missing = schema.required.filter(field => !user[field]);
            
            if (missing.length > 0) {
                throw new Error(`Invalid user data: missing fields ${missing.join(', ')}`);
            }

            // Validate data types
            if (typeof user.did !== 'string' || !user.did.startsWith('did:')) {
                throw new Error('Invalid DID format');
            }

            if (typeof user.handle !== 'string' || !user.handle.includes('.')) {
                throw new Error('Invalid handle format');
            }

            // Validate timestamp
            if (!this.isValidTimestamp(user.last_updated)) {
                throw new Error('Invalid timestamp format');
            }

            return true;
        } catch (err) {
            logger.warn(`User validation failed: ${err.message}`);
            return false;
        }
    }

    validateProfileData(profile) {
        if (!profile?.did || !profile?.handle) {
            return false;
        }
        return (
            typeof profile.did === 'string' &&
            profile.did.startsWith('did:') &&
            typeof profile.handle === 'string' &&
            profile.handle.includes('.')
        );
    }

    validatePack(pack) {
        const schema = this.schemas.pack;
        const missing = schema.required.filter(field => !pack[field]);
        
        if (missing.length > 0) {
            throw new Error(`Invalid pack data: missing fields ${missing.join(', ')}`);
        }

        // Validate data types
        if (typeof pack.rkey !== 'string' || !/^[a-zA-Z0-9]+$/.test(pack.rkey)) {
            throw new Error('Invalid rkey format');
        }

        if (typeof pack.creator_did !== 'string' || !pack.creator_did.startsWith('did:')) {
            throw new Error('Invalid creator DID format');
        }

        // Validate timestamp
        if (!this.isValidTimestamp(pack.updated_at)) {
            throw new Error('Invalid timestamp format');
        }

        return true;
    }

    isValidTimestamp(timestamp) {
        if (typeof timestamp !== 'string') return false;
        const date = new Date(timestamp);
        return date instanceof Date && !isNaN(date);
    }

    sanitizeUser(user) {
        const sanitized = {};
        const allFields = [...this.schemas.user.required, ...this.schemas.user.optional];
        
        for (const field of allFields) {
            if (user[field] !== undefined) {
                sanitized[field] = user[field];
            }
        }
        
        return sanitized;
    }

    sanitizePack(pack) {
        const sanitized = {};
        const allFields = [...this.schemas.pack.required, ...this.schemas.pack.optional];
        
        for (const field of allFields) {
            if (pack[field] !== undefined) {
                sanitized[field] = pack[field];
            }
        }
        
        return sanitized;
    }
}

class FileHandler {
    constructor() {
        this.userCache = new Map();
        this.packCache = new Map();
        this.initialized = false;
        this.failureLog = new Map(); // Track permanent failures
        this.fileFormats = new Map(); // Track file formats (json/ndjson)
        this.writeLock = false;
        this.writeQueue = [];
    }

    async init() {
        if (this.initialized) return;
        
        try {
            // Create empty files if they don't exist
            await this.ensureFilesExist();
            
            // Check and convert file formats if needed
            await this.ensureFileFormats();
            
            // Load data
            const jsonSuccess = await this.loadFromJson();
            if (!jsonSuccess) {
                logger.warn('JSON load failed, trying YAML backups');
                await this.loadFromYaml();
            }
            
            this.initialized = true;
        } catch (err) {
            logger.error(`FileHandler initialization failed: ${err.message}`);
            throw err;
        }
    }

    async acquireLock() {
        if (this.writeLock) {
            await new Promise(resolve => this.writeQueue.push(resolve));
        }
        this.writeLock = true;
    }

    releaseLock() {
        this.writeLock = false;
        const next = this.writeQueue.shift();
        if (next) next();
    }

    async ensureFilesExist() {
        const files = [FILE_PATHS.users, FILE_PATHS.packs, FILE_PATHS.urls];
        await Promise.all(files.map(async (file) => {
            try {
                await fs.access(file);
            } catch (err) {
                if (err.code === 'ENOENT') {
                    await fs.writeFile(file, '');
                    logger.info(`Created empty file: ${file}`);
                } else {
                    throw err;
                }
            }
        }));
    }

    async ensureFileFormats() {
        // Check users.json format
        try {
            const usersContent = await fs.readFile(FILE_PATHS.users, 'utf8');
            if (usersContent.trim().startsWith('[') || usersContent.trim().startsWith('{')) {
                // Convert JSON to NDJSON
                logger.debug(`Converting to NDJSON ...`);
                const users = JSON.parse(usersContent);
                await this.convertToNDJSON(FILE_PATHS.users, users);
                this.fileFormats.set(FILE_PATHS.users, 'ndjson');
            } else {
                this.fileFormats.set(FILE_PATHS.users, 'ndjson');
            }
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn(`Error checking users.json format: ${err.message}`);
            }
        }

        // Check starter_packs.json format
        try {
            const packsContent = await fs.readFile(FILE_PATHS.packs, 'utf8');
            if (packsContent.trim().startsWith('[') || packsContent.trim().startsWith('{')) {
                // Convert JSON to NDJSON
                const packs = JSON.parse(packsContent);
                await this.convertToNDJSON(FILE_PATHS.packs, packs);
                this.fileFormats.set(FILE_PATHS.packs, 'ndjson');
            } else {
                this.fileFormats.set(FILE_PATHS.packs, 'ndjson');
            }
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn(`Error checking starter_packs.json format: ${err.message}`);
            }
        }
    }

    async convertToNDJSON(filepath, data) {
        const backup = `${filepath}.${new Date().toISOString().replace(/[:.]/g, '-')}.bak`;
        await fs.copyFile(filepath, backup);

        const items = Array.isArray(data) ? data : [data];
        const ndjson = items.map(item => JSON.stringify(item)).join('\n') + '\n';
        await fs.writeFile(filepath, ndjson);
        
        logger.info(`Converted ${filepath} to NDJSON format (backup: ${backup})`);
    }

    async loadFailureLog() {
        try {
            const content = await fs.readFile('failure_log.json', 'utf8');
            this.failureLog = new Map(JSON.parse(content));
        } catch (err) {
            if (err.code !== 'ENOENT') logger.warn('Error loading failure log:', err);
        }
    }

    async recordPermanentFailure(id, type, reason) {
        this.failureLog.set(id, {
            type, // 'handle', 'profile', or 'pack'
            reason,
            timestamp: new Date().toISOString(),
            attempts: (this.failureLog.get(id)?.attempts || 0) + 1
        });
        await this.saveFailureLog();
    }

    async saveFailureLog() {
        await fs.writeFile(
            'failure_log.json',
            JSON.stringify(Array.from(this.failureLog.entries()), null, 2)
        );
    }

    async loadFromJson() {
        let success = false;
        
        // Load users
        try {
            const content = await fs.readFile(FILE_PATHS.users, 'utf8');
            for (const line of content.split('\n')) {
                if (!line.trim()) continue;
                try {
                    const user = JSON.parse(line);
                    if (user.did && user.last_updated) {
                        this.updateUserCache(user);
                        success = true;
                    }
                } catch (err) {
                    logger.warn(`Invalid user JSON line: ${err.message}`);
                }
            }
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn(`Error reading users.json: ${err.message}`);
            }
        }

        // Load packs similarly
        try {
            const content = await fs.readFile(FILE_PATHS.packs, 'utf8');
            for (const line of content.split('\n')) {
                if (!line.trim()) continue;
                try {
                    const pack = JSON.parse(line);
                    if (pack.rkey && pack.updated_at) {
                        this.updatePackCache(pack);
                        success = true;
                    }
                } catch (err) {
                    logger.warn(`Invalid pack JSON line: ${err.message}`);
                }
            }
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn(`Error reading starter_packs.json: ${err.message}`);
            }
        }

        return success;
    }

    async loadFromYaml() {
        try {
            const usersYaml = await fs.readFile(FILE_PATHS.usersBackup, 'utf8');
            const packsYaml = await fs.readFile(FILE_PATHS.packsBackup, 'utf8');
            
            yaml.loadAll(usersYaml, doc => {
                if (doc.did && doc.last_updated) {
                    this.updateUserCache(doc);
                }
            });
            
            yaml.loadAll(packsYaml, doc => {
                if (doc.rkey && doc.updated_at) {
                    this.updatePackCache(doc);
                }
            });
            
            return true;
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn('Error loading YAML backups:', err);
            }
            return false;
        }
    }
    
    async writeYamlBackups(users, packs) {
        logger.debug(`Writing yaml backups...`);
        const usersYaml = Array.from(users).map(u => `---\n${yaml.dump(u)}`).join('\n');
        const packsYaml = Array.from(packs).map(p => `---\n${yaml.dump(p)}`).join('\n');
        
        await fs.writeFile(FILE_PATHS.usersBackup, usersYaml);
        await fs.writeFile(FILE_PATHS.packsBackup, packsYaml);
    }

    async appendUser(userData) {
        try {
            await this.acquireLock();
            
            if (!userData.did || !userData.last_updated) {
                throw new Error('Invalid user data');
            }
    
            // Format user data
            const formattedUser = {
                did: userData.did,
                handle: userData.handle,
                display_name: userData.displayName || '',
                followers_count: userData.followers || 0,
                follows_count: userData.following || 0,
                last_updated: userData.last_updated,
                pack_ids: userData.pack_ids || []
            };
    
            // Update cache
            this.updateUserCache(formattedUser);
    
            // Append to NDJSON file - ensure proper line ending
            const jsonLine = JSON.stringify(formattedUser) + '\n';
            await fs.writeFile(FILE_PATHS.users, jsonLine, { flag: 'a' });
    
            // Append to YAML file with proper document separator
            const yamlDoc = '---\n' + yaml.dump(formattedUser);
            await fs.writeFile(FILE_PATHS.usersBackup, yamlDoc, { flag: 'a' });
    
            return true;
        } finally {
            this.releaseLock();
        }
    }

    async appendToUrlsFile(handle, rkey) {
        try {
            await this.acquireLock();
            
            // First check if this line already exists
            const content = await fs.readFile(FILE_PATHS.urls, 'utf8');
            const lines = content.split('\n').filter(Boolean);
            const exists = lines.some(line => {
                const [h, r] = line.split('|').map(s => s.trim());
                return r === rkey;
            });

            if (!exists) {
                const line = `${handle}|${rkey}\n`;
                await fs.appendFile(FILE_PATHS.urls, line);
                logger.debug(`Added new starter pack URL: ${handle}|${rkey}`);
            }

        } catch (err) {
            logger.error(`Error appending to URLs file: ${err.message}`);
            throw err;
        } finally {
            this.releaseLock();
        }
    }

    async appendPack(packData) {
        try {
            await this.acquireLock();
        
            if (!packData.rkey || !packData.updated_at) {
                throw new Error('Invalid pack data');
            }

            // Ensure required fields and format
            const formattedPack = {
                rkey: packData.rkey,
                name: packData.name,
                creator: packData.creator,
                creator_did: packData.creator_did,
                description: packData.description || '',
                user_count: packData.user_count || 0,
                created_at: packData.created_at || new Date().toISOString(),
                updated_at: packData.updated_at,
                users: packData.users || [],
                weekly_joins: packData.weekly_joins || 0,
                total_joins: packData.total_joins || 0
            };

            try {
                // Update cache first
                this.updatePackCache(formattedPack);

                // Append to NDJSON
                const line = JSON.stringify(formattedPack) + '\n';
                await fs.appendFile(FILE_PATHS.packs, line);

                // Backup to YAML
                const yamlDoc = '---\n' + yaml.dump(formattedPack);
                await fs.appendFile(FILE_PATHS.packsBackup, yamlDoc);

                return true;
            } catch (err) {
                logger.error(`Error appending pack ${packData.rkey}: ${err.message}`);
                throw err;
            }
        } finally {
            this.releaseLock();
        }
    }

    // Clean files by writing filtered cache content
    async cleanFiles() {
        logger.debug(`Cleaning files ...`);
        
        // Create backup of current files before cleaning
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        await fs.copyFile(FILE_PATHS.users, `${FILE_PATHS.users}.${timestamp}.bak`);
        await fs.copyFile(FILE_PATHS.packs, `${FILE_PATHS.packs}.${timestamp}.bak`);
        
        // Sort and filter users by timestamp
        const users = Array.from(this.userCache.values())
            .sort((a, b) => new Date(b.last_updated) - new Date(a.last_updated));
        const uniqueUsers = new Map();
        
        // Keep most recent version of each user
        for (const user of users) {
            if (!uniqueUsers.has(user.did)) {
                uniqueUsers.set(user.did, user);
            }
        }
    
        // Sort and filter packs similarly
        const packs = Array.from(this.packCache.values())
            .sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at));
        const uniquePacks = new Map();
        
        for (const pack of packs) {
            if (!uniquePacks.has(pack.rkey)) {
                uniquePacks.set(pack.rkey, pack);
            }
        }
    
        // Write to temporary files first
        const tmpUsers = `${FILE_PATHS.users}.tmp`;
        const tmpPacks = `${FILE_PATHS.packs}.tmp`;
    
        // Write filtered content to temporary files
        await fs.writeFile(tmpUsers, 
            Array.from(uniqueUsers.values())
                .map(u => JSON.stringify(u))
                .join('\n') + '\n'
        );
    
        await fs.writeFile(tmpPacks,
            Array.from(uniquePacks.values())
                .map(p => JSON.stringify(p))
                .join('\n') + '\n'
        );
    
        // Atomically rename temporary files to final destination
        await fs.rename(tmpUsers, FILE_PATHS.users);
        await fs.rename(tmpPacks, FILE_PATHS.packs);
    
        // Update YAML backups
        await this.writeYamlBackups(uniqueUsers.values(), uniquePacks.values());
    }

    // Cache management
    updateUserCache(user) {
        const existing = this.userCache.get(user.did);
        const lastUpdated = new Date(user.last_updated);
        
        if (!existing || lastUpdated > new Date(existing.last_updated)) {
            // Ensure consistent field names
            const formattedUser = {
                did: user.did,
                handle: user.handle,
                display_name: user.displayName || user.display_name || '',
                followers_count: user.followers_count || user.followers || 0,
                follows_count: user.follows_count || user.following || 0,
                last_updated: user.last_updated,
                profile_check_needed: false,
                pack_ids: [...new Set([...(existing?.pack_ids || []), ...(user.pack_ids || [])])]
            };
            
            this.userCache.set(user.did, formattedUser);
        } else if (existing && user.pack_ids) {
            // Update pack_ids even if the profile is not newer
            existing.pack_ids = [...new Set([...existing.pack_ids, ...user.pack_ids])];
        }
    }

    updatePackCache(pack) {
        const existing = this.packCache.get(pack.rkey);
        if (!existing || new Date(pack.updated_at) > new Date(existing.updated_at)) {
            this.packCache.set(pack.rkey, pack);
        }
    }

    // Accessors
    getUser(did) {
        return this.userCache.get(did);
    }

    getPack(rkey) {
        return this.packCache.get(rkey);
    }

    async verifyFileIntegrity() {
        logger.debug(`Verifying files ...`);
        
        const files = [
            { path: FILE_PATHS.users, type: 'ndjson' },
            { path: FILE_PATHS.packs, type: 'ndjson' },
            { path: FILE_PATHS.urls, type: 'text' },
            { path: FILE_PATHS.usersBackup, type: 'yaml' },
            { path: FILE_PATHS.packsBackup, type: 'yaml' }
        ];
    
        const results = {
            verified: [],
            failed: [],
            repaired: []
        };
    
        for (const file of files) {
            try {
                const content = await fs.readFile(file.path, 'utf8');
                let valid = false;
    
                switch (file.type) {
                    case 'ndjson':
                        valid = await this.verifyNDJSON(file.path, content);
                        break;
                    case 'yaml':
                        valid = await this.verifyYAML(file.path, content);
                        break;
                    case 'text':
                        valid = this.verifyURLsFile(content);
                        break;
                }
    
                if (valid) {
                    results.verified.push(file.path);
                } else {
                    await this.repairFile(file);
                    results.repaired.push(file.path);
                }
    
            } catch (err) {
                if (err.code === 'ENOENT') {
                    // File doesn't exist - create empty
                    await this.createEmptyFile(file);
                    results.repaired.push(file.path);
                } else {
                    logger.error(`Error verifying ${file.path}: ${err.message}`);
                    results.failed.push({
                        path: file.path,
                        error: err.message
                    });
                }
            }
        }
    
        return results;
    }
    
    async verifyNDJSON(path, content) {
        const lines = content.split('\n').filter(line => line.trim());
        let valid = true;
        const validLines = [];
    
        for (const line of lines) {
            try {
                const parsed = JSON.parse(line);
                if (path === FILE_PATHS.users) {
                    if (parsed.did && parsed.handle && parsed.last_updated) {
                        validLines.push(line);
                        continue;
                    }
                } else if (path === FILE_PATHS.packs) {
                    if (parsed.rkey && parsed.creator && parsed.updated_at) {
                        validLines.push(line);
                        continue;
                    }
                }
                valid = false; // Invalid data
            } catch {
                valid = false;
            }
        }
    
        if (!valid && validLines.length > 0) {
            // Create backup and write valid lines
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            await fs.copyFile(path, `${path}.${timestamp}.bak`);
            await fs.writeFile(path, validLines.join('\n') + '\n');
        } else if (!valid) {
            logger.warn(`No valid lines found in ${path}. Not overwriting file.`);
        }
    
        return validLines.length > 0;
    }    
    
    async verifyYAML(path, content) {
        let valid = true;
        let validDocs = [];
    
        try {
            const docs = yaml.loadAll(content);
            for (const doc of docs) {
                if (path === FILE_PATHS.usersBackup) {
                    if (doc.did && doc.handle && doc.last_updated) {
                        validDocs.push(doc);
                        continue;
                    }
                } else if (path === FILE_PATHS.packsBackup) {
                    if (doc.rkey && doc.creator && doc.updated_at) {
                        validDocs.push(doc);
                        continue;
                    }
                }
                valid = false; // Invalid document
            }
        } catch {
            valid = false;
        }
    
        if (!valid && validDocs.length > 0) {
            // Create backup and write valid documents
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            await fs.copyFile(path, `${path}.${timestamp}.bak`);
            const yamlContent = validDocs.map(doc => '---\n' + yaml.dump(doc)).join('\n');
            await fs.writeFile(path, yamlContent);
        } else if (!valid) {
            logger.warn(`No valid YAML documents found in ${path}. Not overwriting file.`);
        }
    
        return validDocs.length > 0;
    }
    
    
    verifyURLsFile(content) {
        const lines = content.split('\n').filter(line => line.trim());
        return lines.every(line => {
            const parts = line.split('|');
            return parts.length === 2 && 
                   parts[0].trim() && 
                   /^[a-zA-Z0-9]+$/.test(parts[1].trim());
        });
    }
    
    async createEmptyFile(file) {
        let content = '';
        if (file.type === 'yaml') {
            content = '---\n';
        }
        await fs.writeFile(file.path, content);
    }

    async cleanupBackups(retainDays = 7) {
        const backupFiles = await fs.readdir('.');
        const now = Date.now();
        
        for (const file of backupFiles) {
            if (file.endsWith('.bak')) {
                try {
                    const stats = await fs.stat(file);
                    const ageInDays = (now - stats.mtimeMs) / (1000 * 60 * 60 * 24);
                    
                    if (ageInDays > retainDays) {
                        await fs.unlink(file);
                        logger.debug(`Removed old backup file: ${file}`);
                    }
                } catch (err) {
                    logger.warn(`Error processing backup file ${file}:`, err);
                }
            }
        }
    }
    
    async repairFile(file) {
        // Create backup
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const backupPath = `${file.path}.${timestamp}.bak`;
        await fs.copyFile(file.path, backupPath);
    
        // Initialize empty only if appropriate
        if (file.type !== 'ndjson' && file.type !== 'yaml') {
            await this.createEmptyFile(file);
        } else {
            logger.warn(`Repair of ${file.path} skipped to avoid overwriting with empty content.`);
        }
    }
    

    async cleanup() {
        logger.debug('Starting file handler cleanup...');
        
        try {
            // Release write lock if held
            if (this.writeLock) {
                this.releaseLock();
            }
    
            // Clear any pending write operations
            this.writeQueue = [];
    
            // Clean files before closing
            await this.cleanFiles();
            
            // Clean up old backups
            await this.cleanupBackups();
    
            // Clear caches
            this.userCache.clear();
            this.packCache.clear();
            this.fileFormats.clear();
            this.failureLog.clear();

            await this.cleanupBackups();
    
            logger.debug('File handler cleanup completed');
        } catch (err) {
            logger.error('Error during file handler cleanup:', err);
            throw err;
        }
    }
}

class ApiHandler {
    constructor(agent, rateLimiter) {
        if (!agent) throw new Error('BskyAgent is required');
        this.agent = agent;
        this.rateLimiter = rateLimiter;
        this.publicEndpoint = 'https://public.api.bsky.app';
    }

    async makePublicApiCall(endpoint, params) {
        try {
            logger.debug('Public Bluesky API Call initiating:', endpoint, params)
            
            const url = new URL(`/xrpc/${endpoint}`, this.publicEndpoint);
            Object.entries(params).forEach(([key, value]) => {
                url.searchParams.append(key, value);
            });
            
            const response = await fetch(url.toString(), {
                headers: { 'Accept': 'application/json' }
            });

            if (!response.ok) {
                throw new Error(`Public API error: ${response.status}`);
            }

            return await response.json();
        } catch (err) {
            if (err.status === 429) {
                await this.rateLimiter.handleResponse(err);
            }
            throw err;
        }
    }

    async makeAuthApiCall(endpoint, params) {
        logger.debug(`makeAuthApiCall for:`, endpoint, params)
        let response;
        switch (endpoint) {
            case 'app.bsky.actor.getProfile':
                response = await this.agent.getProfile(params);
                break;
            case 'app.bsky.graph.getList':
                logger.debug(`Calling (with Auth) getList:`, {params});
                response = await this.agent.api.app.bsky.graph.getList(params);
                break;
            case 'app.bsky.graph.getActorStarterPacks':
                logger.debug(`Calling (with Auth) getActorStarterPacks:`, {params});
                response = await this.agent.api.app.bsky.graph.getActorStarterPacks(params);
                break;
            case 'app.bsky.graph.getStarterPack':
                logger.debug(`Calling (with Auth) getStarterPack:`, {params});
                response = await this.agent.api.app.bsky.graph.getStarterPack(params);
                logger.debug('response:', response);
                break;
            default:
                throw new Error(`Unsupported endpoint: ${endpoint}`);
        }
        return response?.data;
    }

    async makeApiCall(endpoint, params, options = {}) {
        logger.debug(`ApiHandler: makeApiCall:`, endpoint, params, options)
        await this.rateLimiter.throttle();
    
        // Try public API first unless auth is required
        if (!options.requireAuth) {
            try {
                const publicResponse = await this.makePublicApiCall(endpoint, params);
                if (publicResponse) return publicResponse;
            } catch (err) {
                if (err.status !== 404) {
                    logger.warn(`Public API failed for ${endpoint}: ${err.message}`);
                }
                // Fall through to authenticated API
            }
        }
    
        try {
            // Use proper auth call
            let response;
            switch (endpoint) {
                case 'app.bsky.graph.getStarterPack':
                    response = await this.agent.api.app.bsky.graph.getStarterPack({ 
                        starterPack: params.uri  // Note: proper parameter name
                    });
                    break;
                case 'app.bsky.actor.getProfile':
                    response = await this.agent.getProfile(params);
                    break;
                case 'app.bsky.graph.getList':
                    response = await this.agent.api.app.bsky.graph.getList(params);
                    break;
                case 'app.bsky.graph.getActorStarterPacks':
                    response = await this.agent.api.app.bsky.graph.getActorStarterPacks(params);
                    break;
                default:
                    throw new Error(`Unsupported endpoint: ${endpoint}`);
            }
    
            if (!response?.data) {
                throw new Error('No data in response');
            }
            logger.debug(`Call response data:`, response.data);
    
            return response.data;
    
        } catch (err) {
            if (err.status === 401) {
                await this.agent.login({
                    identifier: process.env.BSKY_USERNAME,
                    password: process.env.BSKY_PASSWORD
                });
                return this.makeApiCall(endpoint, params, options);
            }
    
            // Properly throw error up
            throw err;
        }
    }

    // Helper methods that use makeApiCall internally
    async getProfile(did) {
        return await this.makeApiCall('app.bsky.actor.getProfile', { actor: did });
    }

    async getList(uri) {
        return await this.makeApiCall('app.bsky.graph.getList', { list: uri });
    }

    async getStarterPack(uri) {
        return await this.makeApiCall('app.bsky.graph.getStarterPack', { uri });
    }

    async getActorStarterPacks(did) {
        logger.debug(`Calling API for Actor packs ${did}`);  
        const packs = await this.makeApiCall('app.bsky.graph.getActorStarterPacks', { actor: did })
        logger.debug('Call results:', packs);
        return packs;
    }

    async resolveHandle(handle) {
        try {
            const response = await this.agent.resolveHandle({ handle });
            return response?.data?.did;
        } catch (err) {
            if (err.status === 401) {
                await this.agent.login({
                    identifier: process.env.BSKY_USERNAME,
                    password: process.env.BSKY_PASSWORD
                });
                return this.resolveHandle(handle);
            }
            throw err;
        }
    }
}

class MockDatabaseManager {
    constructor() {
        this.operations = [];
        // Add a mock db object that matches the structure we expect
        this.db = {
            admin: () => ({
                ping: async () => true
            }),
            collection: () => ({
                // Add mock collection methods as needed
            }),
            listCollections: () => ({
                toArray: async () => []
            })
        };
        logger.info('Initialized Mock Database Manager');
    }

    async verifyIndexes() {
        return [];
    }

    getOperationsSummary() {
        return {
            total: this.operations.length,
            byType: this.groupOperations('type'),
            byCollection: this.groupOperations('collection')
        };
    }

    async safeWrite(collection, operation) {
        this.logOperation({
            type: 'safe_write',
            collection,
            operation
        });
    }

    async safeBulkWrite(collection, operations) {
        this.logOperation({
            type: 'safe_bulk_write',
            collection,
            operationCount: operations.length
        });
    }

    groupOperations(key) {
        return this.operations.reduce((acc, op) => {
            const value = op[key];
            acc[value] = (acc[value] || 0) + 1;
            return acc;
        }, {});
    }

    async init() {
        logger.debug('Mock DB: Database initialization simulated');
        return true;
    }

    logOperation(operation) {
        this.operations.push({
            timestamp: new Date().toISOString(),
            ...operation
        });
        logger.info('Would execute DB operation:', {
            type: operation.type,
            collection: operation.collection,
            details: operation
        });
    }

    // Add more detailed logging for specific operations
    async saveToDB(packData, users) {
        logger.info('Would save to database:', {
            pack: {
                rkey: packData.rkey,
                name: packData.name,
                creator: packData.creator,
                userCount: packData.user_count
            },
            users: users.map(u => ({
                did: u.did,
                handle: u.handle,
                updatedAt: u.last_updated
            }))
        });

        this.logOperation({
            type: 'save',
            collections: ['starter_packs', 'users'],
            data: {
                pack: packData,
                userCount: users.length
            }
        });
    }

    // Mock all other DB operations with detailed logging
    async createCollection(name) {
        this.logOperation({
            type: 'create_collection',
            collection: name
        });
        return true;
    }

    async setupIndexes() {
        this.logOperation({
            type: 'setup_indexes',
            indexes: {
                users: ['did', 'handle', 'last_updated'],
                starter_packs: ['rkey', 'updated_at']
            }
        });
        return true;
    }
}

class DatabaseManager {
    constructor(mongoClient, dbType, logger, dbName = 'starterpacks') {
        this.client = mongoClient;
        this.dbType = dbType;
        this.logger = logger || console;
        this.dbName = dbName;
        this.db = null;
        this.isCosmosDb = DB_INFO[dbType]?.isCosmosDb || false;
        this.lastOperation = Date.now();
        this.operationDelay = 1000; // Base delay between operations
        this.consecutiveThrottles = 0;
        this.maxConsecutiveThrottles = 5;
        this.baseBackoffDelay = 1000;
    }

    async connect(maxRetries = 2) {
        logger.debug('connect');
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.client.connect();
                this.db = this.client.db(this.dbName);
                if (!this.isCosmosDb) {
                    await this.db.command({ ping: 1 });
                }
                return;
            } catch (err) {
                if (attempt === maxRetries - 1) throw err;
                await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
            }
        }
    }

    async init() {
        logger.debug('DB Manager init');
        try {
            await this.connect();
            await this.setupCollections();
            if (!this.isCosmosDb) {
                await this.setupIndexes();
            }
        } catch (err) {
            this.logger.error(`Database initialization failed: ${err.message}`);
            throw err;
        }
    }

    async setupCollections() {
        logger.debug('setupCollections');
        const collections = ['starter_packs', 'users']; // , 'schema_versions'
        const existing = await this.withRetry(
            () => this.db.listCollections().toArray(),
            'list collections'
        );
        const existingNames = new Set(existing.map(c => c.name));

        for (const collection of collections) {
            if (!existingNames.has(collection)) {
                logger.debug('setupCollection:', collection);
                await this.withRetry(
                    async () => {
                        try {
                            await this.db.createCollection(collection);
                            await new Promise(resolve => setTimeout(resolve, 1000));
                        } catch (err) {
                            if (err.code !== 48) throw err; // Ignore if exists
                        }
                    },
                    `create collection ${collection}`
                );
            }
        }
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async getCollectionCounts() {
        try {
            // Use cached counts if recent enough
            const now = Date.now();
            if (this.countCache && (now - this.lastCountCheck) < this.countCacheTimeout) {
                return this.countCache;
            }

            // Add delay before counting to respect rate limits
            await this.delay(1000);

            const counts = {};
            for (const collection of ['users', 'starter_packs']) {
                try {
                    counts[collection] = await this.db.collection(collection).countDocuments();
                    await this.delay(500); // Add delay between counts
                } catch (err) {
                    if (this.isCosmosThrottlingError(err)) {
                        // Return cached counts on throttling
                        if (this.countCache) {
                            this.logger?.warn('Using cached counts due to rate limit');
                            return this.countCache;
                        }
                        // If no cache, return null but don't throw
                        return null;
                    }
                    throw err;
                }
            }

            // Cache the results
            this.countCache = counts;
            this.lastCountCheck = now;
            await this.delay(1000);
            return counts;
        } catch (err) {
            this.logger?.error(`Error getting collection counts: ${err.message}`);
            return null;
        }
    }

    calculateBackoff(retryAfterMs) {
        // Use the RetryAfterMs from Cosmos DB if available
        if (retryAfterMs) {
            return retryAfterMs + 100; // Add small buffer
        }

        // Otherwise use exponential backoff
        const backoff = this.baseBackoffDelay * Math.pow(2, this.consecutiveThrottles);
        return Math.min(backoff, 30000); // Cap at 30 seconds
    }

    async enforceOperationDelay() {
        if (this.isCosmosDb) {
            const timeSinceLastOp = Date.now() - this.lastOperation;
            if (timeSinceLastOp < this.operationDelay) {
                await this.delay(this.operationDelay - timeSinceLastOp);
            }
            this.lastOperation = Date.now();
        }
    }

    async handleThrottlingError(err) {
        this.consecutiveThrottles++;
        
        // Extract RetryAfterMs from error message
        let retryAfterMs = 1000;
        const match = err.message.match(/RetryAfterMs=(\d+)/);
        if (match) {
            retryAfterMs = parseInt(match[1]);
            this.logger?.debug(`retry after: ${err.message}`);
            this.logger?.debug(`so we should wait RetryAfterMs=${retryAfterMs},${retryAfterMs}`);
        }

        const waitTime = this.calculateBackoff(retryAfterMs);
        this.logger?.info(`Rate limit hit, waiting ${waitTime}ms`);
        await this.delay(waitTime);

        // Increase operation delay if we're getting too many throttles
        if (this.consecutiveThrottles > this.maxConsecutiveThrottles) {
            this.operationDelay = Math.min(this.operationDelay * 1.5, 5000);
        }
    }

    async withRetry(operation, name, maxRetries = 5) {
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                return await operation();
            } catch (err) {
                if (err.code === 16500 || err.code === 429 || err.message?.includes('TooManyRequests')) {
                    const retryAfterMs = err.RetryAfterMs || 1000 * Math.pow(2, attempt);
                    this.logger.warn(`Rate limit hit on ${name}, waiting ${retryAfterMs}ms before retry ${attempt + 1}/${maxRetries}`);
                    await new Promise(resolve => setTimeout(resolve, retryAfterMs));
                    continue;
                }
                throw err;
            }
        }
        throw new Error(`Max retries (${maxRetries}) exceeded for ${name}`);
    }

    async safeWrite(collection, operation, options = {}) {
        const maxRetries = 5;
        let lastError = null;
    
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.enforceOperationDelay();
    
                // Add safety check for pack_ids operations
                if (operation.update.$addToSet?.pack_ids) {
                    const doc = await this.db.collection(collection)
                        .findOne(operation.filter, { projection: { _id: 1, pack_ids: 1 }});
    
                    if (!doc) {
                        await this.db.collection(collection).insertOne({
                            ...operation.filter,
                            pack_ids: [operation.update.$addToSet.pack_ids],
                            ...(operation.update.$set || {})
                        });
                    } else {
                        const currentPackIds = Array.isArray(doc.pack_ids) ? doc.pack_ids : [];
                        if (!currentPackIds.includes(operation.update.$addToSet.pack_ids)) {
                            await this.db.collection(collection).updateOne(
                                operation.filter,
                                { 
                                    $set: { 
                                        pack_ids: [...currentPackIds, operation.update.$addToSet.pack_ids],
                                        ...(operation.update.$set || {})
                                    }
                                }
                            );
                        }
                    }
                } else {
                    await this.db.collection(collection).updateOne(
                        operation.filter,
                        operation.update,
                        { ...options, upsert: true }
                    );
                }
    
                this.consecutiveThrottles = 0;
                return;
    
            } catch (err) {
                lastError = err;
                if (this.isCosmosThrottlingError(err)) {
                    await this.handleThrottlingError(err);
                    continue;
                }
                throw err;
            }
        }
    
        throw lastError;
    }

    async safeBulkWrite(collection, operations, options = {}) {
        if (!Array.isArray(operations) || operations.length === 0) {
            logger.debug('No operations to bulk write');
            return;
        }
        
        const formattedOps = operations.map(op => {
            if (!op.updateOne && !op.insertOne && !op.deleteOne) {
                // Convert to proper format if needed
                return {
                    updateOne: {
                        filter: op.filter,
                        update: op.update,
                        upsert: true
                    }
                };
            }
            return op;
        });
        
        if (this.isCosmosDb) {
            // For Cosmos DB, process operations sequentially
            const batchSize = 1; // Process one at a time for maximum safety
            
            for (let i = 0; i < operations.length; i += batchSize) {
                const batch = operations.slice(i, i + batchSize);
                
                for (const op of batch) {
                    await this.safeWrite(collection, {
                        filter: op.filter || op.updateOne.filter,
                        update: op.update || op.updateOne.update,
                        upsert: true
                    }, options);
                }

                // Log progress periodically
                if ((i + batchSize) % 10 === 0) {
                    const progress = ((i + batch.length) / operations.length * 100).toFixed(1);
                    this.logger?.info(`Processed ${i + batch.length}/${operations.length} operations (${progress}%)`);
                }
            }
        } else {
            try {
                    // Original MongoDB bulk write logic
                await this.db.collection(collection).bulkWrite(formattedOps, {
                    ordered: false,
                    ...options
                });
            } catch (err) {
                // Handle bulk write errors
                if (err.code === 11000) { // Duplicate key error
                    logger.warn(`Duplicate key errors in bulk write, falling back to individual writes`);
                    for (const op of formattedOps) {
                        try {
                            await this.safeWrite(collection, {
                                filter: op.updateOne.filter,
                                update: op.updateOne.update
                            });
                        } catch (innerErr) {
                            if (innerErr.code !== 11000) throw innerErr;
                        }
                    }
                } else {
                    throw err;
                }
            }
        } 
        
    }

    isCosmosThrottlingError(err) {
        return err.code === 16500 || 
               err.code === 429 || 
               err.message?.includes('TooManyRequests') ||
               err.message?.includes('Request rate is large') ||
               err.message?.includes('RetryAfterMs');
    }

    async setupIndexes() {
        if (this.isCosmosDb) return; // Skip for Cosmos DB
    
        for (const [collection, indexes] of Object.entries({
            users: [
                { key: { did: 1 }, options: { unique: true, background: true } },
                { key: { handle: 1 }, options: { background: true } }, // Removed unique constraint
                { key: { pack_ids: 1 }, options: { background: true } },
                { key: { last_updated: 1 }, options: { background: true } }
            ],
            starter_packs: [
                { key: { rkey: 1 }, options: { unique: true, background: true } },
                { key: { creator_did: 1 }, options: { background: true } },
                { key: { updated_at: 1 }, options: { background: true } }
            ]
        })) {
            const existing = await this.withRetry(
                () => this.db.collection(collection).indexes(),
                `get ${collection} indexes`
            );
            
            const existingKeys = new Set(existing.map(idx => JSON.stringify(idx.key)));
    
            for (const index of indexes) {
                const indexKey = JSON.stringify(index.key);
                if (!existingKeys.has(indexKey)) {
                    await this.withRetry(
                        () => this.db.collection(collection).createIndex(
                            index.key,
                            index.options
                        ),
                        `create index ${collection}.${indexKey}`
                    );
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            }
        }
    }

    async markPackDeleted(rkey, reason) {
        logger.debug('markPackDeleted');
        try {
            // First mark the pack as deleted
            await this.safeWrite('starter_packs', {
                filter: { rkey },
                update: {
                    $set: {
                        deleted: true,
                        deleted_at: new Date(),
                        deletion_reason: reason
                    }
                }
            });
    
            // Update affected users
            await this.safeBulkWrite('users', [{
                updateMany: {
                    filter: { pack_ids: rkey },
                    update: { 
                        $pull: { pack_ids: rkey },
                        $set: { last_updated: new Date() }
                    }
                }
            }]);
    
            logger.info(`Pack ${rkey} marked as deleted`);
        } catch (err) {
            logger.error(`Failed to mark pack ${rkey} as deleted:`, err);
            throw err;
        }
    }

    async cleanupRemovedUsers(rkey, removedDids) {
        logger.debug('cleanupRemovedUsers');
        try {
            // Update users that were removed from the pack
            const bulkOps = removedDids.map(did => ({
                updateOne: {
                    filter: { did },
                    update: {
                        $pull: { pack_ids: rkey },
                        $set: { last_updated: new Date() }
                    }
                }
            }));

            await this.safeBulkWrite('users', bulkOps);

            // Clean up users with no packs
            await this.safeWrite('users', {
                filter: {
                    did: { $in: removedDids },
                    $or: [
                        { pack_ids: { $size: 0 } },
                        { pack_ids: { $exists: false } }
                    ]
                },
                update: {
                    $set: { 
                        deleted: true, 
                        deleted_at: new Date() 
                    }
                }
            });

            logger.info(`Cleaned up ${removedDids.length} removed users for pack ${rkey}`);
        } catch (err) {
            logger.error(`Failed to cleanup removed users for pack ${rkey}:`, err);
            throw err;
        }
    }
}

class PackTracker {
    constructor() {
        this.knownPacks = new Set();
        this.unprocessedPacks = new Set();
        this.missingPacks = new Map();
        this.completedPacks = new Set();
        this.maxAttempts = 3;
        this.retryAfterDays = 7;
    }

    async loadFromFile(filename) {
        try {
            const content = await fs.readFile(filename, 'utf-8');
            const lines = content.split('\n')
                .filter(line => line.trim() && line.includes('|'));

            for (const line of lines) {
                const [_, rkey] = line.split('|').map(s => s.trim());
                this.knownPacks.add(rkey);
                if (!this.completedPacks.has(rkey) && !this.missingPacks.has(rkey)) {
                    this.unprocessedPacks.add(rkey);
                }
            }
        } catch (err) {
            logger.error(`Error loading known packs: ${err.message}`);
            throw err;
        }
    }

    shouldProcess(rkey) {
        // Always process if unprocessed
        if (this.unprocessedPacks.has(rkey)) return true;
        
        // Skip if completed
        if (this.completedPacks.has(rkey)) return false;

        // Check missing pack status
        if (this.missingPacks.has(rkey)) {
            const status = this.missingPacks.get(rkey);
            const daysSinceLastAttempt = 
                (Date.now() - status.lastAttempt) / (1000 * 60 * 60 * 24);
            
            return status.attempts < this.maxAttempts && 
                   daysSinceLastAttempt >= this.retryAfterDays;
        }

        return true;
    }

    recordMissing(rkey, reason = '') {
        const now = Date.now();
        const existing = this.missingPacks.get(rkey);

        if (existing) {
            existing.attempts++;
            existing.lastAttempt = now;
            existing.reason = reason || existing.reason;
        } else {
            this.missingPacks.set(rkey, {
                firstAttempt: now,
                lastAttempt: now,
                attempts: 1,
                reason
            });
        }

        this.unprocessedPacks.delete(rkey);
    }

    markCompleted(rkey) {
        this.completedPacks.add(rkey);
        this.unprocessedPacks.delete(rkey);
        this.missingPacks.delete(rkey);
    }

    getStats() {
        return {
            known: this.knownPacks.size,
            unprocessed: this.unprocessedPacks.size,
            missing: this.missingPacks.size,
            completed: this.completedPacks.size
        };
    }
}

class MainProcessor {
    constructor(options = {}) {
        const {
            noMongoDB = false,
            noDBWrites = false,
            fromApi = false,
            debug = false
        } = options;

        this.noMongoDB = noMongoDB;
        this.noDBWrites = noDBWrites; 
        this.fromApi = fromApi;
        this.debug = debug;

        // Initialize basic components first
        this.rateLimiter = new RateLimiter();
        this.fileHandler = new FileHandler();
        
        this.packTracker = new PackTracker();
        this.validator = new ValidationHelper(VALIDATION_SCHEMAS);
        this.metrics = metrics;
        this.cleanupHandlers = new Set();
        this.debugManager = new DebugManager({ debug: options.debug });

        // Handle DB initialization based on flags
        if (!noMongoDB) {
            if (noDBWrites) {
                this.dbManager = new MockDatabaseManager();
            } else {
                const isCosmosDb = process.env.DB_TYPE === 'cosmos';
                this.mongoClient = new MongoClient(process.env.MONGODB_URI);
                this.dbManager = new DatabaseManager(this.mongoClient, isCosmosDb);
            }
        }

        this.profileCache = new Map();
        this.profileCacheTTL = 24 * 60 * 60 * 1000;
    }

    async handleProfileError(err, context) {
        const { profile, processingId } = context;
        
        if (err.status === 429 || this.isRateLimitError(err)) {
            await this.rateLimiter.handleResponse(err);
            return { retry: true };
        }
        
        if (err.status === 404 || this.isNotFoundError(err)) {
            await this.markProfileMissing(profile.did, err.message);
            return { retry: false };
        }
        
        // Log error with context
        logger.error('Profile processing error', {
            did: profile.did,
            handle: profile.handle,
            processingId,
            error: err.message,
            stack: err.stack,
            context
        });
        
        return { retry: this.isRetryableError(err) };
    }

    async handleError(err, context) {
        const { operation, data } = context;
        
        metrics.recordError(operation, err);
        
        if (this.verificationHandler) {
            // Log for verification history
            const errorEntry = {
                timestamp: new Date().toISOString(),
                operation,
                error: err.message,
                context: data
            };
            this.verificationHandler.verificationResults.set(
                `${operation}-${Date.now()}`, 
                errorEntry
            );
        }
    
        if (this.isRateLimitError(err)) {
            await this.rateLimiter.handleResponse(err);
            return { retry: true };
        }
    
        return { retry: this.isRetryableError(err) };
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async isRetryableError(err) {
        return (
            err.status === 429 ||
            err.status >= 500 ||
            err.message?.includes('network') ||
            err.message?.includes('timeout') ||
            err.code === 'ECONNRESET'
        );
    }

    async markProfileMissing(did, reason) {
        if (!this.missingProfiles) {
            this.missingProfiles = new Set();
        }
        this.missingProfiles.add(did);
        logger.warn(`Marked profile as missing: ${did} (${reason})`);
    }

    async refreshSession() {
        try {
            await this.agent.login({
                identifier: process.env.BSKY_USERNAME,
                password: process.env.BSKY_PASSWORD
            });
            logger.info('Session refreshed successfully');
        } catch (err) {
            logger.error('Session refresh failed:', err);
            throw err;
        }
    }

    async initializeApiComponents() {
        logger.debug('ApiComponents initializing');
        // Initialize and authenticate with BlueSky
        this.agent = new BskyAgent({
            service: API_CONFIG.baseURLs.primary
        });

        try {
            // Perform authentication
            await this.agent.login({
                identifier: process.env.BSKY_USERNAME,
                password: process.env.BSKY_PASSWORD
            });
            
            // Create API handler after successful authentication
            this.apiHandler = new ApiHandler(this.agent, this.rateLimiter);
            
            // Initialize verification handler with authenticated API handler
            logger.debug('ErrorVerificationHandler initializing');
            
            this.verificationHandler = new ErrorVerificationHandler({
                debugManager: this.debugManager,
                metrics: this.metrics,
                fileHandler: this.fileHandler,
                dbManager: this.dbManager,
                apiHandler: this.apiHandler
            });

            logger.debug('API components initialized successfully');
        } catch (authError) {
            logger.error('Authentication failed:', authError);
            throw new Error('Failed to authenticate with BlueSky');
        }
    }

    async init() {
        try {
            // Step 0: Initialize Bluesky API components
            logger.debug('Initialize Bluesky API components');
            await this.initializeApiComponents();
            
            // Step 1: Initialize file handler first
            logger.debug('Initialize file handler');
            await this.fileHandler.init();
    
            // Step 2: Create TaskManager
            logger.debug('Create TaskManager');
            this.taskManager = new TaskManager(this.fileHandler, this.debug);
            
            // Step 3: Load tasks and data
            logger.debug('Load tasks and data');
            await this.taskManager.initializeTaskList();
            
            // Step 4: Initialize database if needed
            logger.debug('Setting up handling of Mongo-DB', this.noDBWrites)
            if (!this.noMongoDB) {
                if (this.noDBWrites) {
                    logger.info('Running in no-(Mongo-)DB-writes mode - operations will be logged only');
                    this.dbManager = new MockDatabaseManager();
                    await this.dbManager.init();
                } else {
                    const dbType = process.env.DB_TYPE || 'cosmos';
                    const dbConfig = {
                        ...DB_CONFIGS[dbType],
                        maxPoolSize: 10,
                        minPoolSize: 5,
                        waitQueueTimeoutMS: 30000,
                        serverSelectionTimeoutMS: 30000
                    };

                    this.mongoClient = new MongoClient(process.env.MONGODB_URI, dbConfig);
                    this.dbManager = new DatabaseManager(
                        this.mongoClient,
                        dbType,  // Pass dbType instead of isCosmosDb
                        this.logger,
                        'starterpacks',
                        // BATCH_SIZES[dbType] || BATCH_SIZES.cosmos
                    );
                    await this.dbManager.init();
                }
            }
    
            // Step 6: Verify system state
            logger.debug('Verify system state');
            const verificationResults = await this.verificationHandler.verifySystemState();
            
            if (this.debug) {
                logger.debug('System verification results:', verificationResults);
            }
    
            logger.info('Initialization complete');
        } catch (err) {
            logger.error(`Initialization failed: ${err.message}`);
            throw err;
        }
    }

    async processStarterPack(urlLine) {
        const [handle, rkey] = urlLine.split('|').map(s => s.trim());
        if (!handle || !rkey) {
            throw new Error('Invalid URL line format');
        }

        logger.info(`Processing pack: ${handle}|${rkey}`);
        logger.info(`Progress: ${this.taskManager.completedTaskCount}/${this.taskManager.totalTasks} packs`);
    
        try {
            const startTime = Date.now();
            
            // 1. Initial checks and state loading 
            if (this.taskManager.completedTasks.has(rkey)) {
                logger.debug(`Skipping already processed pack: ${rkey}`);
                return true;
            }
    
            // Load existing pack state for comparison
            const existingPack = await this.fileHandler.getPack(rkey);
            const existingUsers = existingPack ? new Set(existingPack.users) : new Set();
    
            // 2. Resolve creator and fetch pack
            const creatorDID = await this.apiHandler.resolveHandle(handle);
            if (!creatorDID) {
                logger.warn(`Creator not found for pack ${rkey}`);
                if (existingPack && !this.noMongoDB && !this.noDBWrites) {
                    await this.dbManager.markPackDeleted(rkey, 'creator_not_found');
                }
                this.taskManager.recordFailure(rkey, 'creator_not_found');
                return false;
            }
    
            const packUri = `at://${creatorDID}/app.bsky.graph.starterpack/${rkey}`;
            const pack = await this.apiHandler.makeAuthApiCall('app.bsky.graph.getStarterPack', { starterPack: packUri });
    
            if (!pack?.starterPack?.record?.list) {
                logger.warn(`Invalid pack structure for ${rkey}`);
                if (existingPack && !this.noMongoDB && !this.noDBWrites) {
                    await this.dbManager.markPackDeleted(rkey, 'invalid_pack_structure');
                }
                this.taskManager.recordFailure(rkey, 'invalid_pack_structure');
                return false;
            }
    
            // 3. Get and process list members
            logger.debug(`Getting list ${pack.starterPack.record.list}`);
            const listMembers = await this.apiHandler.makeApiCall('app.bsky.graph.getList', { list: pack.starterPack.record.list });
            
            if (!listMembers?.items?.length) {
                logger.warn(`Empty list for pack ${rkey}`);
                if (existingPack && !this.noMongoDB && !this.noDBWrites) {
                    await this.dbManager.markPackDeleted(rkey, 'empty_list');
                }
                this.taskManager.recordFailure(rkey, 'empty_list');
                return false;
            }
    
            const processedUsers = await this.processListMembers(listMembers, rkey);
    
            // 4. Handle removed users BEFORE saving new state
            const currentUsers = new Set(processedUsers.map(u => u.did));
            const removedDids = Array.from(existingUsers).filter(did => !currentUsers.has(did));
    
            if (removedDids.length > 0) {
                logger.info(`${removedDids.length} users removed from pack ${rkey}`);
                if (!this.noMongoDB && !this.noDBWrites) {
                    await this.dbManager.cleanupRemovedUsers(rkey, removedDids);
                }
            }
    
            // 5. Prepare and validate pack data
            const packData = {
                rkey,
                name: pack.starterPack.record.name,
                creator: handle,
                creator_did: creatorDID,
                description: pack.starterPack.record.description || '',
                user_count: processedUsers.length,
                created_at: existingPack?.created_at || new Date().toISOString(),
                updated_at: new Date().toISOString(),
                users: processedUsers.map(u => u.did),
                weekly_joins: pack.starterPack.joinedWeekCount || 0,
                total_joins: pack.starterPack.joinedAllTimeCount || 0,
                previous_user_count: existingPack?.user_count || 0,
                previous_update: existingPack?.updated_at || null
            };
    
            // 6. Save data with proper ordering
            // First save to file system
            await this.fileHandler.appendPack(packData);
            for (const user of processedUsers) {
                await this.fileHandler.appendUser(user);
            }
    
            // Then save to MongoDB if enabled
            if (!this.noMongoDB) {
                await this.saveToDB(packData, processedUsers);
            }

            if (this.debug) {
                const dbCounts = await this.dbManager.getCollectionCounts();
                logger.debug('Current database status:', {
                    collections: dbCounts,
                    pendingTasks: this.taskManager.pendingTasks.size,
                    completedTasks: this.taskManager.completedTasks.size
                });
            }
    
            // 7. Update task state and metrics
            await this.taskManager.markTaskCompleted(rkey);
            metrics.recordPackProcessing(true, Date.now() - startTime);
    
            // Log completion
            if (this.debug) {
                const duration = Date.now() - startTime;
                logger.debug('Pack processing completed', {
                    rkey,
                    duration,
                    userCount: processedUsers.length,
                    usersProcessed: processedUsers.length,
                    usersRemoved: removedDids.length
                });
            }
    
            return true;
    
        } catch (err) {
            // 8. Error handling with proper cleanup
            if (err.status === 404) {
                // Pack no longer exists at Bluesky
                if (!this.noMongoDB && !this.noDBWrites) {
                    await this.dbManager.markPackDeleted(rkey, 'pack_not_found');
                }
                this.taskManager.recordFailure(rkey, 'Pack no longer exists', true);
                metrics.recordPackProcessing(false);
                return false;
            }
            
            if (err.status === 429) {
                await this.rateLimiter.handleResponse(err);
                return false;
            }
    
            // Log error with context
            logger.error(`Error processing pack ${rkey}:`, {
                error: err.message,
                status: err.status,
                stack: err.stack,
                handle,
                rkey
            });
    
            this.taskManager.recordFailure(rkey, err.message, err.status === 404);
            metrics.recordPackProcessing(false);
            return false;
        }
    }

    async getListMembers(uri) {
        try {
            logger.debug(`getListMembers for uri: ${uri}`)
            // Try public API first
            try {
                const publicData = await this.makePublicApiCall('app.bsky.graph.getList', {
                    list: uri,
                    limit: 100
                });
                if (publicData?.list?.items) return publicData.list.items;
            } catch (err) {
                if (err.status !== 404) {
                    logger.warn(`Public API list fetch failed: ${err.message}`);
                }
            }
    
            // Fall back to authenticated API
            const response = await this.agent.api.app.bsky.graph.getList({
                list: uri,
                limit: 100
            });
            return response?.data?.list?.items || [];
        } catch (err) {
            logger.error(`Error getting list members for ${uri}: ${err.message}`);
            return [];
        }
    }

    async getStarterPack(uri) {
        try {
            const response = await this.agent.api.app.bsky.graph.getStarterPack({ 
                starterPack: uri 
            });
            return response?.data?.starterPack;
        } catch (err) {
            if (err.status === 404) {
                logger.error(`Starter pack not found: ${uri}`);
                return null;
            }
            throw err;
        }
    }

    async getProfile(did) {
        try {
            // Try public API first
            try {
                const publicData = await this.apiHandler.makeApiCall('app.bsky.actor.getProfile', { actor: did });
                if (publicData) return publicData;
            } catch (err) {
                if (err.status !== 404) {
                    logger.warn(`Public API profile fetch failed for ${did}: ${err.message}`);
                }
            }
    
            // Fall back to authenticated API
            const response = await this.agent.getProfile({ actor: did });
            return response?.data;
        } catch (err) {
            if (err.status === 401) {
                await this.agent.login({
                    identifier: process.env.BSKY_USERNAME,
                    password: process.env.BSKY_PASSWORD
                });
                return this.getProfile(did);
            }
            throw err;
        }
    }

    async processAssociatedPacks(profile, options = {}) {
        const {
            maxDepth = MAX_PACK_DEPTH,  // Now using the global constant
            processedDIDs = new Set(),
            currentDepth = 0,
            parentHandle = null
        } = options;
    
        const results = {
            discovered: 0,
            queued: 0,
            skipped: 0,
            failed: 0
        };
    
        if (currentDepth >= maxDepth || processedDIDs.has(profile.did)) {
            logger.debug(`Skipping associated packs for ${profile.did} (depth: ${currentDepth}, maxDepth: ${maxDepth})`);
            return results;
        }
    
        processedDIDs.add(profile.did);
    
        try {
            // Get packs for current profile
            logger.debug(`Fetching packs for ${profile.handle} (depth: ${currentDepth})`);
            const packs = await this.apiHandler.getActorStarterPacks(profile.did);
            
            if (!packs?.starterPacks?.length) {
                return results;
            }
    
            results.discovered = packs.starterPacks.length;
            logger.info(`Found ${results.discovered} associated packs for ${profile.handle}`);
    
            // Process each pack
            for (const pack of packs.starterPacks) {
                try {
                    const rkey = await this.extractRkeyFromURI(pack.uri);

                    // Add to URLs file
                    await this.fileHandler.appendToUrlsFile(profile.handle, rkey);
                    
                    const shouldProcess = this.taskManager.shouldProcessPack(
                        rkey,
                        await this.fileHandler.getPack(rkey),
                        this.taskManager.failures.get(rkey)
                    );
    
                    if (!shouldProcess.process) {
                        results.skipped++;
                        logger.debug(`Skipping pack ${rkey}: ${shouldProcess.reason}`);
                        continue;
                    }
    
                    // Add to task queue
                    const added = await this.taskManager.addAssociatedPack({
                        creator: profile.handle,
                        rkey,
                        memberCount: pack.starterPack?.record?.items?.length || 0,
                        updatedAt: pack.starterPack?.record?.createdAt || new Date().toISOString()
                    }, profile.did);
    
                    if (added) {
                        results.queued++;
                        this.taskManager.markDirty();  // Mark for checkpoint update
                        this.taskManager.recordPackRelationship(rkey, profile.did);
                        await this.taskManager.maybeWriteCheckpoint();
                        logger.info(`Queued associated pack ${rkey} from ${profile.handle}`);
                    } else {
                        results.skipped++;
                    }
    
                } catch (err) {
                    results.failed++;
                    logger.error(`Failed to process associated pack ${pack.uri}:`, err);
                }
    
                await this.rateLimiter.throttle();
            }
    
            metrics.recordAssociatedPacksMetrics(results);

            if (this.debug) {
                logger.debug('Associated packs processing complete', {
                    profile: profile.handle,
                    results,
                    depth: currentDepth
                });
            }

            return results;
    
        } catch (err) {
            logger.error(`Failed to process associated packs for ${profile.did}:`, err);
            metrics.recordAssociatedPacksMetrics({
                discovered: 0,
                queued: 0,
                skipped: 0,
                failed: 1
            });
            throw err;
        }
    }
    
    async extractRkeyFromURI(uri) {
        const match = uri.match(/starterpack\/([a-zA-Z0-9]+)$/);
        if (!match) {
            throw new Error(`Invalid starter pack URI: ${uri}`);
        }
        const rkey = match[1];
        logger.debug('rkey extracted:', rkey)
        return rkey;
    }

    async processPackMember(member, packRkey) {
        const memberDid = member.did || (member.subject && member.subject.did);
        if (!memberDid) {
            logger.warn(`Could not extract DID from member: ${JSON.stringify(member)}`);
            return null;
        }
    
        try {
            // Check cache first
            let profile = this.profileCache.get(memberDid);
            const now = Date.now();
            const cacheTime = profile?.cacheTime || 0;
    
            if (!profile || (now - cacheTime > this.profileCacheTTL)) {
                const fetchedProfile = await this.getProfile(memberDid);
                if (fetchedProfile) {
                    profile = {
                        did: fetchedProfile.did,
                        handle: fetchedProfile.handle,
                        displayName: fetchedProfile.displayName || '',
                        followers_count: fetchedProfile.followersCount || 0,
                        follows_count: fetchedProfile.followsCount || 0,
                        pack_ids: [packRkey],
                        last_updated: new Date().toISOString(),
                        cacheTime: now
                    };
                    this.profileCache.set(memberDid, profile);
                }
            } else {
                // Update pack_ids for existing profile
                if (!profile.pack_ids?.includes(packRkey)) {
                    profile.pack_ids = [...(profile.pack_ids || []), packRkey];
                }
            }
    
            return profile;
    
        } catch (err) {
            logger.warn(`Error processing member ${memberDid}:`, err);
            return null;
        }
    }

    async processProfile(profile, options = {}) {
        const {
            rkey = null,
            force = false,
            processAssociated = true,
            parentPack = null,
            processingId = `${Date.now()}-${Math.random()}`,
            source = 'direct'
        } = options;
    
        const startTime = Date.now();
        const processedPacks = new Set();
        const changes = {
            renamed: [],
            updated: [],
            added: [],
            failed: [],
            removed: [],
            discovered: [],
            packs: {
                discovered: 0,
                processed: 0,
                failed: 0
            }
        };
    
        if (this.debug) {
            logger.debug('Starting profile processing', {
                did: profile.did,
                handle: profile.handle,
                hasAssociatedPacks: !!profile.associated?.starterPacks,
                parentPack,
                processingId,
                source
            });
        }
    
        try {
            // 1. Validate profile existence and format
            if (!profile || !profile.did || !profile.handle) {
                if (!this.noMongoDB && !this.noDBWrites) {
                    logger.debug('notifying MongoDB about profile_not_found_or_invalid:', options.did);
                    await this.dbManager.safeWrite('users', {
                        filter: { did: options.did || profile?.did },
                        update: { 
                            $set: { 
                                deleted: true, 
                                deleted_at: new Date(),
                                deletion_reason: 'profile_not_found_or_invalid',
                                last_check: new Date()
                            } 
                        }
                    });
                }
                await this.taskManager.recordMissingProfile(options.did || profile?.did);
                return {
                    success: false,
                    changes,
                    error: 'Profile not found or invalid'
                };
            }
    
            // 2. Validation of data format
            const validationStart = Date.now();
            if (!this.validator.validateUser({
                did: profile.did,
                handle: profile.handle,
                last_updated: new Date().toISOString()
            })) {
                throw new Error('Invalid profile data format');
            }
            metrics?.recordOperation('validation', Date.now() - validationStart);
    
            // 3. Load and check existing state
            const existing = await this.fileHandler.getUser(profile.did);
            
            // Handle cache unless forced
            if (existing && !force) {
                const lastUpdated = new Date(existing.last_updated);
                const daysSinceUpdate = (Date.now() - lastUpdated.getTime()) / (1000 * 60 * 60 * 24);
                if (daysSinceUpdate < 7) {
                    if (this.debug) {
                        logger.debug('Using cached profile', {
                            did: profile.did,
                            daysSinceUpdate,
                            processingId
                        });
                    }
                    return { 
                        success: true, 
                        changes,
                        cached: true 
                    };
                }
            }
    
            // 4. Check for changes and record them
            if (existing) {
                if (existing.handle !== profile.handle) {
                    changes.renamed.push({
                        did: profile.did,
                        oldHandle: existing.handle,
                        newHandle: profile.handle,
                        timestamp: new Date().toISOString()
                    });
                }
                changes.updated.push(profile.did);
            } else {
                changes.added.push(profile.did);
            }
    
            // 5. Prepare new user data
            const userData = {
                did: profile.did,
                handle: profile.handle,
                display_name: profile.displayName || '',
                followers_count: profile.followersCount,
                follows_count: profile.followsCount,
                associated: profile.associated || {},
                last_updated: new Date().toISOString(),
                pack_ids: [...new Set([...(existing?.pack_ids || []), rkey].filter(Boolean))],
                handle_history: changes.renamed.length > 0 ? 
                    [...(existing?.handle_history || []), changes.renamed[0]] : 
                    existing?.handle_history,
                last_check: new Date()
            };
    
            if (this.debug) {
                logger.debug('Prepared user data:', {
                    userData,
                    changes
                });
            }
    
            // 6. Save data with proper ordering
            // First save to file system
            const fileStart = Date.now();
            await this.fileHandler.appendUser(userData);
            metrics?.recordOperation('file_write', Date.now() - fileStart);
    
            // Then save to MongoDB if enabled
            if (!this.noMongoDB) {
                if (this.noDBWrites) {
                    await this.dbManager.logOperation({
                        type: 'upsert',
                        collection: 'users',
                        filter: { did: profile.did },
                        data: userData,
                        changes: changes.renamed.length > 0 ? changes.renamed : undefined
                    });
                } else {
                    const dbStart = Date.now();
                    logger.debug('Preparing MongoDB write for ', profile.did);
                    await this.dbManager.safeWrite('users', {
                        filter: { did: profile.did },
                        update: {
                            $set: userData,
                            ...(rkey ? { $addToSet: { pack_ids: rkey } } : {})
                        },
                        upsert: true
                    });
                    metrics?.recordOperation('db_write', Date.now() - dbStart);
                }
            }
    
            // 7. Process associated packs if appropriate
            if ((profile.associated?.starterPacks > 0 || force) && !parentPack) {
                logger.debug('Associated Pack found:', profile.associated.starterPacks);
                const packResults = await this.processAssociatedPacks(profile, {
                    processedPacks,
                    processingId
                });
    
                // Handle quick processing mode
                if (source === 'quick_process') {
                    while (this.taskManager.pendingTasks.size > 0) {
                        await this.taskManager.processNextTask(this);
                        await this.taskManager.maybeWriteCheckpoint();
                    }
                }
    
                changes.packs = packResults;
            }
    
            // 8. Record metrics
            metrics?.recordUserProcessing(true);
            const duration = Date.now() - startTime;
            metrics?.recordProfileProcessing(duration);
    
            // 9. Return success with changes and timing
            return {
                success: true,
                changes,
                timing: {
                    total: duration,
                    validation: metrics?.getOperationTime('validation'),
                    fileWrite: metrics?.getOperationTime('file_write'),
                    dbWrite: metrics?.getOperationTime('db_write'),
                    associatedPacks: metrics?.getOperationTime('associated_packs')
                }
            };
    
        } catch (err) {
            // 10. Error handling with proper cleanup
            metrics?.recordUserProcessing(false);
            metrics?.recordError('profile_processing', err);
    
            if (err.status === 404) {
                if (!this.noMongoDB && !this.noDBWrites) {
                    await this.dbManager.safeWrite('users', {
                        filter: { did: profile.did },
                        update: { 
                            $set: { 
                                deleted: true, 
                                deleted_at: new Date(),
                                deletion_reason: 'api_not_found',
                                last_check: new Date()
                            } 
                        }
                    });
                }
                await this.taskManager.recordMissingProfile(profile.did);
            }
    
            // Log error with full context
            logger.error(`Error processing profile ${profile.did}:`, {
                error: err.message,
                status: err.status,
                processingId,
                parentPack,
                duration: Date.now() - startTime,
                stack: err.stack,
                changes
            });
    
            changes.failed.push({
                did: profile.did,
                reason: err.message,
                status: err.status,
                timestamp: new Date().toISOString()
            });
    
            return {
                success: false,
                changes,
                error: err.message,
                status: err.status
            };
        }
    }

    async resolveHandle(handle) {
        try {
            if (!handle) {
                throw new Error('No handle provided');
            }
            // Check if it's already a DID
            if (handle.startsWith('did:')) {
                return handle;
            }
            // Sanitize the handle
            const sanitized = this.sanitizeHandle(handle);
            logger.info(`Attempting to resolve sanitized handle: ${sanitized}`);
    
            // Try public API first
            try {
                const publicData = await this.makePublicApiCall('com.atproto.identity.resolveHandle', 
                    { handle });
                if (publicData?.did) return publicData.did;
            } catch (err) {
                if (err.status !== 404) {
                    logger.warn(`Public API handle resolution failed: ${err.message}`);
                }
            }

            // Fall back to authenticated API
            const response = await this.agent.resolveHandle({ handle });
            
            if (response?.data?.did) {
                logger.info(`Successfully resolved handle ${sanitized} to DID ${response.data.did}`);
                return response?.data?.did;
            } else {
                logger.error(`No DID found in response for handle: ${sanitized}`);
                return null;
            }
        } catch (err) {
            if (err.status === 401) {
                await this.agent.login({
                    identifier: process.env.BSKY_USERNAME,
                    password: process.env.BSKY_PASSWORD
                });
                return this.resolveHandle(handle);
            }
            throw err;
        }
    }

    async processListMembers(listdata, packRkey) {
        //logger.debug(`Processing List `, listdata, packRkey);
        const processedUsers = [];
        const totalMembers = listdata.list.listItemCount;
        let processedCount = 0;
    
        if (this.debug) {
            logger.debug(`Starting to process ${totalMembers} members for pack ${packRkey}`);
        }
    
        for (const member of listdata.items) {
            processedCount++;
            try {
                // Get full profile for the member to check for associated packs
                const memberProfile = await this.apiHandler.getProfile(member.subject.did);
                logger.debug(`Full profile for member ${processedCount} of pack ${packRkey}:`, memberProfile);
                
                if (memberProfile) {
                    const profile = await this.processPackMember(member, packRkey);
                    logger.debug(`Processed profile:`, profile);
                    if (profile) {
                        processedUsers.push(profile);
    
                        // If this member has starter packs, process them
                        if (memberProfile.associated?.starterPacks > 0) {
                            const packResults = await this.processAssociatedPacks(memberProfile, {
                                maxDepth: MAX_PACK_DEPTH,
                                processedDIDs: new Set(),
                                currentDepth: 0,
                                fromPackMember: true  // Flag to indicate this is from pack member processing
                            });
    
                            if (this.debug && packResults.discovered > 0) {
                                logger.debug('Found associated packs from pack member:', {
                                    did: memberProfile.did,
                                    handle: memberProfile.handle,
                                    discovered: packResults.discovered,
                                    queued: packResults.queued
                                });
                            }
                        }
                    }
                }
    
                if (this.debug && processedCount % 10 === 0) {
                    logger.debug(`Processing progress`, {
                        packRkey,
                        progress: `${processedCount}/${totalMembers}`,
                        successRate: `${(processedUsers.length/processedCount*100).toFixed(1)}%`
                    });
                }
            } catch (err) {
                logger.warn(`Failed to process member in pack ${packRkey}:`, err);
                if (err.status === 404) {
                    this.taskManager.recordMissingProfile(member.did);
                }
            }
        }
    
        return processedUsers;
    }

    async saveToDB(packData, users) {
        if (this.noMongoDB) return;
    
        try {
            // First save the pack
            logger.debug('Writing pack to MongoDB:', packData.rkey);
            await this.dbManager.safeWrite('starter_packs', {
                filter: { rkey: packData.rkey },
                update: { $set: packData },
                upsert: true
            });
    
            // Then process users in batches
            logger.debug(`Writing ${users.length} users to MongoDB`);
            const userOperations = users.map(user => ({
                updateOne: {
                    filter: { did: user.did },
                    update: { 
                        $set: {
                            ...user,
                            pack_ids: user.pack_ids // Include pack_ids in $set instead of $addToSet
                        }
                    },
                    upsert: true
                }
            }));
    
            await this.dbManager.safeBulkWrite('users', userOperations);
    
            if (this.debug) {
                const counts = await this.dbManager.getCollectionCounts();
                logger.debug('MongoDB collection counts after write:', {
                    collections: counts,
                    lastOperation: {
                        pack: packData.rkey,
                        usersProcessed: users.length
                    }
                });
            }
        } catch (err) {
            logger.error(`Database write failed for pack ${packData.rkey}:`, err);
            throw err;
        }
    }

    clearInternalState() {
        this.activeOperations = new Set();
        this.processingCache = new Map();
        this.profileCache.clear();
        this.verificationResults?.clear();
    }
    
    isRateLimitError(err) {
        return err.status === 429 || 
               err.message?.includes('rate limit') ||
               err.code === 'RATE_LIMIT';
    }
    
    isNotFoundError(err) {
        return err.status === 404 ||
               err.message?.includes('not found') ||
               err.code === 'NOT_FOUND';
    }

    async cleanup() {
        logger.debug('Starting cleanup process...');
        
        try {
            const cleanupTasks = [];
    
            // Only add cleanup tasks for initialized components
            if (this.fileHandler && typeof this.fileHandler.cleanup === 'function') {
                cleanupTasks.push(
                    Promise.resolve(this.fileHandler.cleanup()).catch(err => {
                        logger.error('Error during file handler cleanup:', err);
                    })
                );
            }
    
            if (this.verificationHandler && typeof this.verificationHandler.cleanup === 'function') {
                cleanupTasks.push(
                    Promise.resolve(this.verificationHandler.cleanup()).catch(err => {
                        logger.error('Error during verification handler cleanup:', err);
                    })
                );
            }
    
            if (this.debugManager && typeof this.debugManager.cleanup === 'function') {
                cleanupTasks.push(
                    Promise.resolve(this.debugManager.cleanup()).catch(err => {
                        logger.error('Error during debug manager cleanup:', err);
                    })
                );
            }
    
            if (!this.noMongoDB) {
                if (this.noDBWrites) {
                    // Log final DB operations summary
                    const summary = this.dbManager.getOperationsSummary();
                    logger.info('Database Operations Summary:', summary);
                } else if (this.mongoClient) {
                    cleanupTasks.push(
                        Promise.resolve(this.mongoClient.close(true)).catch(err => {
                            logger.error('Error closing MongoDB connection:', err);
                        })
                    );
                }
            }
    
            if (this.taskManager && typeof this.taskManager.maybeWriteCheckpoint === 'function') {
                cleanupTasks.push(
                    Promise.resolve(this.taskManager.maybeWriteCheckpoint(true)).catch(err => {
                        logger.error('Error saving final checkpoint:', err);
                    })
                );
            }

            // Only wait if we have tasks
            if (cleanupTasks.length > 0) {
                await Promise.all(cleanupTasks);
            }
    
            // Clear caches and internal state
            if (this.profileCache) {
                this.profileCache.clear();
            }
            this.clearInternalState();
    
            logger.debug('Cleanup completed successfully');
        } catch (err) {
            logger.error('Error during cleanup:', err);
            // Don't rethrow - we're in cleanup
        }
    }
    
}

class RateLimiter {
    constructor() {
        this.requestWindow = 5 * 60 * 1000;  // 5 minutes
        this.maxRequests = 5000;
        this.requests = [];
        this.safetyFactor = 0.9;  // Use 90% of max rate
        this.initialBackoff = 1000;
        this.maxBackoff = 30000;
        this.currentBackoff = this.initialBackoff;
        this.consecutive429s = 0;

        // Clean up old requests periodically
        setInterval(() => {
            const now = Date.now();
            this.requests = this.requests.filter(time => 
                now - time < this.requestWindow
            );
        }, 60000).unref(); // Don't keep process alive for this timer
    }

    reset() {
        this.requests = [];
        this.consecutive429s = 0;
        this.currentBackoff = this.initialBackoff;
    }
    
    async throttle() {
        const now = Date.now();
        this.requests = this.requests.filter(time => now - time < this.requestWindow);
        
        const effectiveLimit = Math.floor(this.maxRequests * this.safetyFactor);
        
        if (this.requests.length >= effectiveLimit) {
            const oldestRequest = this.requests[0];
            const waitTime = (oldestRequest + this.requestWindow) - now;
            await new Promise(resolve => setTimeout(resolve, waitTime));
        }
        
        this.requests.push(now);
    }

    async handleResponse(response) {
        const remaining = response?.headers?.['x-ratelimit-remaining'];
        const reset = response?.headers?.['x-ratelimit-reset'];
        
        // Handle rate limit headers if present
        if (remaining !== undefined && remaining < 100) {
            const delayMs = Math.max(1000, (this.requestWindow / this.maxRequests) * 2);
            await new Promise(resolve => setTimeout(resolve, delayMs));
        }
        
        // Handle 429 responses
        if (response?.status === 429) {
            this.consecutive429s++;
            const waitTime = this.calculateBackoff();
            await new Promise(resolve => setTimeout(resolve, waitTime));
            return true;
        }
        
        if (response?.ok) {
            this.reset(); // Reset after successful response
        }
        return false;
    }

    calculateBackoff() {
        const backoff = Math.min(
            this.initialBackoff * Math.pow(2, this.consecutive429s),
            this.maxBackoff
        );
        return backoff + (Math.random() * 1000); // Add jitter
    }

    getStats() {
        return {
            currentRequests: this.requests.length,
            windowSize: this.requestWindow,
            effectiveLimit: Math.floor(this.maxRequests * this.safetyFactor),
            consecutive429s: this.consecutive429s
        };
    }
}

/* 
module.exports = {
    main,
    quickProcessUser,
    quickProcessPack,
    cleanFiles
};
*/

export { main, quickProcessUser, quickProcessPack, cleanFiles };

// Add debug logging if enabled
if (process.env.DEBUG) {
    logger.level = 'debug';
}

// Parse command line arguments
function parseArgs() {
    const args = process.argv.slice(2);
    return {
        noMongoDB: args.includes('--nomongodb'),
        noDBWrites: args.includes('--nodbwrites'),
        fromApi: args.includes('--fromapi'),
        debug: args.includes('--debug'),
        addUser: args.includes('--adduser') ? args[args.indexOf('--adduser') + 1] : null,
        addPack: args.includes('--addstarterpack') ? args[args.indexOf('--addstarterpack') + 1] : null,
        cleanFiles: args.includes('--cleanfiles'),
        purge: args.includes('--purge')
    };
}

// Validate environment
function validateEnv(args) {
    const required = ['BSKY_USERNAME', 'BSKY_PASSWORD'];
    if (!process.env.MONGODB_URI && !args.noMongoDB && !args.noDBWrites) {
        required.push('MONGODB_URI');
    }

    const missing = required.filter(key => !process.env[key]);
    if (missing.length > 0) {
        throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
    }
}

// Handle cleanup and shutdown
async function handleShutdown(signal) {
    logger.info(`\nReceived ${signal}. Starting graceful shutdown...`);
    
    try {
        if (processor) {
            // Save current state
            await processor.taskManager.maybeWriteCheckpoint(true);
            
            // Clean up resources
            await processor.cleanup();
            
            // Log final stats
            const stats = processor.metrics.getMetrics();
            logger.info('Final processing statistics:', {
                packsProcessed: stats.processing.packsProcessed,
                usersProcessed: stats.processing.usersProcessed,
                totalDuration: `${Math.floor((Date.now() - processor.startTime) / 60000)} minutes`
            });
        }
    } catch (err) {
        logger.error('Error during shutdown:', err);
    } finally {
        logger.info('Shutdown complete');
        process.exit(0);
    }
}

// Quick process functions
async function handleQuickProcess(args, processor, startTime) {
    if (args.addUser) {
        const result = await quickProcessUser(args.addUser, {
            processor,
            force: args.force,
            debug: args.debug,
            processAssociated: true
        });

        // Force checkpoint write after processing
        await processor.taskManager.maybeWriteCheckpoint(true);

        if (processor.debug) {
            logger.debug('User processing completed', {
                changes: result.changes,
                timing: {
                    total: Date.now() - startTime
                }
            });
        }

        if (args.addPack) {
            await quickProcessPack(args.addPack, { processor });
            // Force another checkpoint write
            await processor.taskManager.maybeWriteCheckpoint(true);
        }

        return result;
    }

    if (args.addPack) {
        const result = await quickProcessPack(args.addPack, { processor });
        await processor.taskManager.maybeWriteCheckpoint(true);
        return result;
    }
}

async function quickProcessUser(identifier, options = {}) {
    const { processor, force = false, debug = false, processAssociated = true } = options;
    
    try {
        if (debug) {
            logger.debug(`Quick processing user: ${identifier}`);
        }

        // Get profile
        const response = await processor.agent.getProfile({ actor: identifier });
        if (!response?.data) {
            throw new Error(`Could not find profile for ${identifier}`);
        }

        // Initialize task manager if needed AND load existing data
        if (!processor.taskManager) {
            processor.taskManager = new TaskManager(processor.fileHandler, debug);
            await processor.taskManager.initializeTaskList(); // This loads all necessary data
        }

        const result = await processor.processProfile(response.data, {
            force,
            processAssociated,
            debug
        });

        // Process tasks until completion, including associated packs
        while (processor.taskManager.pendingTasks.size > 0) {
            await processor.taskManager.processNextTask(processor);
            // Write checkpoint periodically
            await processor.taskManager.maybeWriteCheckpoint();
        }

        // Force final checkpoint write
        await processor.taskManager.maybeWriteCheckpoint(true);

        return result;
    } catch (err) {
        logger.error(`Error processing user ${identifier}:`, err);
        throw err;
    }
}

async function quickProcessPack(identifier, options = {}) {
    const { processor } = options;

    try {
        // CHANGE: Initialize task manager if needed AND load existing data
        if (!processor.taskManager) {
            processor.taskManager = new TaskManager(processor.fileHandler, processor.debug);
            await processor.taskManager.initializeTaskList();
        }

        let urlLine;
        if (identifier.startsWith('at://')) {
            const [_, handle, __, rkey] = identifier.split('/');
            urlLine = `${handle}|${rkey}`;
        } else {
            const pack = await processor.fileHandler.getPack(identifier);
            if (!pack) {
                throw new Error(`Cannot resolve pack with rkey ${identifier}`);
            }
            urlLine = `${pack.creator}|${identifier}`;
        }

        // Add as task
        await processor.taskManager.addTask({
            handle: urlLine.split('|')[0],
            rkey: urlLine.split('|')[1],
            priority: 1,
            source: 'quick_process'
        });

        // Process until completion
        while (processor.taskManager.pendingTasks.size > 0) {
            const success = await processor.taskManager.processNextTask(processor);
            // Write checkpoint periodically
            await processor.taskManager.maybeWriteCheckpoint();
        }

        // Force final checkpoint write
        await processor.taskManager.maybeWriteCheckpoint(true);

        return true;
    } catch (err) {
        logger.error(`Error processing pack ${identifier}:`, err);
        throw err;
    }
}

async function cleanFiles() {
    logger.debug('Starting file cleanup...');
    const processor = new MainProcessor({ noMongoDB: true, debug: args.debug });
    
    try {
        await processor.init();
        await processor.fileHandler.cleanFiles();
        logger.debug('File cleanup completed');
    } catch (err) {
        logger.error(`Error during file cleanup: ${err.message}`);
        throw err;
    } finally {
        await processor.cleanup();
    }
}

// Main function
class TaskManager {
    constructor(fileHandler, debug = false) {
        this.fileHandler = fileHandler;
        this.debug = debug;
        this.pendingTasks = new Map();
        this.completedTasks = new Set();
        this.failures = new Map();
        this.missingProfiles = new Set();
        this.packRelationships = new Map();
        this.startTime = Date.now();
        this.lastCheckpoint = Date.now();
        this.CHECKPOINT_INTERVAL = 20 * 60 * 1000; // 20 minutes
        this.checkpointDirty = false;  // Track if we need to write
        this.totalTasks = 0;
        this.completedTaskCount = 0;
        this.currentPackUsersTotal = 0;
        this.currentPackUsersProcessed = 0;
    }

    async initializeTaskList() {
        try {
            // Try to load checkpoint first, but don't fail if missing/corrupt
            let checkpoint = null;
            try {
                checkpoint = await this.loadCheckpoint();
            } catch (err) {
                logger.warn('Could not load checkpoint, starting fresh:', err);
                checkpoint = {
                    completedPacks: [],
                    missingPacks: [],
                    missingProfiles: []
                };
            }

            // Load data from main files first, fall back to YAML if needed
            const { packs: existingPacks, users: existingUsers } = 
                await this.loadExistingDataWithFallback();

            // Load URL list - this must exist and be valid
            const urlPacks = await this.loadUrlsFile();

            // Initialize state
            await this.buildConsolidatedTaskList(urlPacks, existingPacks, checkpoint);

            // Update caches
            for (const pack of existingPacks.values()) {
                this.fileHandler.updatePackCache(pack);
            }
            for (const user of existingUsers.values()) {
                this.fileHandler.updateUserCache(user);
            }

            if (this.debug) {
                logger.debug('Task list initialized:', {
                    pending: this.pendingTasks.size,
                    completed: this.completedTasks.size,
                    failed: this.failures.size,
                    existingPacks: existingPacks.size,
                    existingUsers: existingUsers.size,
                    urls: urlPacks.size
                });
            }
        } catch (err) {
            logger.error('Error initializing task list:', err);
            throw err;
        }
    }

    async loadExistingDataWithFallback() {
        const existingPacks = new Map();
        const existingUsers = new Map();
    
        try {
            await this.loadFromNDJSON(existingPacks, existingUsers);
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn('Failed to load from NDJSON files:', err);
            } else {
                logger.info('No existing NDJSON files found, will create new ones');
            }
            
            // Try YAML backups only if JSON failed for non-ENOENT reason
            if (err.code !== 'ENOENT') {
                try {
                    logger.info('Attempting to load from YAML backups...');
                    await this.loadFromYAMLBackups(existingPacks, existingUsers);
                } catch (backupErr) {
                    if (backupErr.code !== 'ENOENT') {
                        logger.error('Failed to load from YAML backups:', backupErr);
                    } else {
                        logger.info('No YAML backups found, starting fresh');
                    }
                    // Continue with empty maps - we'll rebuild from scratch
                }
            }
        }
    
        return { packs: existingPacks, users: existingUsers };
    }

    async loadUrlsFile() {
        const urls = new Map();
        try {
            const content = await fs.readFile(FILE_PATHS.urls, 'utf8');
            for (const line of content.split('\n').filter(Boolean)) {
                const [handle, rkey] = line.split('|').map(s => s.trim());
                if (handle && rkey) {
                    urls.set(rkey, { handle, rkey });
                }
            }
            return urls;
        } catch (err) {
            if (err.code === 'ENOENT') {
                // For a new installation, start with empty URL list
                logger.info('No URLs file found, starting fresh');
                return new Map();
            }
            logger.error('Error loading URLs file:', err);
            throw err;
        }
    }

    shouldProcessPack(rkey, existingPack, failure) {
        // Don't check completedTasks here anymore! We only skip if:
        if (failure?.permanent) {
            return { process: false, reason: 'permanent_failure' };
        }
    
        // If we have an existing pack, check its age
        if (existingPack) {
            const lastUpdate = new Date(existingPack.updated_at);
            const daysSinceUpdate = (Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60 * 24);
            
            // Skip if recently updated (unless it previously failed)
            if (daysSinceUpdate < 7 && !failure) {
                return { process: false, reason: 'recently_updated' };
            }
        }
    
        // If it failed before, check cooling period
        if (failure) {
            const lastAttempt = new Date(failure.lastAttempt);
            const daysSinceAttempt = (Date.now() - lastAttempt.getTime()) / (1000 * 60 * 60 * 24);
            const requiredCooling = Math.pow(2, failure.attempts);
            
            if (daysSinceAttempt < requiredCooling) {
                return { process: false, reason: 'cooling_period' };
            }
        }
    
        return { process: true };
    }

    async cleanup() {
        logger.debug('Starting cleanup process...');
        
        try {
            const cleanupTasks = [];
        
            if (this.fileHandler) {
                cleanupTasks.push(
                    this.fileHandler.cleanup().catch(err => {
                        logger.error('Error during file handler cleanup:', err);
                    })
                );
            }
        
            if (this.verificationHandler) {
                cleanupTasks.push(
                    this.verificationHandler.cleanup().catch(err => {
                        logger.error('Error during verification handler cleanup:', err);
                    })
                );
            }
        
            if (this.debugManager) {
                cleanupTasks.push(
                    this.debugManager.cleanup().catch(err => {
                        logger.error('Error during debug manager cleanup:', err);
                    })
                );
            }
        
            if (this.mongoClient) {
                cleanupTasks.push(
                    this.mongoClient.close(true).catch(err => {
                        logger.error('Error closing MongoDB connection:', err);
                    })
                );
            }
        
            if (this.taskManager) {
                cleanupTasks.push(
                    this.taskManager.maybeWriteCheckpoint(true).catch(err => {
                        logger.error('Error saving final checkpoint:', err);
                    })
                );
            }
    
            if (cleanupTasks.length > 0) {
                await Promise.allSettled(cleanupTasks);
            }
        
            // Clear caches and internal state
            if (this.profileCache) {
                this.profileCache.clear();
            }
            this.clearInternalState();
        
            logger.debug('Cleanup completed successfully');
        } catch (err) {
            logger.error('Error during cleanup:', err);
            // Don't rethrow - we're in cleanup
        }
    }

    async loadFromNDJSON(packsMap, usersMap) {
        // Load and validate NDJSON files
        const packsContent = await fs.readFile(FILE_PATHS.packs, 'utf8');
        const usersContent = await fs.readFile(FILE_PATHS.users, 'utf8');

        let validPackLines = 0, invalidPackLines = 0;
        let validUserLines = 0, invalidUserLines = 0;

        // Process packs
        for (const line of packsContent.split('\n').filter(Boolean)) {
            try {
                const pack = JSON.parse(line);
                if (this.validatePack(pack)) {
                    packsMap.set(pack.rkey, pack);
                    validPackLines++;
                } else {
                    invalidPackLines++;
                }
            } catch (err) {
                invalidPackLines++;
            }
        }

        // Process users
        for (const line of usersContent.split('\n').filter(Boolean)) {
            try {
                const user = JSON.parse(line);
                if (this.validateUser(user)) {
                    usersMap.set(user.did, user);
                    validUserLines++;
                } else {
                    invalidUserLines++;
                }
            } catch (err) {
                invalidUserLines++;
            }
        }

        // If we have too many invalid lines, consider the files corrupt
        const packCorruptionThreshold = 0.1;  // 10% invalid lines
        const userCorruptionThreshold = 0.1;

        if (invalidPackLines / (validPackLines + invalidPackLines) > packCorruptionThreshold ||
            invalidUserLines / (validUserLines + invalidUserLines) > userCorruptionThreshold) {
            throw new Error('Files appear to be corrupt - too many invalid lines');
        }
    }

    async loadFromYAMLBackups(packsMap, usersMap) {
        const packsYaml = await fs.readFile(FILE_PATHS.packsBackup, 'utf8');
        const usersYaml = await fs.readFile(FILE_PATHS.usersBackup, 'utf8');

        // Load packs
        yaml.loadAll(packsYaml, doc => {
            if (this.validatePack(doc)) {
                packsMap.set(doc.rkey, doc);
            }
        });

        // Load users
        yaml.loadAll(usersYaml, doc => {
            if (this.validateUser(doc)) {
                usersMap.set(doc.did, doc);
            }
        });
    }

    validatePack(pack) {
        return pack && 
               pack.rkey && 
               pack.creator && 
               pack.updated_at &&
               typeof pack.rkey === 'string' &&
               typeof pack.creator === 'string' &&
               typeof pack.updated_at === 'string';
    }

    validateUser(user) {
        return user && 
               user.did && 
               user.handle && 
               user.last_updated &&
               typeof user.did === 'string' &&
               typeof user.handle === 'string' &&
               typeof user.last_updated === 'string';
    }

    async loadCheckpoint() {
        try {
            const checkpointData = await fs.readFile('checkpoint.json', 'utf8');
            const checkpoint = JSON.parse(checkpointData);
            
            // Initialize sets from checkpoint
            this.completedTasks = new Set(checkpoint.completedPacks || []);
            this.missingProfiles = new Set(checkpoint.missingProfiles || []);
            
            // Initialize failures map
            if (checkpoint.missingPacks) {
                for (const {rkey, reason, attempts, timestamp} of checkpoint.missingPacks) {
                    this.failures.set(rkey, {
                        reason,
                        attempts: attempts || 1,
                        lastAttempt: timestamp,
                        permanent: attempts >= 3
                    });
                }
            }
            
            return checkpoint;
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn('Error loading checkpoint:', err);
            }
            return {
                completedPacks: [],
                missingPacks: [],
                missingProfiles: []
            };
        }
    }

    calculateTaskPriority(rkey, existingPack, failure) {
        let priority = 0;

        // Higher priority for packs we've never processed
        if (!existingPack) {
            priority += 3;
        } else {
            // Priority based on age of data
            const daysSinceUpdate = (Date.now() - new Date(existingPack.updated_at).getTime()) 
                / (1000 * 60 * 60 * 24);
            if (daysSinceUpdate > 30) priority += 2;
            else if (daysSinceUpdate > 7) priority += 1;

            // Priority based on user count (more users = higher priority)
            if (existingPack.user_count > 1000) priority += 2;
            else if (existingPack.user_count > 100) priority += 1;
        }

        // Lower priority for previously failed attempts
        if (failure) {
            priority = Math.max(0, priority - failure.attempts);
        }

        return priority;
    }

    async maybeWriteCheckpoint(force = false) {
        const now = Date.now();
        if (!force && (!this.checkpointDirty || now - this.lastCheckpoint < this.CHECKPOINT_INTERVAL)) {
            return;
        }

        try {
            await this.writeCheckpoint();
            this.lastCheckpoint = now;
            this.checkpointDirty = false;
        } catch (err) {
            logger.error('Failed to write checkpoint:', err);
        }
    }

    markDirty() {
        this.checkpointDirty = true;
    }

    async buildConsolidatedTaskList(urlPacks, existingPacks, checkpoint) {
        // Initialize with empty data if any is undefined
        urlPacks = urlPacks || new Map();
        existingPacks = existingPacks || new Map();
        checkpoint = checkpoint || {
            completedPacks: [],
            missingPacks: [],
            missingProfiles: []
        };
    
        this.pendingTasks.clear();
        const skipped = new Map();
        this.totalTasks = urlPacks.size;  // Set total tasks count here
    
        // Process URLs if we have any
        for (const [rkey, urlData] of urlPacks) {
            const existingPack = existingPacks.get(rkey);
            const failure = this.failures.get(rkey);
    
            const { process, reason } = this.shouldProcessPack(rkey, existingPack, failure);
    
            if (process) {
                const priority = this.calculateTaskPriority(rkey, existingPack, failure);
                
                await this.addTask({
                    handle: urlData.handle,
                    rkey,
                    priority,
                    source: 'initial',
                    existingData: existingPack || null,
                    previousAttempts: failure?.attempts || 0
                });
            } else {
                skipped.set(rkey, reason);
            }
        }
    
        if (this.debug) {
            logger.debug('Task list built:', {
                total: this.totalTasks,
                pending: this.pendingTasks.size,
                skipped: skipped.size,
                new: Array.from(this.pendingTasks.values()).filter(t => !t.existingData).length,
                updates: Array.from(this.pendingTasks.values()).filter(t => t.existingData).length,
                failureRetries: Array.from(this.pendingTasks.values()).filter(t => t.previousAttempts > 0).length
            });
        }
    }

    async loadExistingPacks() {
        const packs = new Map();
        try {
            const content = await fs.readFile(FILE_PATHS.packs, 'utf8');
            for (const line of content.split('\n').filter(Boolean)) {
                try {
                    const pack = JSON.parse(line);
                    if (pack.rkey && pack.updated_at) {
                        packs.set(pack.rkey, pack);
                    }
                } catch (err) {
                    logger.warn(`Invalid pack JSON line: ${err.message}`);
                }
            }
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn('Error loading existing packs:', err);
            }
        }
        return packs;
    }


    async initializeFromCheckpoint() {
        try {
            const checkpointData = await fs.readFile(FILE_PATHS.checkpoints, 'utf8');
            const checkpoint = JSON.parse(checkpointData);

            // Clear existing state
            this.pendingTasks.clear();
            this.completedTasks.clear();
            this.failures.clear();
            this.missingProfiles.clear();
            this.packRelationships.clear();

            // Rebuild completed tasks
            if (checkpoint.completedPacks) {
                for (const rkey of checkpoint.completedPacks) {
                    this.completedTasks.add(rkey);
                }
            }

            // Rebuild failures
            if (checkpoint.missingPacks) {
                for (const {rkey, reason, attempts, timestamp} of checkpoint.missingPacks) {
                    this.failures.set(rkey, {
                        reason,
                        attempts: attempts || 1,
                        lastAttempt: timestamp,
                        permanent: attempts >= 3
                    });
                }
            }

            // Rebuild missing profiles
            if (checkpoint.missingProfiles) {
                for (const did of checkpoint.missingProfiles) {
                    this.missingProfiles.add(did);
                }
            }

            // Validate against files
            await this.validateAgainstFiles();

            if (this.debug) {
                logger.debug('Initialized from checkpoint:', {
                    completed: this.completedTasks.size,
                    failures: this.failures.size,
                    missingProfiles: this.missingProfiles.size
                });
            }
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn('Error loading checkpoint, starting fresh:', err);
            }
            // Start with clean state if no checkpoint exists
            await this.createInitialCheckpoint();
        }
    }

    async createInitialCheckpoint() {
        const checkpoint = {
            version: "1.0",
            timestamp: new Date().toISOString(),
            completedPacks: [],
            missingPacks: [],
            missingProfiles: []
        };

        await this.writeCheckpoint(checkpoint);
    }

    async writeCheckpoint() {
        const checkpoint = {
            version: "1.0",
            timestamp: new Date().toISOString(),
            completedPacks: Array.from(this.completedTasks),
            missingPacks: Array.from(this.failures.entries()).map(([rkey, data]) => ({
                rkey,
                reason: data.reason,
                attempts: data.attempts,
                timestamp: data.lastAttempt
            })),
            missingProfiles: Array.from(this.missingProfiles),
            fileStates: {
                lastPackUpdate: this.fileHandler.packCache.size,
                lastUserUpdate: this.fileHandler.userCache.size
            }
        };
    
        const tempPath = FILE_PATHS.checkpointsBackup;
        try {
            await fs.writeFile(tempPath, JSON.stringify(checkpoint, null, 2));
            await fs.rename(tempPath, FILE_PATHS.checkpoints);
        } catch (err) {
            logger.error('Failed to write checkpoint:', err);
            try {
                await fs.unlink(tempPath);
            } catch (e) {
                // Ignore error if temp file doesn't exist
            }
            throw err;
        }
    }

    async validateAgainstFiles() {
        try {
            // Load and parse packs file
            const packsContent = await fs.readFile(FILE_PATHS.packs, 'utf8');
            const processedPacks = new Set();

            for (const line of packsContent.split('\n').filter(Boolean)) {
                try {
                    const pack = JSON.parse(line);
                    if (pack.rkey) {
                        processedPacks.add(pack.rkey);
                    }
                } catch (e) {
                    logger.warn(`Invalid pack JSON line: ${e.message}`);
                }
            }

            // Remove completed tasks that don't exist in files
            for (const rkey of this.completedTasks) {
                if (!processedPacks.has(rkey)) {
                    this.completedTasks.delete(rkey);
                    logger.warn(`Removed non-existent pack from completed tasks: ${rkey}`);
                }
            }

            // Load and parse users file for profile validation
            const usersContent = await fs.readFile(FILE_PATHS.users, 'utf8');
            const processedProfiles = new Set();

            for (const line of usersContent.split('\n').filter(Boolean)) {
                try {
                    const user = JSON.parse(line);
                    if (user.did) {
                        processedProfiles.add(user.did);
                    }
                } catch (e) {
                    logger.warn(`Invalid user JSON line: ${e.message}`);
                }
            }

            // Clean up missing profiles that actually exist
            for (const did of this.missingProfiles) {
                if (processedProfiles.has(did)) {
                    this.missingProfiles.delete(did);
                    logger.warn(`Removed existing profile from missing profiles: ${did}`);
                }
            }

        } catch (err) {
            logger.error('Error validating against files:', err);
            throw err;
        }
    }

    getInitialState() {
        return {
            version: "1.0",
            lastProcessedIndex: -1,
            lastProcessedDate: null,
            dailyStats: {},
            errors: [],
            rateLimitHits: [],
            packStats: {},
            lastMemoryUsage: null,
            startTime: Date.now(),
            completedPacks: [],
            missingPacks: [],
            missingProfiles: [],
            processedUsers: []
        };
    }

    async addTask(taskData) {
        const { handle, rkey, priority = 0, source = 'direct', parentDid = null } = taskData;

        if (this.completedTasks.has(rkey)) {
            if (this.debug) {
                logger.debug(`Skipping already completed task: ${rkey}`);
            }
            return false;
        }

        const failure = this.failures.get(rkey);
        if (failure?.permanent) {
            if (this.debug) {
                logger.debug(`Skipping permanently failed task: ${rkey}`);
            }
            return false;
        }

        this.pendingTasks.set(rkey, {
            handle,
            rkey,
            priority,
            source,
            parentDid,
            addedAt: new Date().toISOString(),
            attempts: failure?.attempts || 0
        });
        this.markDirty();
        await this.maybeWriteCheckpoint(); 

        return true;
    }

    async getNextTask() {
        // Convert to array for sorting
        const tasks = Array.from(this.pendingTasks.values());
        
        // Sort by priority (higher first) and attempts (fewer first)
        tasks.sort((a, b) => {
            if (a.priority !== b.priority) return b.priority - a.priority;
            return a.attempts - b.attempts;
        });

        return tasks[0] || null;
    }

    calculateDynamicPriority(packInfo) {
        let priority = 1; // Base priority for associated packs

        // Increase priority for packs with more members
        if (packInfo.memberCount > 100) priority += 1;
        if (packInfo.memberCount > 1000) priority += 1;

        // Increase priority for packs that were recently updated
        const daysSinceUpdate = (Date.now() - new Date(packInfo.updatedAt).getTime()) / (1000 * 60 * 60 * 24);
        if (daysSinceUpdate < 1) priority += 2;
        else if (daysSinceUpdate < 7) priority += 1;

        // Reduce priority for previously failed attempts
        const failure = this.failures.get(packInfo.rkey);
        if (failure) {
            priority = Math.max(0, priority - failure.attempts);
        }

        return priority;
    }

    async addAssociatedPack(packInfo, parentDid) {
        const priority = this.calculateDynamicPriority(packInfo);

        // Add to URLs file first
        await this.fileHandler.appendToUrlsFile(packInfo.creator, packInfo.rkey);
        
        return this.addTask({
            handle: packInfo.creator,
            rkey: packInfo.rkey,
            priority,
            source: 'associated',
            parentDid,
            discoveredAt: new Date().toISOString(),
            memberCount: packInfo.memberCount
        });
    }

    // Track relationships between packs and their discoverers
    recordPackRelationship(rkey, parentDid) {
        if (!this.packRelationships) {
            this.packRelationships = new Map();
        }
        
        if (!this.packRelationships.has(rkey)) {
            this.packRelationships.set(rkey, new Set());
        }
        this.packRelationships.get(rkey).add(parentDid);
    }

    async processNextTask(processor) {
        const task = await this.getNextTask();
        if (!task) return null;

        this.completedTaskCount++;
        logger.info(`Processing pack ${this.completedTaskCount}/${this.totalTasks}: ${task.rkey} (${task.handle})`);

        try {
            const success = await processor.processStarterPack(`${task.handle}|${task.rkey}`);
            
            if (success) {
                this.completedTasks.add(task.rkey);
                this.pendingTasks.delete(task.rkey);
                this.markDirty();

                // Log progress
                const progress = ((this.completedTaskCount / this.totalTasks) * 100).toFixed(1);
                logger.info(`Pack processing complete (${progress}% overall progress)`);
            }

            return success;
        } catch (err) {
            logger.error(`Error processing task ${task.rkey}:`, err);
            return false;
        }
    }

    updateUserProgress(total, current) {
        this.currentPackUsersTotal = total;
        this.currentPackUsersProcessed = current;
        logger.info(`Processing user ${current}/${total}`);
    }

    async loadExistingData() {
        const existingPacks = new Map();
        const existingUsers = new Map();
    
        try {
            // Load packs
            const packsContent = await fs.readFile(FILE_PATHS.packs, 'utf8');
            for (const line of packsContent.split('\n').filter(Boolean)) {
                try {
                    const pack = JSON.parse(line);
                    if (pack.rkey && pack.updated_at) {
                        existingPacks.set(pack.rkey, pack);
                    }
                } catch (err) {
                    logger.warn(`Invalid pack JSON line: ${err.message}`);
                }
            }
    
            // Load users
            const usersContent = await fs.readFile(FILE_PATHS.users, 'utf8');
            for (const line of usersContent.split('\n').filter(Boolean)) {
                try {
                    const user = JSON.parse(line);
                    if (user.did && user.last_updated) {
                        existingUsers.set(user.did, user);
                    }
                } catch (err) {
                    logger.warn(`Invalid user JSON line: ${err.message}`);
                }
            }
    
            return { packs: existingPacks, users: existingUsers };
        } catch (err) {
            if (err.code === 'ENOENT') {
                logger.info('No existing data files found, starting fresh');
                return { packs: new Map(), users: new Map() };
            }
            logger.warn('Error loading existing data:', err);
            throw err;
        }
    }

    async recordFailure(rkey, reason, permanent = false) {
        const failure = this.failures.get(rkey) || {
            attempts: 0,
            firstAttempt: new Date().toISOString()
        };

        failure.attempts++;
        failure.lastAttempt = new Date().toISOString();
        failure.reason = reason;
        failure.permanent = permanent || failure.attempts >= 3;

        this.failures.set(rkey, failure);
        if (failure.permanent) {
            this.pendingTasks.delete(rkey);
        }

        this.markDirty();
        await this.maybeWriteCheckpoint(); 

        this.markDirty();
    }

    isRecentlyProcessed(timestamp, daysThreshold = 10) {
        const lastUpdate = new Date(timestamp);
        const daysSinceUpdate = (Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60 * 24);
        return daysSinceUpdate < daysThreshold;
    }

    async buildTaskList(urlsContent, existingPacks = new Map()) {
        const tasks = [];
        const skipped = [];
        
        for (const line of urlsContent.split('\n').filter(Boolean)) {
            const [handle, rkey] = line.split('|').map(s => s.trim());
            if (!handle || !rkey) continue;

            // Check completed tasks from current run
            if (this.completedTasks.has(rkey)) {
                skipped.push({ rkey, reason: 'already_completed' });
                continue;
            }

            // Check permanent failures
            const failure = this.failures.get(rkey);
            if (failure) {
                const daysSinceLastAttempt = 
                    (Date.now() - new Date(failure.lastAttempt).getTime()) / (1000 * 60 * 60 * 24);
                
                if (failure.attempts >= 3 && daysSinceLastAttempt < 7) {
                    skipped.push({ 
                        rkey, 
                        reason: `permanent_failure: ${failure.reason}`,
                        attempts: failure.attempts
                    });
                    continue;
                }
            }

            const existingPack = existingPacks.get(rkey);
            if (existingPack && this.isRecentlyProcessed(existingPack.updated_at)) {
                skipped.push({ rkey, reason: 'recently_processed' });
                continue;
            }

            tasks.push({
                handle,
                rkey,
                priority: existingPack ? 1 : 0,
                existingPack,
                previousAttempts: failure?.attempts || 0
            });
        }

        // Sort by priority (new packs first) and retry attempts
        tasks.sort((a, b) => {
            if (a.priority !== b.priority) return b.priority - a.priority;
            return a.previousAttempts - b.previousAttempts;
        });

        if (this.debug) {
            logger.debug('Task list built', {
                total: tasks.length,
                skipped: skipped.length,
                new: tasks.filter(t => t.priority === 0).length,
                updates: tasks.filter(t => t.priority === 1).length,
                failureRetries: tasks.filter(t => t.previousAttempts > 0).length
            });
        }

        return { tasks, skipped };
    }

    async saveCheckpoint() {
        const checkpoint = {
            timestamp: new Date().toISOString(),
            completed: Array.from(this.completedTasks).map(rkey => ({ rkey })),
            duration: Date.now() - this.startTime
        };

        await fs.writeFile(
            FILE_PATHS.checkpoints,
            JSON.stringify(checkpoint, null, 2)
        );
    }

    recordMissingProfile(did) {
        this.missingProfiles.add(did);
    }

    async markTaskCompleted(rkey) {
        this.completedTasks.add(rkey);
        this.pendingTasks.delete(rkey);
        this.failures.delete(rkey);
        this.markDirty();
        await this.maybeWriteCheckpoint(); 
    }

    async cleanup() {
        logger.debug('Starting task manager cleanup...');
        try {
            // Force write final checkpoint
            await this.maybeWriteCheckpoint(true);
            
            // Clear all internal collections
            this.pendingTasks.clear();
            this.completedTasks.clear();
            this.failures.clear();
            this.missingProfiles.clear();
            
            logger.debug('Task manager cleanup completed');
        } catch (err) {
            logger.error('Error during task manager cleanup:', err);
            throw err;
        }
    }
}

// main function
async function main() {
    const args = parseArgs();
    logger.info('CLI args:', args);
    validateEnv(args);
    
    let processor;
    const startTime = Date.now();
    
    try {
        metrics.recordStartup();
        
        processor = new MainProcessor({
            noMongoDB: args.noMongoDB,
            noDBWrites: args.noDBWrites,  
            fromApi: args.fromApi,
            debug: args.debug || process.env.DEBUG
        });

        await processor.init();
        logger.info('Initialization complete');

        // Handle maintenance commands first
        if (args.purge || args.cleanFiles) {
            await handleMaintenanceCommands(args, processor);
            return;
        }

        // Handle single-item processing
        if (args.addUser || args.addPack) {
            const result = await handleQuickProcess(args, processor);
            return result;
        }

        // Initialize from checkpoint
        await processor.taskManager.initializeFromCheckpoint();
        
        // Load existing data first
        const { packs: existingPacks } = await processor.taskManager.loadExistingData();
        
        // Load and parse tasks
        const urlsContent = await fs.readFile(FILE_PATHS.urls, 'utf8');
        const { tasks } = await processor.taskManager.buildTaskList(urlsContent, existingPacks);

        // Add all tasks to task manager
        for (const task of tasks) {
            await processor.taskManager.addTask({
                handle: task.handle,
                rkey: task.rkey,
                priority: task.priority,
                source: 'initial'
            });
        }

        // Process tasks until none remain
        let processedCount = 0;
        let failedCount = 0;

        while (processor.taskManager.pendingTasks.size > 0) {
            const success = await processor.taskManager.processNextTask(processor);
            
            if (success === null) break; // no more tasks
            
            processedCount++;
            if (!success) failedCount++;

            // Write checkpoint periodically
            await processor.taskManager.maybeWriteCheckpoint();
        }

        // Force final checkpoint write
        await processor.taskManager.maybeWriteCheckpoint(true);

        // Final metrics
        const finalMetrics = metrics.getMetrics();
        logger.debug('Processing completed', {
            duration: `${Math.floor((Date.now() - startTime) / 60000)} minutes`,
            processed: processedCount,
            failed: failedCount,
            stats: finalMetrics
        });

    } catch (err) {
        logger.error('Fatal error:', err);
        metrics.recordFatalError(err);
        throw err;
    } finally {
        if (processor) {
            await processor.cleanup();
        }
    }
}

// Error handlers
process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
});

process.on('uncaughtException', (error) => {
    logger.error('Uncaught Exception:', error);
    process.exit(1);
});

process.on('SIGINT', () => handleShutdown('SIGINT'));
process.on('SIGTERM', () => handleShutdown('SIGTERM'));

// Run the program
main().catch(err => {
    logger.error('Fatal error in main:', err);
    process.exit(1);
});

