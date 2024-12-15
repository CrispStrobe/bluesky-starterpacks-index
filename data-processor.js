#!/usr/bin/env node
// v023
import * as dotenv from 'dotenv';
import { BskyAgent } from '@atproto/api';
import { MongoClient } from 'mongodb';
import fs from 'fs/promises';
import winston from 'winston';
import yaml from 'js-yaml';
import path from 'path';
import { createReadStream, createWriteStream } from 'fs';

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
    mongodb: 1000
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

const MAX_RETRY_ATTEMPTS = 3;
const MONGODB_TIMEOUT = 5000;

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
                // Remove simple format to handle custom formatting
                winston.format.printf(({ timestamp, level, message, ...meta }) => {
                    // Exclude 'service' from metadata
                    const { service, ...rest } = meta;
                    // Check if there are additional metadata fields
                    const hasAdditionalMeta = Object.keys(rest).length > 0;
                    // Format the log message
                    let log = `${timestamp} [${level}]: ${message}`;
                    // Append additional metadata if present
                    if (hasAdditionalMeta) {
                        log += `\n${JSON.stringify(rest, null, 2)}`;
                    }
                    return log;
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

    recordPackStats(stats) {
        if (!this.metrics.packStats) {
            this.metrics.packStats = new Map();
        }
    
        const {rkey, ...packStats} = stats;
        this.metrics.packStats.set(rkey, {
            timestamp: new Date().toISOString(),
            ...packStats
        });
    
        // Update summary metrics
        if (!this.metrics.summary) {
            this.metrics.summary = {
                totalUsersProcessed: 0,
                totalNewUsers: 0,
                totalUpdatedUsers: 0,
                totalRemovedUsers: 0,
                totalAssociatedPacks: {
                    discovered: 0,
                    queued: 0,
                    skipped: 0
                }
            };
        }
    
        this.metrics.summary.totalUsersProcessed += stats.userStats?.processed || 0;
        this.metrics.summary.totalNewUsers += stats.userStats?.newToMongoDB || 0;
        this.metrics.summary.totalUpdatedUsers += stats.userStats?.updated || 0;
        this.metrics.summary.totalRemovedUsers += stats.userStats?.removed || 0;
    
        if (stats.associatedPacks) {
            this.metrics.summary.totalAssociatedPacks.discovered += stats.associatedPacks.discovered || 0;
            this.metrics.summary.totalAssociatedPacks.queued += stats.associatedPacks.queued || 0;
            this.metrics.summary.totalAssociatedPacks.skipped += stats.associatedPacks.skipped || 0;
        }
    
        // Log summary every 10 packs
        if (this.metrics.packStats.size % 10 === 0) {
            logger.info('Processing summary:', {
                packsProcessed: this.metrics.packStats.size,
                usersProcessed: this.metrics.summary.totalUsersProcessed,
                newUsers: this.metrics.summary.totalNewUsers,
                updatedUsers: this.metrics.summary.totalUpdatedUsers,
                removedUsers: this.metrics.summary.totalRemovedUsers,
                associatedPacks: this.metrics.summary.totalAssociatedPacks
            });
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

        if (this.skipVerification) {
            logger.debug('Skipping files verification in purgefiles mode');
            return results;
        }
    
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
        this.handleCache = new Map();
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
            if (usersContent.trim()) {  // Only process if file is not empty
                try {
                    // Try parsing first line to determine format
                    const firstLine = usersContent.split('\n')[0].trim();
                    if (firstLine.startsWith('[') || firstLine.startsWith('{')) {
                        // Convert JSON to NDJSON
                        logger.debug(`Converting to NDJSON ...`);
                        const users = JSON.parse(firstLine.startsWith('[') ? usersContent : `[${usersContent}]`);
                        await this.convertToNDJSON(FILE_PATHS.users, users);
                    }
                    this.fileFormats.set(FILE_PATHS.users, 'ndjson');
                } catch (parseErr) {
                    // If parse fails, assume it's already NDJSON
                    logger.debug('Assuming users file is already NDJSON format');
                    this.fileFormats.set(FILE_PATHS.users, 'ndjson');
                }
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
            if (packsContent.trim()) {
                try {
                    const firstLine = packsContent.split('\n')[0].trim();
                    if (firstLine.startsWith('[') || firstLine.startsWith('{')) {
                        const packs = JSON.parse(firstLine.startsWith('[') ? packsContent : `[${packsContent}]`);
                        await this.convertToNDJSON(FILE_PATHS.packs, packs);
                    }
                    this.fileFormats.set(FILE_PATHS.packs, 'ndjson');
                } catch (parseErr) {
                    logger.debug('Assuming packs file is already NDJSON format');
                    this.fileFormats.set(FILE_PATHS.packs, 'ndjson');
                }
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

    async validatePackMembership(processor, userDid, packRkey) {
        // Validate all inputs
        if (!processor?.dbManager?.db) {
            logger.error('Invalid processor or missing dbManager');
            return null;
        }
        if (!userDid || !packRkey) {
            logger.warn(`Invalid parameters - userDid: ${userDid}, packRkey: ${packRkey}`);
            return null;
        }
    
        try {
            // Get pack data
            const pack = await processor.dbManager.db.collection('starter_packs')
                .findOne({ rkey: packRkey });
            
            if (!pack) {
                logger.warn(`Pack ${packRkey} not found while validating membership`);
                return null;
            }
    
            if (!pack.list || !pack.creator_did) {
                logger.warn(`Pack ${packRkey} missing required fields (list or creator_did)`);
                return null;
            }
    
            // Use pack.list directly instead of constructing URI
            const listData = await processor.apiHandler.makeApiCall('app.bsky.graph.getList', {
                list: pack.list
            });
    
            if (!listData?.items) {
                logger.warn(`No list data returned for pack ${packRkey}`);
                return false;
            }
    
            return listData.items.some(member => 
                member?.subject?.did === userDid
            );
        } catch (err) {
            logger.error(`Error validating membership for ${userDid} in pack ${packRkey}:`, err);
            return null;
        }
    }

    async appendUser(userData) {
        try {
            await this.acquireLock();

            logger.debug('FileHandler: Received user data for append:', userData);
            
            if (!userData.did || !userData.last_updated) {
                throw new Error('Invalid user data');
            }

            const existing = this.userCache.get(userData.did);
        
            // Preserve existing pack_ids if not explicitly provided
            if (!userData.pack_ids && existing?.pack_ids) {
                userData.pack_ids = existing.pack_ids;
            } else if (userData.pack_ids && existing?.pack_ids) {
                // Merge with existing pack_ids
                userData.pack_ids = [...new Set([
                    ...existing.pack_ids,
                    ...userData.pack_ids
                ])];
            }
    
            // Format user data - maintain existing structure
            const formattedUser = {
                did: userData.did,
                handle: userData.handle,
                display_name: userData.displayName || userData.display_name ||'',
                followers_count: userData.followers || userData.followers_count || 0,
                follows_count: userData.following || userData.follows_count || 0,
                last_updated: userData.last_updated,
                pack_ids: userData.pack_ids || [],
                // Add new fields while maintaining backward compatibility
                handle_history: userData.handle_history || [],
                description: userData.description || '',
                avatar: userData.avatar || '',
                indexed_at: userData.indexed_at || null,
                created_at: userData.created_at || null
            };

            logger.debug('FileHandler: Formatted user data:', formattedUser);
    
            // Update cache
            this.updateUserCache(formattedUser);
    
            // Append to NDJSON file - ensure proper line ending
            const jsonLine = JSON.stringify(formattedUser) + '\n';
            await fs.writeFile(FILE_PATHS.users, jsonLine, { flag: 'a' });
    
            // Append to YAML file with proper document separator
            const yamlDoc = '---\n' + yaml.dump(formattedUser);
            await fs.writeFile(FILE_PATHS.usersBackup, yamlDoc, { flag: 'a' });

            logger.debug('FileHandler: Successfully saved user data to files');
    
            return true;
        } finally {
            this.releaseLock();
        }
    }

    async appendUserMetadata(userData) {
        const metadata = {
            did: userData.did,
            handle: userData.handle,
            display_name: userData.displayName || '',
            description: userData.description || '',
            handle_history: userData.handle_history || [],
            last_updated: new Date().toISOString()
        };

        // If the user already exists, merge handle history
        const existing = this.userCache.get(userData.did);
        if (existing) {
            if (existing.handle !== userData.handle) {
                metadata.handle_history = [
                    ...(existing.handle_history || []),
                    {
                        handle: existing.handle,
                        timestamp: existing.last_updated
                    }
                ];
            } else {
                metadata.handle_history = existing.handle_history || [];
            }
        }

        return metadata;
    }

    async appendToUrlsFile(handle, rkey) {
        if (!handle || !rkey) {
            throw new Error('Invalid parameters: handle and rkey are required');
        }
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
        if (!user?.did) { 
            logger.warn("Called updateUserCache withouth user parameter.");
            return;
        }
        const existing = this.userCache.get(user.did);
        const lastUpdated = new Date(user.last_updated);
        
        if (!existing || lastUpdated > new Date(existing.last_updated)) {
            // Handle renamed profiles
            if (existing && existing.handle !== user.handle) {
                const renameEntry = {
                    oldHandle: existing.handle,
                    timestamp: existing.last_updated
                };
                user.handle_history = [...(existing.handle_history || []), renameEntry];
            }
            
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
        if (!did) {
            logger.warn('Attempted to get user with undefined DID');
            return null;
        }
        return this.userCache.get(did);
    }

    getPack(rkey) {
        return this.packCache.get(rkey);
    }

    async getPacksByCreator(did) {
        const packs = [];
        
        // Read NDJSON file line by line - no need for lock since we're just reading
        const content = await fs.readFile(FILE_PATHS.packs, 'utf8');
        const lines = content.split('\n').filter(Boolean);
        
        for (const line of lines) {
            try {
                const pack = JSON.parse(line);
                if (pack.creator_did === did) {
                    packs.push(pack);
                }
            } catch (err) {
                logger.warn(`Invalid pack JSON line: ${err.message}`);
                continue;
            }
        }
    
        return packs;
    }

    async getUserByHandle(handle) {
        // Check handle cache first
        const did = this.handleCache.get(handle);
        if (did) {
            return this.userCache.get(did);
        }
        return null;
    }

    async getUserByHistoricalHandle(handle) {
        const sanitizedHandle = handle.toLowerCase().trim();
        for (const user of this.userCache.values()) {
            if (user.handle.toLowerCase() === sanitizedHandle) return user;
            if (user.handle_history?.some(entry => 
                entry.oldHandle.toLowerCase() === sanitizedHandle
            )) {
                return user;
            }
        }
        return null;
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

        if (this.skipVerification) {
            logger.info('Skipping integrity verification in purgefiles mode');
            return results;
        }
    
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
    constructor(agent, rateLimiter, fileHandler) {
        if (!agent) throw new Error('BskyAgent is required');
        this.agent = agent;
        this.rateLimiter = rateLimiter;
        this.fileHandler = fileHandler;  // Store fileHandler
        this.publicEndpoint = 'https://public.api.bsky.app';
    }

    async makePublicApiCall(endpoint, params) {
        try {
            logger.debug(`Public Bluesky API Call initiating for ${endpoint}:`, params);
            
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
                logger.debug(`Calling (with Auth) getProfile:`, {params});
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
        logger.debug(`ApiHandler: makeApiCall for ${endpoint}:`, params, options)
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

    async resolveHandle(rawHandle, opts = {}) {
        const {
            retries = 0,
            maxRetries = 3,
            method = null
        } = opts;
    
        const RETRY_DELAY = 2000;
        const allErrors = [];
    
        try {
            if (!rawHandle) {
                throw new Error('No handle provided');
            }
    
            // If it's already a DID, return it
            if (rawHandle.startsWith('did:')) {
                return rawHandle;
            }
    
            // Clean the handle
            const handle = this.sanitizeHandle(rawHandle);
            logger.debug(`Resolving handle attempt ${retries + 1}/${maxRetries}: ${handle} (method: ${method || 'all'})`);
    
            // Track what methods we've tried
            const methodsTried = new Set();
    
            // First check cache (no API call)
            const cacheResult = await this.checkCache(handle);
            if (cacheResult) {
                logger.debug(`Found ${handle} in cache: ${cacheResult}`);
                return cacheResult;
            }
    
            // Try each method in sequence
            const methods = [
                {
                    name: 'public',
                    fn: async () => {
                        try {
                            await this.rateLimiter.throttle();
                            const result = await this.makePublicApiCall(
                                'com.atproto.identity.resolveHandle',
                                { handle }
                            );
                            if (result?.did) return result.did;
                        } catch (err) {
                            logger.debug(`Public API failed for ${handle}:`, {
                                error: err.message,
                                status: err.status,
                                type: err.error
                            });
                            allErrors.push({ method: 'public', error: err });
                            
                            if (err.status === 429) {
                                await this.rateLimiter.handleResponse(err);
                            }
                            return null;
                        }
                    }
                },
                {
                    name: 'getProfile',
                    fn: async () => {
                        try {
                            await this.rateLimiter.throttle();
                            const profile = await this.makePublicApiCall(
                                'app.bsky.actor.getProfile',
                                { actor: handle }
                            );
                            if (profile?.did) return profile.did;
                        } catch (err) {
                            logger.debug(`GetProfile failed for ${handle}:`, {
                                error: err.message,
                                status: err.status,
                                type: err.error
                            });
                            allErrors.push({ method: 'getProfile', error: err });
                            
                            if (err.status === 429) {
                                await this.rateLimiter.handleResponse(err);
                            }
                            return null;
                        }
                    }
                },
                {
                    name: 'search',
                    fn: async () => {
                        try {
                            await this.rateLimiter.throttle();
                            const searchResult = await this.makePublicApiCall(
                                'app.bsky.actor.searchActors',
                                { 
                                    q: handle,
                                    limit: 1
                                }
                            );
    
                            if (searchResult?.actors?.[0]) {
                                const actor = searchResult.actors[0];
                                // Verify exact match
                                if (this.sanitizeHandle(actor.handle) === handle) {
                                    logger.debug(`Found ${handle} via search as ${actor.did}`);
                                    return actor.did;
                                }
                            }
                        } catch (err) {
                            logger.debug(`Search failed for ${handle}:`, {
                                error: err.message,
                                status: err.status,
                                type: err.error
                            });
                            allErrors.push({ method: 'search', error: err });
                            
                            if (err.status === 429) {
                                await this.rateLimiter.handleResponse(err);
                            }
                            return null;
                        }
                    }
                },
                {
                    name: 'auth',
                    fn: async () => {
                        try {
                            await this.refreshTokenIfNeeded();
                            await this.rateLimiter.throttle();
                            const response = await this.agent.resolveHandle({ handle });
                            if (response?.data?.did) return response.data.did;
                        } catch (err) {
                            logger.debug(`Auth API failed for ${handle}:`, {
                                error: err.message,
                                status: err.status,
                                type: err.error
                            });
                            allErrors.push({ method: 'auth', error: err });
    
                            if (err.status === 401) {
                                await this.refreshTokenIfNeeded(true);
                            }
                            if (err.status === 429) {
                                await this.rateLimiter.handleResponse(err);
                            }
                            return null;
                        }
                    }
                }
                
            ];
    
            // Try each method (or specific method if specified)
            for (const m of methods) {
                if (method && m.name !== method) continue;
                if (methodsTried.has(m.name)) continue;
    
                methodsTried.add(m.name);
                const result = await m.fn();
                if (result) return result;
    
                // Add delay between methods
                await new Promise(resolve => setTimeout(resolve, 500));
            }
    
            // If we still have retries left, try again
            if (retries < maxRetries - 1) {
                const delay = RETRY_DELAY * Math.pow(2, retries);
                logger.debug(`All methods failed for ${handle}, retrying in ${delay}ms...`);
                
                // Log detailed errors if in debug mode
                logger.debug('Resolution attempts failed:', {
                    handle,
                    attempt: retries + 1,
                    errors: allErrors.map(e => ({
                        method: e.method,
                        message: e.error.message,
                        status: e.error.status,
                        type: e.error.error
                    }))
                });
    
                await new Promise(resolve => setTimeout(resolve, delay));
                return this.resolveHandle(rawHandle, {
                    retries: retries + 1,
                    maxRetries
                });
            }
    
            throw new Error(`Could not resolve handle after ${maxRetries} attempts: ${rawHandle}`);
    
        } catch (err) {
            // Log comprehensive error info
            logger.error(`Handle resolution failed for ${rawHandle}:`, {
                attempt: retries + 1,
                method: method || 'all',
                error: {
                    message: err.message,
                    status: err.status,
                    type: err.error
                },
                previousErrors: allErrors.map(e => ({
                    method: e.method,
                    message: e.error.message,
                    status: e.error.status,
                    type: e.error.error
                }))
            });
    
            throw err;
        }
    }

    sanitizeHandle(handle) {
        if (!handle) return '';
        
        // Remove invisible characters and extra spaces
        let cleaned = handle
            .replace(/[\u200B-\u200D\uFEFF]/g, '')  // Remove zero-width chars
            .replace(/\s+/g, '')                     // Remove all whitespace
            .trim();
        
        // Remove URL parts if present
        cleaned = cleaned.replace(/^(https?:\/\/)?(www\.)?/, '');
        
        // Normalize domain
        cleaned = cleaned.replace(/\.bsky\.social$/, '');
        cleaned = cleaned.toLowerCase();
        
        // Add domain if missing
        if (!cleaned.includes('.')) {
            cleaned = `${cleaned}.bsky.social`;
        }
        
        return cleaned;
    }
    
    async checkCache(handle) {
        if (!this.fileHandler) return null;
    
        try {
            // Check direct cache
            const cachedUser = await this.fileHandler.getUser(handle);
            if (cachedUser?.did) return cachedUser.did;
    
            // Check handle history
            const historicalUser = await this.fileHandler.getUserByHistoricalHandle(handle);
            if (historicalUser?.did) {
                logger.info(`Found ${handle} in history as ${historicalUser.handle}`);
                return historicalUser.did;
            }
        } catch (err) {
            logger.debug(`Cache check failed for ${handle}: ${err.message}`);
        }
        return null;
    }
    
    async refreshTokenIfNeeded(force = false) {
        if (!this.agent) {
            throw new Error('No agent initialized');
        }

        try {
            const shouldRefresh = force || !this._lastTokenRefresh || 
                (Date.now() - this._lastTokenRefresh) > 45 * 60 * 1000; // 45 minutes

            if (shouldRefresh) {
                await this.agent.login({
                    identifier: process.env.BSKY_USERNAME,
                    password: process.env.BSKY_PASSWORD
                });
                this._lastTokenRefresh = Date.now();
                logger.debug('Successfully refreshed auth token');
            }
        } catch (err) {
            logger.error('Failed to refresh token:', err);
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

function normalizeHandle(handle) {
    if (!handle) return '';
    
    // Remove any protocol/URL parts
    let normalized = handle.replace(/^(http[s]?:\/\/)?(www\.)?/, '');
    
    // Remove bsky.social suffix if present
    normalized = normalized.replace(/\.bsky\.social$/, '');
    
    // Convert to lowercase
    normalized = normalized.toLowerCase();
    
    // Add .bsky.social if not present
    if (!normalized.includes('.')) {
        normalized = `${normalized}.bsky.social`;
    }
    
    return normalized;
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
        this.operationDelay = 1000;         // Base delay between operations
        this.consecutiveThrottles = 0;
        this.maxConsecutiveThrottles = 5;
        this.baseBackoffDelay = 1000;
        this.session = null;
    }

    // Helper function for cleaning pack_ids array
    sanitizePackIds(packIds) {
        if (!Array.isArray(packIds)) {
            return typeof packIds === 'string' ? [packIds] : [];
        }
        
        // Filter and clean array items
        const cleaned = packIds.reduce((acc, id) => {
            if (typeof id === 'string') {
                acc.push(id);
            } else if (id && typeof id === 'object' && id.$each) {
                if (Array.isArray(id.$each)) {
                    acc.push(...id.$each.filter(item => typeof item === 'string'));
                }
            }
            return acc;
        }, []);
        
        // Remove duplicates and empty values
        return [...new Set(cleaned.filter(Boolean))];
    }

    async getKnownStarterPackRkeys() {
        try {
            // Only get rkeys in one efficient query
            const cursor = this.db.collection('starter_packs')
                .find({}, { projection: { rkey: 1, _id: 0 } })
                .batchSize(10000);  // Adjust batch size for memory efficiency
    
            const knownRkeys = new Set();
            await cursor.forEach(doc => {
                if (doc.rkey) knownRkeys.add(doc.rkey);
            });
    
            logger.info(`Retrieved ${knownRkeys.size} known starter pack rkeys from MongoDB`);
            return knownRkeys;
        } catch (err) {
            logger.error('Error fetching known starter pack rkeys:', err);
            throw err;
        }
    }

    async getKnownUserDIDs() {
        try {
            // Only get DIDs in one efficient query
            const cursor = this.db.collection('users')
                .find({}, { projection: { did: 1, _id: 0 } })
                .batchSize(10000);

            const knownDIDs = new Set();
            await cursor.forEach(doc => {
                if (doc.did) knownDIDs.add(doc.did);
            });

            logger.info(`Retrieved ${knownDIDs.size} known user DIDs from MongoDB`);
            return knownDIDs;
        } catch (err) {
            logger.error('Error fetching known user DIDs:', err);
            throw err;
        }
    }

    async performPackIdsCleanup() {
        if (this.noMongoDB || this.noDBWrites) {
            logger.info('Skipping pack_ids cleanup - MongoDB not enabled');
            return;
        }
    
        try {
            logger.info('Starting comprehensive pack_ids cleanup...');
    
            // First get all users
            const allUsers = await this.db.collection('users').find({}).toArray();
            
            // Filter problematic users in memory
            const problematicUsers = allUsers.filter(user => {
                return (user.pack_ids === null) ||
                       (Array.isArray(user.pack_ids) && user.pack_ids.some(id => 
                           id && typeof id === 'object' && '$each' in id
                       ));
            });
    
            logger.info(`Found ${problematicUsers.length} users with problematic pack_ids`);
            let successCount = 0;
            let failCount = 0;
    
            for (const user of problematicUsers) {
                try {
                    logger.debug(`Processing user ${user.did}:`, {
                        originalPackIds: user.pack_ids
                    });
    
                    // Clean the array
                    const cleanPackIds = Array.isArray(user.pack_ids) ?
                        user.pack_ids
                            .filter(id => typeof id === 'string')
                            .filter(Boolean) : [];
    
                    // Simple update with clean array
                    const updateResult = await this.db.collection('users').updateOne(
                        { did: user.did },
                        { 
                            $set: { 
                                pack_ids: cleanPackIds,
                                created_packs: cleanPackIds.length ? [] : [],
                                last_updated: new Date().toISOString()
                            }
                        }
                    );
    
                    if (updateResult.modifiedCount > 0) {
                        successCount++;
                        logger.info(`Cleaned arrays for user ${user.did} - now has ${cleanPackIds.length} pack_ids`);
                    } else {
                        failCount++;
                        logger.warn(`No changes made for user ${user.did}`);
                    }
    
                } catch (err) {
                    failCount++;
                    logger.error(`Error cleaning arrays for user ${user.did}:`, err);
                }
            }
    
            logger.info('Pack_ids cleanup completed:', {
                totalProcessed: problematicUsers.length,
                successCount,
                failCount
            });
    
            return {
                processed: problematicUsers.length,
                success: successCount,
                failed: failCount
            };
    
        } catch (err) {
            logger.error('Fatal error during pack_ids cleanup:', err);
            throw err;
        }
    }

    async connect(maxRetries = 2) {
        logger.debug('connect');
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.client.connect();
                this.db = this.client.db(this.dbName);
                
                // Start a session if we don't have one
                if (!this.session || this.session.hasEnded) {
                    this.session = this.client.startSession();
                }
                
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

    async ensureSession() {
        if (!this.session || this.session.hasEnded) {
            try {
                if (this.session) {
                    await this.session.endSession();
                }
                this.session = this.client.startSession();
                logger.debug('Created new MongoDB session');
            } catch (err) {
                logger.error('Error creating new session:', err);
                // Add metrics recording
                if (this.metrics) {
                    this.metrics.recordDbOperation('create_session', 0, false);
                }
                throw err;
            }
        }
        return this.session;
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

    async setupIndexes() {
        if (this.isCosmosDb) return; // Skip for Cosmos DB
    
        for (const [collection, indexes] of Object.entries({
            users: [
                { key: { did: 1 }, options: { unique: true, background: true } },
                { key: { handle: 1 }, options: { background: true } },
                { key: { pack_ids: 1 }, options: { background: true } }, // which starterpacks is the user a member of?
                { key: { last_updated: 1 }, options: { background: true } },
                { key: { created_packs: 1 }, options: { background: true, sparse: true } }, 
                { key: { 'handle_history.oldHandle': 1 }, options: { 
                    background: true,
                    sparse: true  // Only index documents that have handle_history
                }},
                { key: { deleted: 1 }, options: { background: true } }
            ],
            starter_packs: [
                { key: { rkey: 1 }, options: { unique: true, background: true } },
                { key: { creator_did: 1 }, options: { background: true } },
                { key: { updated_at: 1 }, options: { background: true } },
                { key: { deleted: 1 }, options: { background: true } }
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

    async fixNullArrays() {
        if (this.noMongoDB || this.noDBWrites) {
            return;
        }
    
        try {
            // Fix users with null pack_ids
            await this.db.collection('users').updateMany(
                { pack_ids: null },
                { $set: { pack_ids: [] } }
            );
    
            // Fix users with null created_packs
            await this.db.collection('users').updateMany(
                { created_packs: null },
                { $set: { created_packs: [] } }
            );
    
            logger.info('Fixed null arrays in database');
        } catch (err) {
            logger.error('Error fixing null arrays:', err);
            throw err;
        }
    }

    async safeWrite(collection, operation, options = {}) {
        const operationId = `${collection}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        const startTime = Date.now();
        const stats = { retries: 0, delays: 0 };
    
        const cleanForLogging = (obj) => {
            if (!obj) return null;
            const cleaned = JSON.parse(JSON.stringify(obj));
            delete cleaned.session;
            delete cleaned.sessionPool;
            delete cleaned.client;
            return cleaned;
        };
    
        // Initial debug log
        logger.debug(`MongoDB write operation details:`, {
            operationId,
            collection,
            operation: {
                filter: cleanForLogging(operation.filter),
                update: cleanForLogging(operation.update)
            },
            options: cleanForLogging(options)
        });
    
        try {
            // Validation
            if (!operation.filter || !operation.update) {
                throw new Error(`Invalid operation parameters for ${collection}`);
            }
    
            // Special handling for created_at conflicts
            if (options.upsert && operation.update.$set?.created_at && operation.update.$setOnInsert?.created_at) {
                logger.debug(`Detected created_at conflict, reorganizing update operation`, {
                    operationId,
                    collection
                });
                
                // Check if document exists
                const existing = await this.db.collection(collection)
                    .findOne(operation.filter, { projection: { _id: 1, created_at: 1 } });
    
                if (existing) {
                    // Use existing created_at
                    delete operation.update.$setOnInsert.created_at;
                    operation.update.$set.created_at = existing.created_at;
                } else {
                    // Document doesn't exist - move created_at to $setOnInsert only
                    const createdAt = operation.update.$set.created_at || operation.update.$setOnInsert.created_at;
                    delete operation.update.$set.created_at;
                    operation.update.$setOnInsert.created_at = createdAt;
                }
            }
    
            // Execute write with retries
            const maxRetries = 5;
            let lastError = null;
    
            for (let attempt = 0; attempt < maxRetries; attempt++) {
                try {
                    await this.ensureSession();
                    await this.enforceOperationDelay();
    
                    const writeOptions = {
                        ...options,
                        session: this.session
                    };
    
                    const result = await this.db.collection(collection)
                        .updateOne(
                            operation.filter,
                            operation.update,
                            writeOptions
                        );
    
                    // Verify write if requested
                    if (options._requiresAck) {
                        const verifiedDoc = await this.db.collection(collection)
                            .findOne(operation.filter, { session: this.session });
    
                        if (!verifiedDoc) {
                            throw new Error('Write verification failed');
                        }
    
                        logger.debug(`Write verified:`, {
                            operationId,
                            collection,
                            exists: true,
                            document: cleanForLogging(verifiedDoc)
                        });
                    }
    
                    // Log success with details
                    logger.info(`Write operation successful:`, {
                        operationId,
                        collection,
                        duration: Date.now() - startTime,
                        stats: {
                            matchedCount: result.matchedCount,
                            modifiedCount: result.modifiedCount,
                            upsertedCount: result.upsertedCount,
                            retries: stats.retries,
                            delays: stats.delays
                        }
                    });
    
                    return result;
    
                } catch (err) {
                    lastError = err;
                    stats.retries++;
    
                    // Handle specific error types
                    if (err.code === 40 && err.message.includes('created_at')) {
                        // This is our conflict error - try to fix it
                        logger.warn(`created_at conflict detected, retrying with fixed operation`, {
                            operationId,
                            attempt: attempt + 1
                        });
                        // Remove created_at from $set if it exists in both
                        if (operation.update.$set?.created_at && operation.update.$setOnInsert?.created_at) {
                            delete operation.update.$set.created_at;
                        }
                        continue;
                    }
                    
                    if (err.code === 11000) { // Duplicate key
                        if (collection === 'users') {
                            logger.warn(`Duplicate key detected, retrying without upsert:`, {
                                operationId,
                                attempt: attempt + 1,
                                error: err.message
                            });
                            options.upsert = false;
                            continue;
                        }
                        throw err; // For non-users collections, fail fast on duplicates
                    }
    
                    if (this.isCosmosThrottlingError(err)) {
                        stats.delays++;
                        await this.handleThrottlingError(err);
                        continue;
                    }
    
                    if (err.message.includes('session')) {
                        logger.warn(`Session error, recreating:`, {
                            operationId,
                            attempt: attempt + 1,
                            error: err.message
                        });
                        await this.ensureSession();
                        continue;
                    }
    
                    // Log failure details
                    logger.error(`Write attempt failed:`, {
                        operationId,
                        collection,
                        attempt: attempt + 1,
                        error: {
                            message: err.message,
                            code: err.code,
                            stack: err.stack
                        }
                    });
    
                    // If not retryable, stop immediately
                    if (!this.isRetryableError(err)) {
                        throw err;
                    }
    
                    // Apply exponential backoff
                    const backoffMs = Math.min(1000 * Math.pow(2, attempt), 30000);
                    stats.delays++;
                    await new Promise(resolve => setTimeout(resolve, backoffMs));
                }
            }
    
            throw lastError || new Error(`Write failed after ${maxRetries} attempts`);
    
        } catch (err) {
            // Log comprehensive error information
            logger.error(`Write operation failed:`, {
                operationId,
                collection,
                duration: Date.now() - startTime,
                stats,
                error: {
                    message: err.message,
                    code: err.code,
                    stack: err.stack
                },
                operation: {
                    filter: cleanForLogging(operation.filter),
                    update: cleanForLogging(operation.update)
                }
            });
            throw err;
        }
    }
    
    // Helper method to determine if error is retryable
    isRetryableError(err) {
        return (
            err.code === 11000 || // Duplicate key
            this.isCosmosThrottlingError(err) ||
            err.message.includes('session') ||
            err.message.includes('connection') ||
            err.code === 'ETIMEDOUT' ||
            err.name === 'MongoNetworkError' ||
            (err.code >= 500 && err.code < 600) // Server errors
        );
    }

    async safeBulkWrite(collection, operations, options = {}) {
        const operationId = `bulk-${collection}-${Date.now()}`;
        const stats = {
            startTime: Date.now(),
            processed: 0,
            successful: 0,
            failed: 0,
            retried: 0,
            duplicates: 0,
            batches: 0,
            errors: []
        };
    
        // Early validation
        if (!Array.isArray(operations) || operations.length === 0) {
            logger.debug('No operations to bulk write');
            return { acknowledged: true, stats };
        }
    
        // Operation analysis and preparation
        const normalizedOps = operations.map(op => {
            if (!op.updateOne && !op.insertOne && !op.deleteOne) {
                return {
                    updateOne: {
                        filter: op.filter,
                        update: op.update,
                        upsert: op.upsert ?? true
                    }
                };
            }
            return op;
        });
    
        // Calculate optimal batch size
        const calculateBatchSize = (ops) => {
            const baseSize = this.isCosmosDb ? 
                BATCH_SIZES[this.dbType] : 
                BATCH_SIZES.mongodb;
    
            // Reduce batch size if operations are complex
            const complexOps = ops.filter(op => 
                (op.updateOne?.update.$set && 
                 Object.keys(op.updateOne.update.$set).length > 10) ||
                op.updateOne?.arrayFilters?.length > 0
            ).length;
    
            return Math.max(1, Math.floor(baseSize * (1 - (complexOps / ops.length) * 0.5)));
        };
    
        const batchSize = calculateBatchSize(normalizedOps);
    
        logger.info(`Starting bulk write operation`, {
            operationId,
            collection,
            totalOperations: normalizedOps.length,
            batchSize,
            operationTypes: normalizedOps.reduce((acc, op) => {
                const type = Object.keys(op)[0];
                acc[type] = (acc[type] || 0) + 1;
                return acc;
            }, {})
        });
    
        // Split into batches
        const batches = [];
        for (let i = 0; i < normalizedOps.length; i += batchSize) {
            batches.push(normalizedOps.slice(i, i + batchSize));
        }
    
        const processBatch = async (batch, batchIndex) => {
            const batchStart = Date.now();
            const batchId = `${operationId}-batch-${batchIndex}`;
            
            try {
                if (this.isCosmosDb) {
                    // Process sequentially for Cosmos DB
                    for (const op of batch) {
                        try {
                            await this.safeWrite(collection, {
                                filter: op.updateOne?.filter || op.filter,
                                update: op.updateOne?.update || op.update
                            }, {
                                ...options,
                                upsert: op.updateOne?.upsert ?? true
                            });
                            stats.successful++;
                        } catch (err) {
                            if (err.code === 11000) {
                                stats.duplicates++;
                                // Retry without upsert
                                try {
                                    await this.safeWrite(collection, {
                                        filter: op.updateOne?.filter || op.filter,
                                        update: op.updateOne?.update || op.update
                                    }, {
                                        ...options,
                                        upsert: false
                                    });
                                    stats.retried++;
                                    stats.successful++;
                                } catch (retryErr) {
                                    stats.failed++;
                                    stats.errors.push({
                                        batchId,
                                        error: retryErr.message,
                                        code: retryErr.code,
                                        operation: op
                                    });
                                }
                            } else {
                                stats.failed++;
                                stats.errors.push({
                                    batchId,
                                    error: err.message,
                                    code: err.code,
                                    operation: op
                                });
                            }
                        }
                        stats.processed++;
                    }
                } else {
                    // Bulk write for MongoDB
                    try {
                        const result = await this.db.collection(collection)
                            .bulkWrite(batch, {
                                ordered: false,
                                ...options,
                                session: this.session
                            });
                        
                        stats.successful += result.modifiedCount + result.upsertedCount;
                        stats.processed += batch.length;
                    } catch (err) {
                        if (err.code === 11000) {
                            // Handle duplicates by retrying each operation
                            logger.warn(`Handling duplicates in batch ${batchId}`);
                            for (const op of batch) {
                                try {
                                    await this.safeWrite(collection, {
                                        filter: op.updateOne?.filter || op.filter,
                                        update: op.updateOne?.update || op.update
                                    }, {
                                        ...options,
                                        upsert: false
                                    });
                                    stats.successful++;
                                    stats.duplicates++;
                                    stats.retried++;
                                } catch (retryErr) {
                                    if (retryErr.code !== 11000) {
                                        stats.failed++;
                                        stats.errors.push({
                                            batchId,
                                            error: retryErr.message,
                                            code: retryErr.code,
                                            operation: op
                                        });
                                    }
                                }
                            }
                            stats.processed += batch.length;
                        } else {
                            throw err;
                        }
                    }
                }
    
                stats.batches++;
                const batchDuration = Date.now() - batchStart;
                
                // Log progress
                if (stats.batches % 5 === 0 || stats.processed === normalizedOps.length) {
                    const progress = (stats.processed / normalizedOps.length * 100).toFixed(1);
                    const opsPerSecond = (stats.processed / ((Date.now() - stats.startTime) / 1000)).toFixed(1);
                    
                    logger.info(`Bulk write progress`, {
                        operationId,
                        progress: `${progress}%`,
                        processed: stats.processed,
                        successful: stats.successful,
                        failed: stats.failed,
                        duplicates: stats.duplicates,
                        retried: stats.retried,
                        opsPerSecond,
                        lastBatchDuration: batchDuration
                    });
                }
    
            } catch (err) {
                logger.error(`Batch processing failed`, {
                    operationId,
                    batchId,
                    error: err.message,
                    code: err.code,
                    batchSize: batch.length
                });
                throw err;
            }
        };
    
        try {
            // Process batches with controlled concurrency
            const concurrencyLimit = this.isCosmosDb ? 1 : 3;
            
            for (let i = 0; i < batches.length; i += concurrencyLimit) {
                const currentBatches = batches.slice(i, Math.min(i + concurrencyLimit, batches.length));
                await Promise.all(
                    currentBatches.map((batch, index) => 
                        processBatch(batch, i + index)
                    )
                );
    
                // Throttle between batch groups
                if (i + concurrencyLimit < batches.length) {
                    await this.enforceOperationDelay();
                }
            }
    
            // Final statistics
            const duration = Date.now() - stats.startTime;
            logger.info(`Bulk write operation completed`, {
                operationId,
                duration: `${(duration / 1000).toFixed(1)}s`,
                totalOperations: normalizedOps.length,
                successful: stats.successful,
                failed: stats.failed,
                duplicates: stats.duplicates,
                retried: stats.retried,
                opsPerSecond: (stats.processed / (duration / 1000)).toFixed(1),
                errorRate: `${((stats.failed / stats.processed) * 100).toFixed(1)}%`
            });
    
            return {
                acknowledged: true,
                ...stats,
                duration
            };
    
        } catch (err) {
            logger.error(`Bulk write operation failed`, {
                operationId,
                error: err.message,
                code: err.code,
                stats
            });
            throw err;
        }
    }

    isCosmosThrottlingError(err) {
        return err.code === 16500 || 
               err.code === 429 || 
               err.message?.includes('TooManyRequests') ||
               err.message?.includes('Request rate is large') ||
               err.message?.includes('RetryAfterMs');
    }

    async cleanup() {
            if (this.session) {
                try {
                    await this.session.endSession();
                } catch (err) {
                    logger.error('Error ending session:', err);
                }
                this.session = null;
            }
            if (this.client) {
                try {
                    await this.client.close(true);
                } catch (err) {
                    logger.error('Error closing MongoDB connection:', err);
                }
            }
        }
    
        async markPackStatus(rkey, status, reason) {
            const timestamp = new Date().toISOString();
        
            try {
                const update = {
                    status,
                    status_reason: reason,
                    status_updated_at: timestamp,
                    last_updated: timestamp
                };
        
                if (status === 'deleted') {
                    update.deleted = true;
                    update.deleted_at = timestamp;
                    update.deletion_reason = reason;
                }
        
                await this.safeWrite('starter_packs', {
                    filter: { rkey },
                    update: { $set: update }
                });
        
                // Handle cascading user updates only for deletion
                if (status === 'deleted') {
                    await this.safeBulkWrite('users', [
                        {
                            updateMany: {
                                filter: { pack_ids: rkey },
                                update: {
                                    $pull: { pack_ids: rkey },
                                    $set: { last_updated: timestamp }
                                }
                            }
                        },
                        {
                            updateMany: {
                                filter: { created_packs: rkey },
                                update: {
                                    $pull: { created_packs: rkey },
                                    $set: { last_updated: timestamp }
                                }
                            }
                        }
                    ]);
                }
        
            } catch (err) {
                logger.error(`Failed to mark pack ${rkey} as ${status}:`, err);
                throw err;
            }
        }
    
    async markPackDeleted(rkey, reason, skipUserUpdates = false) {
        logger.debug('markPackDeleted:', { rkey, reason, skipUserUpdates });
        try {
            const existingPack = await this.db.collection('starter_packs')
                .findOne({ rkey });
    
            if (!existingPack) {
                logger.warn(`Attempted to mark non-existent pack as deleted: ${rkey}`);
                return;
            }
    
            // Mark pack as deleted with metadata
            await this.safeWrite('starter_packs', {
                filter: { rkey },
                update: {
                    $set: {
                        deleted: true,
                        deleted_at: new Date(),
                        deletion_reason: reason,
                        members_at_deletion: existingPack.users?.length || 0,
                        last_known_state: {
                            creator_did: existingPack.creator_did,
                            creator: existingPack.creator,
                            name: existingPack.name,
                            updated_at: existingPack.updated_at
                        }
                    }
                }
            });
    
            // Update users only if not skipped
            if (!skipUserUpdates) {
                await this.safeBulkWrite('users', [
                    {
                        updateMany: {
                            filter: { pack_ids: rkey },
                            update: {
                                $pull: { pack_ids: rkey },
                                $set: { last_updated: new Date() }
                            }
                        }
                    },
                    {
                        updateMany: {
                            filter: { created_packs: rkey },
                            update: {
                                $pull: { created_packs: rkey },
                                $set: { last_updated: new Date() }
                            }
                        }
                    }
                ]);
            }
    
            logger.info(`Pack ${rkey} marked as deleted`, {
                reason,
                creator: existingPack.creator_did,
                memberCount: existingPack.users?.length || 0,
                name: existingPack.name,
                timestamp: new Date().toISOString(),
                skipUserUpdates
            });
    
        } catch (err) {
            logger.error(`Failed to mark pack ${rkey} as deleted:`, err);
            throw err;
        }
    }

    async cleanupRemovedUsers(rkey, removedDids) {
        logger.debug('cleanupRemovedUsers:', { rkey, removedDids });
        try {
            // First find all affected users
            const existingUsers = await this.db.collection('users')
                .find({ did: { $in: removedDids } })
                .toArray();

            const existingDids = new Set(existingUsers.map(u => u.did));

            // Only process existing users
            const bulkOps = removedDids
                .filter(did => existingDids.has(did))
                .map(did => ({
                    updateOne: {
                        filter: { 
                            did,
                            deleted: { $ne: true }
                        },
                        update: {
                            $pull: { pack_ids: rkey },
                            $set: { 
                                last_updated: new Date().toISOString()
                            }
                        },
                        upsert: false
                    }
                }));

            if (bulkOps.length > 0) {
                await this.safeBulkWrite('users', bulkOps);
            }

            // Update deletion status for users with no packs
            for (const did of existingDids) {
                const user = await this.db.collection('users').findOne({ did });
                if (user && (!user.pack_ids || user.pack_ids.length === 0)) {
                    await this.safeWrite('users', {
                        filter: { did },
                        update: {
                            $set: { 
                                deleted: true, 
                                deleted_at: new Date().toISOString(),
                                deletion_reason: 'no_remaining_packs'
                            }
                        }
                    }, { upsert: false });
                }
            }

            logger.info(`Cleaned up ${removedDids.length} removed users for pack ${rkey}`, {
                existing: existingDids.size,
                processed: bulkOps.length
            });
        } catch (err) {
            logger.error(`Failed to cleanup removed users for pack ${rkey}:`, err);
            throw err;
        }
    }

    async handleCascadingDeletions(did, type) {
        if (type === 'user') {
            // Find all packs created by this user
            const createdPacks = await this.db.collection('starter_packs')
                .find({ creator_did: did })
                .toArray();
                
            for (const pack of createdPacks) {
                await this.markPackDeleted(pack.rkey, 'creator_deleted', {
                    creatorDid: did,
                    deletionTime: new Date(),
                    memberCount: pack.users?.length || 0
                });
            }
            
            // Update all packs where user was a member
            await this.db.collection('starter_packs').updateMany(
                { users: did },
                { 
                    $pull: { users: did },
                    $set: { last_updated: new Date() }
                }
            );
        }
    }
}

class MainProcessor {
    constructor(options = {}) {
        const {
            noMongoDB = false,
            noDBWrites = false,
            fromApi = false,
            debug = false,
            mode = 'normal'  // 'normal', 'quick', 'purge', 'cleanup', 'maintenance'
        } = options;

        // Store configuration
        this.config = {
            noMongoDB,
            noDBWrites,
            fromApi,
            debug,
            mode
        };

        // initialization tracking
        this.initialized = {
            api: false,
            db: false,
            files: false,
            taskManager: false
        };

        // Only create core components (don't initialize)
        this.rateLimiter = new RateLimiter();
        this.metrics = metrics;
        this.validator = new ValidationHelper(VALIDATION_SCHEMAS);
        this.fileHandler = new FileHandler();
        // Don't create TaskManager yet
        this.taskManager = null;  // Will be created after DB init
        
        // Initialize status tracking
        this.initialized = {
            api: false,
            db: false,
            files: false,
            taskManager: false
        };

        this.profileCache = new Map();
        this.profileCacheTTL = CACHE_CONFIG.profileCacheTTL;
    }

    // Replace with consolidated mode-specific methods:
    async initializeComponents(mode = 'normal') {
        switch (mode) {
            case 'api_only':
                await this.initializeApi();
                break;
                
            case 'db_only':
                await this.initializeDb();
                break;
                
            case 'minimal':
                await this.initializeApi();
                await this.initializeDb();
                break;
                
            case 'normal':
                await this.initializeApi();
                await this.initializeDb();
                // Don't auto-initialize files - let main control this
                break;
        }
    }

    // Core initialization methods
    async initializeApi() {
        if (this.initialized.api) return;

        logger.debug('Initializing API components...');
        this.agent = new BskyAgent({
            service: API_CONFIG.baseURLs.primary
        });

        try {
            await this.agent.login({
                identifier: process.env.BSKY_USERNAME,
                password: process.env.BSKY_PASSWORD
            });
            
            this.apiHandler = new ApiHandler(
                this.agent, 
                this.rateLimiter,
                this.fileHandler
            );

            this.verificationHandler = new ErrorVerificationHandler({
                debugManager: this.debugManager,
                metrics: this.metrics,
                fileHandler: this.fileHandler,
                dbManager: this.dbManager,
                apiHandler: this.apiHandler
            });

            this.initialized.api = true;
            logger.debug('API components initialized');
        } catch (err) {
            logger.error('API initialization failed:', err);
            throw err;
        }
    }

    async initializeDb() {
        if (this.initialized.db || this.config.noMongoDB) return;

        logger.debug('Initializing database...');
        try {
            if (this.config.noDBWrites) {
                this.dbManager = new MockDatabaseManager();
            } else {
                const dbType = process.env.DB_TYPE || 'cosmos';
                const dbConfig = {
                    ...DB_CONFIGS[dbType],
                    maxPoolSize: this.config.mode === 'quick' ? 50 : 10,
                    minPoolSize: this.config.mode === 'quick' ? 10 : 5,
                    waitQueueTimeoutMS: 30000,
                    serverSelectionTimeoutMS: 30000
                };

                this.mongoClient = new MongoClient(process.env.MONGODB_URI, dbConfig);
                this.dbManager = new DatabaseManager(
                    this.mongoClient,
                    dbType,
                    this.logger,
                    'starterpacks'
                );
            }
            
            await this.dbManager.init();
            await this.dbManager.ensureSession();
            
            // Create TaskManager after DB is initialized
            this.taskManager = new TaskManager(
                this.fileHandler, 
                this.config.debug, 
                this.dbManager, 
                this.config.noMongoDB
            );
            
            this.initialized.db = true;
            logger.debug('Database initialized');
        } catch (err) {
            logger.error('Database initialization failed:', err);
            throw err;
        }
    }

    async initializeFiles() {
        if (this.initialized.files) return;

        logger.debug('Initializing file handler...');
        try {
            await this.fileHandler.init();
            this.initialized.files = true;
            logger.debug('File handler initialized');
        } catch (err) {
            logger.error('File handler initialization failed:', err);
            throw err;
        }
    }

    // Factory method for creating processor instances
    static async create(options) {
        const processor = new MainProcessor(options);
        
        // Do base initialization based on mode
        switch(options.mode) {
            case 'purge':
                // Nothing to initialize
                break;
                
            case 'cleanup':
                await processor.initializeDb();
                break;
                
            case 'quick':
                await processor.initializeApi();
                await processor.initializeDb();
                await processor.initializeFiles();
                break;

            case 'maintenance':
                await processor.initializeDb();
                await processor.initializeFiles();
                break;
                
            case 'normal':
                await processor.initializeApi();
                await processor.initializeDb();
                break;
        }
        
        return processor;
    }

    // Processing methods
    async processTasks(tasks) {
        let processedCount = 0;
        let failedCount = 0;
    
        for (const task of tasks) {
            try {
                // Add each task to pendingTasks first
                this.taskManager.pendingTasks.set(task.rkey, task);
                
                const success = await this.taskManager.processNextTask(this);
                
                if (success) {
                    processedCount++;
                    if (processedCount % 10 === 0) {
                        await this.taskManager.maybeWriteCheckpoint();
                    }
                } else {
                    failedCount++;
                }
    
                if ((processedCount + failedCount) % 100 === 0) {
                    logger.info('Processing status:', {
                        remaining: tasks.length - (processedCount + failedCount),
                        processed: processedCount,
                        failed: failedCount,
                        successRate: `${((processedCount / (processedCount + failedCount)) * 100).toFixed(1)}%`
                    });
                }
            } catch (err) {
                logger.error(`Failed to process task ${task.rkey}:`, err);
                failedCount++;
            }
        }
    
        return { processedCount, failedCount };
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
        
        // Rate limit handling
        if (this.isRateLimitError(err)) {
            await this.rateLimiter.handleResponse(err);
            return { retry: true };
        }
    
        // Not found handling
        if (this.isNotFoundError(err)) {
            return { retry: false };
        }
    
        // Log error with context
        logger.error(`Operation error: ${operation}`, {
            error: err.message,
            status: err.status,
            operation,
            data,
            stack: err.stack
        });
    
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
            
            // Create API handler after successful authentication, passing fileHandler
            this.apiHandler = new ApiHandler(
                this.agent, 
                this.rateLimiter,
                this.fileHandler  // Pass fileHandler
            );
            
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

    async initMinimal_old() {
        logger.debug('Minimal initialization for quick process');
        
        // Initialize API components
        await this.initializeApiComponents();
        
        // Initialize file handler (needed for lookups)
        await this.fileHandler.init();
        
        // Initialize DB if needed, but don't load all data
        if (!this.noMongoDB) {
            if (!this.noDBWrites) {
                const dbType = process.env.DB_TYPE || 'cosmos';
                const dbConfig = {
                    ...DB_CONFIGS[dbType],
                    maxPoolSize: 50,
                    minPoolSize: 10,
                    waitQueueTimeoutMS: 30000,
                    serverSelectionTimeoutMS: 30000
                };
                
                this.mongoClient = new MongoClient(process.env.MONGODB_URI, dbConfig);
                this.dbManager = new DatabaseManager(
                    this.mongoClient,
                    dbType,
                    this.logger,
                    'starterpacks'
                );
                await this.dbManager.init();
                await this.dbManager.ensureSession();
            } else {
                this.dbManager = new MockDatabaseManager();
                await this.dbManager.init();
            }
        }
        
        logger.debug('Minimal initialization complete');
    }

    async removeMembershipWithProof(processor, userDid, packRkey, proof) {
        // Add stronger proof validation
        if (!proof || !proof.type || !proof.verifiedAt) {
            throw new Error('Invalid proof for membership removal');
        }
        
        // Add detailed logging
        logger.info(`Removing membership with proof`, {
            userDid,
            packRkey,
            proof,
            timestamp: new Date().toISOString()
        });
    
        try {
            // Update MongoDB - use $pull to remove this specific membership
            if (!processor.noMongoDB) {
                await processor.dbManager.safeWrite('users', {
                    filter: { did: userDid },
                    update: {
                        $pull: { pack_ids: packRkey }
                    }
                });
    
                // Also update the pack's users array
                await processor.dbManager.safeWrite('starter_packs', {
                    filter: { rkey: packRkey },
                    update: {
                        $pull: { users: userDid }
                    }
                });
            }
    
            // Update local file data
            const userData = await processor.fileHandler.getUser(userDid);
            if (userData) {
                userData.pack_ids = userData.pack_ids.filter(id => id !== packRkey);
                await processor.fileHandler.appendUser(userData);
            }
    
            logger.info(`Successfully removed membership for ${userDid} from pack ${packRkey}`);
            return true;
        } catch (err) {
            logger.error(`Error removing membership for ${userDid} from pack ${packRkey}:`, err);
            throw err;
        }
    }

    async retryWithBackoff(operation, operationName, maxRetries = 3) {
        let lastError;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                return await operation();
            } catch (err) {
                lastError = err;
                if (err.status === 429) {
                    await this.rateLimiter.handleResponse(err);
                } else {
                    await new Promise(resolve => 
                        setTimeout(resolve, Math.pow(2, attempt) * 1000)
                    );
                }
            }
        }
        throw lastError;
    }

    async processStarterPack(urlLine) {
        const stats = {
            startTime: Date.now(),
            stages: {},
            memoryUsage: {},
            userStats: {
                processed: 0,
                newToMongoDB: 0,
                updated: 0,
                removed: 0,
                failed: 0
            },
            associatedPacks: {
                discovered: 0,
                queued: 0
            }
        };
    
        const recordStageTime = (stage) => {
            stats.stages[stage] = {
                endTime: Date.now(),
                duration: Date.now() - (stats.stages[stage]?.startTime || stats.startTime)
            };
            stats.memoryUsage[stage] = process.memoryUsage();
        };
    
        const startStage = (stage) => {
            stats.stages[stage] = { startTime: Date.now() };
            logger.debug(`Starting stage: ${stage}`);
        };
    
        try {
            // Input validation
            if (!urlLine?.includes('|')) {
                throw new Error('Invalid urlLine format');
            }
    
            const [handle, rkey] = urlLine.split('|').map(s => s.trim());
            if (!handle || !rkey) {
                throw new Error('Invalid pack identifier format');
            }
    
            startStage('initialization');
            const taskInfo = this.taskManager?.pendingTasks?.get(rkey);
            const failure = this.taskManager.failures.get(rkey);
    
            // Early exits
            if (failure?.permanent) {
                logger.info(`Skipping permanently failed pack ${rkey}`);
                return false;
            }
    
            if (this.taskManager.completedTasks.has(rkey)) {
                logger.debug(`Skipping already processed pack: ${rkey}`);
                return true;
            }
    
            logger.info(`Processing pack: ${handle}|${rkey}`, {
                taskType: taskInfo?.source || 'direct',
                priority: taskInfo?.priority || 0,
                discoveredFrom: taskInfo?.parentDid || null,
                progress: `${this.taskManager.completedTasks.size}/${this.taskManager.pendingTasks.size + this.taskManager.completedTasks.size}`
            });
            recordStageTime('initialization');
    
            // Load existing state
            startStage('loadExisting');
            const existingPack = await this.fileHandler.getPack(rkey);
            const existingUsers = existingPack ? new Set(existingPack.users) : new Set();
            recordStageTime('loadExisting');
    
            // Creator resolution and pack fetching (with retries)
            startStage('fetchPack');
            let creatorDID;
            try {
                creatorDID = await this.retryWithBackoff(
                    () => this.apiHandler.resolveHandle(handle),
                    'resolve_handle'
                );
            } catch (err) {
                logger.warn(`Creator not found for pack ${rkey}`);
                if (existingPack) {
                    await this.dbManager.markPackDeleted(rkey, 'creator_not_found');
                }
                this.taskManager.recordFailure(rkey, 'creator_not_found');
                return false;
            }
    
            const packUri = `at://${creatorDID}/app.bsky.graph.starterpack/${rkey}`;
            const pack = await this.retryWithBackoff(
                () => this.apiHandler.makeAuthApiCall('app.bsky.graph.getStarterPack', { starterPack: packUri }),
                'fetch_pack'
            );
            recordStageTime('fetchPack');
    
            // Validate pack state
            startStage('validation');
            if (!pack?.starterPack || pack.starterPack.record?.hidden || !pack.starterPack.record?.list) {
                const reason = !pack?.starterPack ? 'pack_not_found' : 
                              pack.starterPack.record?.hidden ? 'hidden' : 
                              'invalid_structure';
                await this.taskManager.markPackStatus(rkey, reason === 'hidden' ? 'hidden' : 'deleted', reason);
                return false;
            }
            recordStageTime('validation');
    
            // Process members
            startStage('processMembers');
            const listMembers = await this.retryWithBackoff(
                () => this.apiHandler.makeApiCall('app.bsky.graph.getList', { 
                    list: pack.starterPack.record.list 
                }),
                'fetch_list'
            );
    
            if (!listMembers?.items?.length) {
                await this.taskManager.markPackStatus(rkey, 'deleted', 'empty_list');
                return false;
            }
    
            // Process in batches
            const BATCH_SIZE = 50;
            const processedUsers = [];
            for (let i = 0; i < listMembers.items.length; i += BATCH_SIZE) {
                const batch = listMembers.items.slice(i, i + BATCH_SIZE);
                const batchResults = await Promise.all(
                    batch.map(member => this.processPackMember(member, rkey))
                );
                processedUsers.push(...batchResults.filter(Boolean));
    
                logger.debug(`Processed member batch ${i + 1}-${i + batch.length}/${listMembers.items.length}`);
            }
            stats.userStats.processed = processedUsers.length;
            recordStageTime('processMembers');
    
            // Handle membership changes
            startStage('membershipChanges');
            const currentUsers = new Set(processedUsers.map(u => u.did));
            
            // Handle removed users
            const removedDids = Array.from(existingUsers).filter(did => !currentUsers.has(did));
            if (removedDids.length > 0) {
                await this.dbManager.cleanupRemovedUsers(rkey, removedDids);
                stats.userStats.removed = removedDids.length;
            }
            recordStageTime('membershipChanges');
    
            // Prepare pack data
            startStage('savePack');
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
    
            // Save data with proper ordering and atomic updates
            await this.fileHandler.appendPack(packData);
            
            // Save users in batches
            for (let i = 0; i < processedUsers.length; i += BATCH_SIZE) {
                const batch = processedUsers.slice(i, i + BATCH_SIZE);
                await Promise.all(batch.map(user => this.fileHandler.appendUser(user)));
            }
            recordStageTime('savePack');
    
            // MongoDB updates
            startStage('mongodb');
            if (!this.noMongoDB && !this.noDBWrites) {
                const writeResults = await this.saveToDB(packData, processedUsers);
                stats.userStats.newToMongoDB = writeResults.newUsers;
                stats.userStats.updated = writeResults.updatedUsers;
            }
            recordStageTime('mongodb');
    
            // Process associated packs
            startStage('associatedPacks');
            const packCreator = { 
                did: creatorDID, 
                handle 
            };
            const associatedResults = await this.processAssociatedPacks(packCreator, {
                forceProcess: false,
                parentPack: rkey
            });
            stats.associatedPacks = associatedResults;
            recordStageTime('associatedPacks');
    
            // Finalize
            startStage('finalization');
            await this.taskManager.markTaskCompleted(rkey);
            await this.taskManager.markPackStatus(rkey, 'completed', null);
            
            // Record metrics
            metrics.recordPackProcessing(true, Date.now() - stats.startTime);
            metrics.recordPackStats({
                rkey,
                ...stats
            });
    
            // Log completion
            logger.info(`Pack ${rkey} processing complete:`, {
                duration: `${((Date.now() - stats.startTime) / 1000).toFixed(1)}s`,
                stages: Object.entries(stats.stages).reduce((acc, [stage, timing]) => {
                    acc[stage] = `${(timing.duration / 1000).toFixed(1)}s`;
                    return acc;
                }, {}),
                userStats: stats.userStats,
                associatedPacks: stats.associatedPacks,
                memoryPeak: Math.max(...Object.values(stats.memoryUsage)
                    .map(m => m.heapUsed)) / 1024 / 1024
            });
            recordStageTime('finalization');
    
            return true;
    
        } catch (err) {
            // Comprehensive error handling
            const errorContext = {
                rkey: urlLine?.split('|')[1],
                stage: Object.keys(stats.stages).pop(),
                duration: Date.now() - stats.startTime,
                processedStats: stats,
                error: {
                    message: err.message,
                    status: err.status,
                    stack: err.stack
                }
            };
    
            if (err.status === 404) {
                await this.handlePackNotFound(errorContext);
            } else if (err.status === 429) {
                await this.rateLimiter.handleResponse(err);
                logger.warn('Rate limit hit, will retry', errorContext);
                return false;
            } else {
                logger.error('Pack processing failed:', errorContext);
                await this.taskManager.recordFailure(
                    errorContext.rkey, 
                    err.message,
                    err.status === 404
                );
            }
    
            metrics.recordPackProcessing(false);
            metrics.recordError('process_pack', err);
            
            return false;
        }
    }

    async getListMembers(uri) {
        try {
            logger.debug(`getListMembers for uri: ${uri}`)
            // Try public API first
            try {
                const publicData = await this.ApiHandler.makePublicApiCall('app.bsky.graph.getList', {
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
                if (this.isRateLimitError(err)) {
                    await this.rateLimiter.handleResponse(err);
                    return this.getProfile(did);
                }
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
            if (this.isRateLimitError(err)) {
                await this.rateLimiter.handleResponse(err);
                return this.getProfile(did);
            }
            throw err;
        }
    }

    async processAssociatedPacks(currentProfile, options = {}) {
        try {
            logger.info(`Processing associated packs for ${currentProfile.handle} (${currentProfile.did})`);
            const packs = await this.apiHandler.getActorStarterPacks(currentProfile.did);
            
            if (!packs?.starterPacks?.length) {
                logger.debug(`No associated packs found for ${currentProfile.handle}`);
                return { discovered: 0, queued: 0, skipped: 0, failed: 0 };
            }
    
            const results = {
                discovered: packs.starterPacks.length,
                queued: 0,
                skipped: 0,
                failed: 0
            };
    
            logger.info(`Found ${results.discovered} associated packs for ${currentProfile.handle}`);
    
            for (const pack of packs.starterPacks) {
                try {
                    const rkey = await this.extractRkeyFromURI(pack.uri);
                    
                    // Get member count properly from the list data
                    let memberCount = 0;
                    try {
                        if (pack.record?.list) {
                            const listData = await this.apiHandler.makeApiCall(
                                'app.bsky.graph.getList',
                                { list: pack.record.list }
                            );
                            memberCount = listData?.items?.length || 0;
                        }
                    } catch (err) {
                        logger.warn(`Failed to get member count for pack ${rkey}: ${err.message}`);
                    }
                    
                    // Check if pack exists in MongoDB
                    const existsInMongoDB = !this.noMongoDB && await this.dbManager.db
                        .collection('starter_packs')
                        .findOne({ rkey }, { projection: { _id: 1 } });
    
                    if (!existsInMongoDB) {
                        logger.debug(`Adding new pack ${rkey} from ${currentProfile.handle} (${memberCount} members)`);
                        const added = await this.taskManager.addAssociatedPack({
                            rkey,
                            creator: currentProfile.handle,
                            memberCount,
                            uri: pack.uri,
                            name: pack.record?.name,
                            description: pack.record?.description,
                            createdAt: pack.record?.createdAt,
                            list: pack.record?.list
                        }, currentProfile.did);
    
                        if (added) {
                            results.queued++;
                        }
                    } else {
                        results.skipped++;
                        logger.debug(`Skipped existing pack ${rkey} from ${currentProfile.handle}`);
                    }
    
                } catch (err) {
                    results.failed++;
                    logger.error(`Failed to process pack from ${currentProfile.handle}:`, err);
                }
            }
    
            logger.info(`Associated packs processing complete for ${currentProfile.handle}:`, results);
            return results;
        } catch (err) {
            logger.error(`Failed to process packs for ${currentProfile.handle}:`, err);
            throw err;
        }
    }
    
    async extractRkeyFromURI(uri) {
        const match = uri.match(/starterpack\/([a-zA-Z0-9]+)$/);
        if (!match) {
            throw new Error(`Invalid starter pack URI: ${uri}`);
        }
        const rkey = match[1];
        logger.debug(`rkey extracted: ${rkey}.`);
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
            source = 'direct',
            existingMemberships = [],     
            existingCreatedPacks = [],
            forceProcess = false,     
        } = options;
    
        const startTime = Date.now();
        const changes = {
            renamed: [],
            updated: [],
            added: [],
            failed: [],
            removed: [],
            packs: { discovered: 0, processed: 0, failed: 0 }
        };
    
        try {
            // 1. Validate initial input
            if (!profile?.did) {
                logger.warn('Invalid profile data provided');
                return { success: false, changes, error: 'Invalid profile data' };
            }
    
            // 2. Get current profile state from API
            const currentProfile = await this.apiHandler.getProfile(profile.did);
            logger.debug('getProfile result to process:', currentProfile);
            
            // 3. Handle non-existent/deleted profiles
            if (!currentProfile) {
                // ... deletion handling code stays the same ...
                return { success: false, changes, error: 'Profile not found' };
            }
    
            // 4. Get existing profile state from files ONLY if we need historical data
            const existingProfile = await this.fileHandler.getUser(profile.did);
            logger.debug('Existing profile data:', existingProfile);
    
            // 5. Detect changes
            const profileState = {
                isNew: !existingProfile,
                isRenamed: existingProfile && existingProfile.handle !== currentProfile.handle,
                hasDisplayNameChange: existingProfile && existingProfile.display_name !== currentProfile.displayName,
                hasPackMemberships: existingProfile?.pack_ids?.length > 0,
                isCreator: existingProfile?.created_packs?.length > 0
            };
    
            // 6. Early return ONLY if not forced AND not quick_process AND no packs to discover
            if (!force && source !== 'quick_process' && existingProfile) {
                const daysSinceUpdate = (Date.now() - new Date(existingProfile.last_updated).getTime()) 
                    / (1000 * 60 * 60 * 24);
                // Only skip if we have no associated packs to discover
                if (daysSinceUpdate < 7 && !currentProfile.associated?.starterPacks) {
                    return { success: true, changes, cached: true };
                }
            }
    
            // 7. Prepare user data
            const userData = {
                did: currentProfile.did,
                handle: currentProfile.handle,
                display_name: currentProfile.displayName || '',
                description: currentProfile.description || '',
                followers_count: currentProfile.followersCount || 0,
                follows_count: currentProfile.followsCount || 0,
                posts_count: currentProfile.postsCount || 0,
                last_updated: new Date().toISOString(),
                handle_history: [
                    ...(existingProfile?.handle_history || []),
                    profileState.isRenamed ? {
                        oldHandle: existingProfile.handle,
                        timestamp: existingProfile.last_updated
                    } : []
                ].filter(Boolean),
                pack_ids: [...new Set([
                    ...(existingProfile?.pack_ids || []),
                    ...(rkey ? [rkey] : [])
                ])],
                created_packs: [...new Set([
                    ...(existingProfile?.created_packs || []),
                    ...existingCreatedPacks
                ])]
            };
    
            // 8. Update files first (append-only operations)
            logger.debug('appending user data to files:', userData);
            await this.fileHandler.appendUser(userData);
    
            // 9. Update creator's packs if name changed
            if ((profileState.isRenamed || profileState.hasDisplayNameChange) && profileState.isCreator) {
                logger.debug('Namechange detected.');
                const affectedPacks = await this.fileHandler.getPacksByCreator(currentProfile.did);
                
                for (const pack of affectedPacks) {
                    const updatedPack = {
                        ...pack,
                        creator: currentProfile.handle,
                        creator_display_name: currentProfile.displayName,
                        updated_at: new Date().toISOString()
                    };
                    await this.fileHandler.appendPack(updatedPack);
                }
    
                // Update MongoDB after files
                if (!this.noMongoDB && !this.noDBWrites) {
                    await this.dbManager.safeWrite('starter_packs', {
                        filter: { creator_did: currentProfile.did },
                        update: {
                            $set: {
                                creator: currentProfile.handle,
                                creator_display_name: currentProfile.displayName,
                                last_updated: new Date().toISOString()
                            }
                        }
                    });
                }
            }
    
            // 10. Update MongoDB user data if enabled
            if (!this.noMongoDB && !this.noDBWrites) {
                logger.debug('Writing user data to MongoDB:', userData);

                // First cleanup any $each artifacts
                await this.dbManager.db.collection('users').updateOne(
                    { did: currentProfile.did },
                    { $set: { pack_ids: [], created_packs: [] } }
                );

                // Then get existing data
                const existingDoc = await this.dbManager.db.collection('users')
                    .findOne(
                        { did: currentProfile.did }, 
                        { projection: { pack_ids: 1, created_packs: 1 } }
                    );

                // Clean and merge arrays
                const cleanPackIds = Array.isArray(existingDoc?.pack_ids) ? 
                    existingDoc.pack_ids.filter(id => typeof id === 'string') : [];
                const cleanCreatedPacks = Array.isArray(existingDoc?.created_packs) ?
                    existingDoc.created_packs.filter(id => typeof id === 'string') : [];

                // Create clean update operation
                const updateOp = {
                    filter: { did: currentProfile.did },
                    update: {
                        $set: {
                            ...userData,
                            pack_ids: [...new Set([
                                ...cleanPackIds,
                                ...(userData.pack_ids || []),
                                ...(rkey ? [rkey] : [])
                            ])].filter(id => typeof id === 'string'),
                            created_packs: [...new Set([
                                ...cleanCreatedPacks,
                                ...(userData.created_packs || []),
                                ...existingCreatedPacks
                            ])].filter(id => typeof id === 'string')
                        }
                    }
                };

                logger.debug('MongoDB update operation:', {
                    filter: updateOp.filter,
                    arrays: {
                        pack_ids: updateOp.update.$set.pack_ids,
                        created_packs: updateOp.update.$set.created_packs
                    }
                });

                await this.dbManager.safeWrite('users', updateOp);
            }
    
            // 11. Process associated packs if needed
            if (processAssociated && currentProfile.associated?.starterPacks > 0) {
                logger.debug(`Processing ${currentProfile.associated.starterPacks} associated packs.`);
                const packResults = await this.processAssociatedPacks(currentProfile, {
                    parentDid: currentProfile.did,
                    processingId,
                    forceProcess
                });
                changes.packs = packResults;

                // For quick process, handle discovered packs immediately
                if (source === 'quick_process') {
                    while (this.taskManager.pendingTasks.size > 0) {
                        logger.debug('Processing next task...');
                        await this.taskManager.processNextTask(this);
                    }
                }
            }
    
            // 12. Record changes
            logger.debug('recording state changes...');
            if (profileState.isNew) changes.added.push(currentProfile.did);
            if (profileState.isRenamed) changes.renamed.push({
                did: currentProfile.did,
                oldHandle: existingProfile.handle,
                newHandle: currentProfile.handle,
                timestamp: new Date().toISOString()
            });
            if (!profileState.isNew) changes.updated.push(currentProfile.did);
    
            // Record metrics
            metrics?.recordUserProcessing(true);
            metrics?.recordProfileProcessing(Date.now() - startTime);

            // If this was part of a task, mark it complete
            if (options.parentPack) {
                await this.taskManager.markTaskCompleted(options.parentPack);
            }
    
            return { success: true, changes };
    
        } catch (err) {
            metrics?.recordUserProcessing(false);
            metrics?.recordError('profile_processing', err);
    
            logger.error(`Error processing profile ${profile?.did}:`, {
                error: err.message,
                status: err.status,
                processingId,
                parentPack,
                stack: err.stack
            });
    
            changes.failed.push({
                did: profile.did,
                reason: err.message,
                status: err.status,
                timestamp: new Date().toISOString()
            });
    
            return { success: false, changes, error: err.message };
        }
    }

    async processListMembers(listdata, packRkey) {
        //logger.debug(`Processing List `, listdata, packRkey);
        const processedUsers = [];
        const seenDids = new Set();
        const totalMembers = listdata.list.listItemCount;
        let processedCount = 0;
    
        if (this.debug) {
            logger.debug(`Starting to process ${totalMembers} members for pack ${packRkey}`);
        }
    
        for (const member of listdata.items) {
            processedCount++;
            try {
                const memberDid = member.did || (member.subject && member.subject.did);
        
                if (!memberDid) {
                    logger.warn(`Invalid member data in pack ${packRkey}`);
                    continue;
                }

                if (seenDids.has(memberDid)) {
                    logger.warn(`Duplicate member ${memberDid} in pack ${packRkey}`);
                    continue;
                }

                seenDids.add(memberDid);

                // Check if we should process this user
                if (!this.taskManager.shouldProcessUser(memberDid)) {
                    continue;
                }
                
                // Get full profile for the member to check for associated packs
                const memberProfile = await this.apiHandler.getProfile(memberDid);
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
                if (this.isRateLimitError(err)) {
                    await this.rateLimiter.handleResponse(err);
                    continue;
                }
                logger.warn(`Failed to process member in pack ${packRkey}:`, err);
                if (err.status === 404) {
                    this.taskManager.recordMissingProfile(memberDid);
                }
            }
        }
    
        return processedUsers;
    }

    async saveToDB(packData, users) {
        if (this.noMongoDB) return { newUsers: 0, updatedUsers: 0 };
    
        try {
            const writeResults = { newUsers: 0, updatedUsers: 0 };
    
            // 1. Format pack data
            
            const formattedPack = {
                rkey: packData.rkey,
                creator: packData.creator,
                creator_did: packData.creator_did,
                description: packData.description || '',
                name: packData.name,
                previous_update: packData.previous_update || null,
                previous_user_count: packData.previous_user_count || 0,
                total_joins: packData.total_joins || 0,
                updated_at: packData.updated_at || new Date().toISOString(),
                user_count: packData.user_count || 0,
                users: packData.users || [],
                weekly_joins: packData.weekly_joins || 0,
                last_updated: new Date().toISOString(),
                status: packData.status || 'completed',
                status_reason: packData.status_reason || null,
                status_updated_at: packData.status_updated_at || new Date().toISOString()
            };

            // Check if pack exists
            const existingPack = await this.dbManager.db.collection('starter_packs')
                .findOne({ rkey: formattedPack.rkey }, { projection: { _id: 1, created_at: 1 } });

            const updateOperation = {
                filter: { rkey: formattedPack.rkey },
                update: {
                    $set: formattedPack
                }
            };

            // Only add created_at for new documents
            if (!existingPack) {
                updateOperation.update.$setOnInsert = {
                    created_at: packData.created_at || new Date().toISOString()
                };
            } else {
                // For existing documents, keep the original created_at
                formattedPack.created_at = existingPack.created_at;
                updateOperation.update.$set.created_at = existingPack.created_at;
            }

            // 2. writing prepared data

            logger.debug('Writing pack to MongoDB - formatted data:', {
                original: packData,
                formatted: formattedPack,
                isNew: !existingPack
            });

            await this.dbManager.safeWrite('starter_packs', updateOperation, { upsert: true });
    
            // 3. Get existing users first
            const userDids = users.map(u => u.did);
            const existingUsers = await this.dbManager.db.collection('users')
                .find({ did: { $in: userDids } })
                .project({ did: 1, _id: 0 })
                .toArray();
    
            const existingDids = new Set(existingUsers.map(u => u.did));
            logger.debug(`Found ${existingDids.size} existing users out of ${userDids.length} total`);
    
            // 4. Process users in batches
            const dbType = process.env.DB_TYPE || 'cosmos';
            const batchSize = BATCH_SIZES[dbType];
            
            for (let i = 0; i < users.length; i += batchSize) {
                const batch = users.slice(i, i + batchSize);
                
                // Log first user of first batch for verification
                if (i === 0 && batch.length > 0) {
                    const firstUser = batch[0];
                    const formattedUser = {
                        did: firstUser.did,
                        cacheTime: firstUser.cacheTime || Date.now(),
                        displayName: firstUser.displayName || '',
                        followers_count: firstUser.followers_count || 0,
                        follows_count: firstUser.follows_count || 0,
                        handle: firstUser.handle,
                        last_updated: firstUser.last_updated || new Date().toISOString(),
                        pack_ids: [...new Set(firstUser.pack_ids || [])]
                    };
    
                    logger.debug('First user write sample:', {
                        original: firstUser,
                        formatted: formattedUser,
                        isNew: !existingDids.has(firstUser.did)
                    });
                }
    
                // Prepare bulk operations
                const bulkOps = batch.map(user => ({
                    updateOne: {
                        filter: { did: user.did },
                        update: {
                            $set: {
                                did: user.did,
                                cacheTime: user.cacheTime || Date.now(),
                                displayName: user.displayName || '',
                                followers_count: user.followers_count || 0,
                                follows_count: user.follows_count || 0,
                                handle: user.handle,
                                last_updated: user.last_updated || new Date().toISOString(),
                                pack_ids: [...new Set(user.pack_ids || [])]
                            }
                        },
                        upsert: true
                    }
                }));
    
                // Update stats
                batch.forEach(user => {
                    if (existingDids.has(user.did)) {
                        writeResults.updatedUsers++;
                    } else {
                        writeResults.newUsers++;
                    }
                });
    
                await this.dbManager.safeBulkWrite('users', bulkOps);
    
                // Log progress
                logger.info('Batch processing progress:', {
                    batchStart: i,
                    batchSize: batch.length,
                    processed: Math.min(i + batchSize, users.length),
                    total: users.length,
                    newUsers: writeResults.newUsers,
                    updatedUsers: writeResults.updatedUsers
                });
            }
    
            // Final stats
            logger.debug('MongoDB write operation completed:', {
                pack: formattedPack.rkey,
                stats: {
                    usersTotal: users.length,
                    existingUsers: existingDids.size,
                    newUsers: writeResults.newUsers,
                    updatedUsers: writeResults.updatedUsers
                }
            });
    
            return writeResults;
    
        } catch (err) {
            logger.error(`Database write failed for pack ${packData.rkey}:`, {
                error: err.message,
                code: err.code,
                stack: err.stack
            });
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
        
        const cleanupPromises = [];
        
        try {
            // Only add cleanup tasks for initialized components
            if (this.initialized.files && this.fileHandler?.cleanup) {
                cleanupPromises.push(
                    this.fileHandler.cleanup()
                );
            }
    
            if (this.initialized.db && !this.config.noMongoDB && this.mongoClient) {
                cleanupPromises.push(
                    this.mongoClient.close(true)
                );
            }
    
            if (this.initialized.taskManager && this.taskManager?.maybeWriteCheckpoint) {
                cleanupPromises.push(
                    this.taskManager.maybeWriteCheckpoint(true)
                );
            }
    
            if (cleanupPromises.length > 0) {
                await Promise.all(
                    cleanupPromises.map(p => p.catch(err => 
                        logger.error('Error during component cleanup:', err)
                    ))
                );
            }
    
            logger.debug('Cleanup completed successfully');
        } catch (err) {
            logger.error('Error during cleanup:', err);
        }
    }

    clearInternalState() {
        this.activeOperations?.clear();
        this.processingCache?.clear();
        this.profileCache?.clear();
        this.verificationResults?.clear();
        
        // Clear task-related state
        if (this.taskManager) {
            this.taskManager.pendingTasks?.clear();
            this.taskManager.completedTasks?.clear();
            this.taskManager.failures?.clear();
        }
    }
    
}

class RateLimiter {
    constructor() {
        this.requestWindow = 5 * 60 * 1000;  // 5 minutes
        this.maxRequests = 7500;
        this.requests = [];
        this.safetyFactor = 0.95;  // Use 95% of max rate
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
        purge: args.includes('--purge'),
        purgefiles: args.includes('--purgefiles'),
        cleanpackids: args.includes('--cleanpackids'),
        updateAll: args.includes('--updateall'),
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

async function handleShutdown(signal, currentProcessor = null) {
    logger.info(`\nReceived ${signal}. Starting graceful shutdown...`);
    
    try {
        if (currentProcessor) {
            // Save current state
            if (currentProcessor.taskManager) {
                await currentProcessor.taskManager.maybeWriteCheckpoint(true);
            }
            
            // Clean up resources
            await currentProcessor.cleanup();
            
            // Log final stats
            if (currentProcessor.metrics) {
                const stats = currentProcessor.metrics.getMetrics();
                logger.info('Final processing statistics:', {
                    packsProcessed: stats.processing.packsProcessed,
                    usersProcessed: stats.processing.usersProcessed,
                    totalDuration: `${Math.floor((Date.now() - currentProcessor.startTime) / 60000)} minutes`
                });
            }
        }
    } catch (err) {
        logger.error('Error during shutdown:', err);
    } finally {
        process.exit(0);
    }
}

async function handleMaintenanceCommands(args, processor) {

    if (args.cleanFiles) {
        logger.info('Starting file cleanup...');
        try {
            // FileHandler.cleanFiles already handles backups and atomic updates
            await processor.fileHandler.cleanFiles();

            // If MongoDB enabled, sync it with cleaned files
            if (!processor.noMongoDB && !processor.noDBWrites) {
                await processor.dbManager.safeBulkWrite('users', 
                    Array.from(processor.fileHandler.userCache.values()).map(user => ({
                        updateOne: {
                            filter: { did: user.did },
                            update: { $set: user },
                            upsert: true
                        }
                    }))
                );

                await processor.dbManager.safeBulkWrite('starter_packs', 
                    Array.from(processor.fileHandler.packCache.values()).map(pack => ({
                        updateOne: {
                            filter: { rkey: pack.rkey },
                            update: { $set: pack },
                            upsert: true
                        }
                    }))
                );
            }

            logger.info('File cleanup and sync completed');
            return true;
        } catch (err) {
            logger.error('Error during file cleanup:', err);
            throw err;
        }
    }

    if (args.purge) {
        logger.info('Starting purge operation...');
        try {
            // 1. Handle MongoDB if enabled
            if (!processor.noMongoDB && !processor.noDBWrites) {
                // Drop and recreate collections
                await processor.dbManager.db.collection('users').drop().catch(err => {
                    if (err.code !== 26) throw err; // 26 = collection doesn't exist
                });
                await processor.dbManager.db.collection('starter_packs').drop().catch(err => {
                    if (err.code !== 26) throw err;
                });
                
                await processor.dbManager.setupCollections();
                await processor.dbManager.setupIndexes();
                logger.info('MongoDB collections reset');
            }

            // 2. Clear caches
            processor.fileHandler.userCache.clear();
            processor.fileHandler.packCache.clear();
            processor.fileHandler.handleCache.clear();

            // 3. Write empty files (both NDJSON and YAML)
            await fs.writeFile(FILE_PATHS.users, '');
            await fs.writeFile(FILE_PATHS.packs, '');
            await fs.writeFile(FILE_PATHS.usersBackup, '---\n');
            await fs.writeFile(FILE_PATHS.packsBackup, '---\n');

            // 4. Clear checkpoints
            await fs.unlink(FILE_PATHS.checkpoints).catch(() => {});
            await fs.unlink(FILE_PATHS.checkpointsBackup).catch(() => {});

            logger.info('Purge completed successfully');
            return true;
        } catch (err) {
            logger.error('Error during purge:', err);
            throw err;
        }
    }
}

// Quick process functions
async function handleQuickProcess(args, processor, startTime) {

    try {
        // Ensure proper MongoDB initialization and session
        if (!processor.dbManager && !processor.noMongoDB) {
            const dbType = process.env.DB_TYPE || 'cosmos';
            const dbConfig = {
                ...DB_CONFIGS[dbType],
                maxPoolSize: 10,
                minPoolSize: 5,
                waitQueueTimeoutMS: 30000,
                serverSelectionTimeoutMS: 30000
            };

            processor.mongoClient = new MongoClient(process.env.MONGODB_URI, dbConfig);
            processor.dbManager = new DatabaseManager(
                processor.mongoClient,
                dbType,
                processor.logger,
                'starterpacks'
            );
            await processor.dbManager.init(); // This establishes the connection
            await processor.dbManager.ensureSession(); // Explicitly ensure session
        } else if (processor.dbManager) {
            // If dbManager exists, ensure it has a valid session
            await processor.dbManager.ensureSession();
        }

        // Create TaskManager but DON'T load full task list
        if (!processor.taskManager) {
            processor.taskManager = new TaskManager(
                processor.fileHandler, 
                args.debug,
                processor.dbManager
            );
            
            // Only load existing data for lookups, but don't build task list
            const { packs: existingPacks } = await processor.taskManager.loadExistingData();
            processor.taskManager.existingPacks = existingPacks;
            
            // Initialize empty task tracking
            processor.taskManager.pendingTasks = new Map();
            processor.taskManager.completedTasks = new Set();
            processor.taskManager.failures = new Map();
        }
        
        if (args.addUser) {
            logger.debug('Commencing quick user addition');
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
    } catch (err) {
        logger.error(`Error setting up quick processing:`, err);
        throw err;
    }
}

async function quickProcessUser(identifier, options = {}) {
    const {
        processor,
        force = false,
        debug = false,
        maxDepth = process.env.MAX_PACK_DEPTH || 2,
        currentDepth = 0,
        processedDIDs = new Set(),
        parentPack = null
    } = options;

    try {
        if (debug) {
            logger.debug(`Quick processing user: ${identifier} (depth: ${currentDepth}/${maxDepth})`);
        }

        // 1. Resolve identifier (handle or DID)
        let did;
        if (identifier.startsWith('did:')) {
            did = identifier;
        } else {
            try {
                did = await processor.apiHandler.resolveHandle(identifier);
            } catch (err) {
                if (err.status === 404) {
                    // Check historical handles
                    const historicalUser = await processor.fileHandler.getUserByHistoricalHandle(identifier);
                    if (historicalUser) {
                        did = historicalUser.did;
                        logger.info(`Found DID ${did} via historical handle ${identifier}`);
                    } else {
                        throw new Error(`Could not resolve user ${identifier}`);
                    }
                } else {
                    throw err;
                }
            }
        }

        // Skip if we've already processed this profile at this depth or shallower
        if (processedDIDs.has(did)) {
            logger.debug(`Skipping already processed profile: ${did}`);
            return { success: true, skipped: true };
        }

        // 2. Get current profile
        const response = await processor.agent.getProfile({ actor: did });
        if (!response?.data) {
            throw new Error(`Could not find profile for ${did}`);
        }

        // Track that we've processed this profile
        processedDIDs.add(did);

        // 3. Process profile
        const result = await processor.processProfile(response.data, {
            force: true,
            processAssociated: true,
            debug,
            isInitialUser: currentDepth === 0,
            source: 'quick_process',
            parentPack,
            forceProcess: true  // ignore if profile is already in mongodb, enforce
        });

        // 4. Process discovered packs if we haven't hit depth limit
        if (currentDepth < maxDepth && processor.taskManager.pendingTasks.size > 0) {
            logger.debug(`Processing ${processor.taskManager.pendingTasks.size} discovered packs at depth ${currentDepth}`);
            
            while (processor.taskManager.pendingTasks.size > 0) {
                const task = await processor.taskManager.getNextTask();
                if (!task) break;

                // Process the pack
                const success = await processor.processStarterPack(`${task.handle}|${task.rkey}`);
                if (success) {
                    result.changes.packs.processed = (result.changes.packs.processed || 0) + 1;

                    // Get pack members and process them recursively
                    const pack = await processor.fileHandler.getPack(task.rkey);
                    if (pack?.users?.length) {
                        for (const memberDid of pack.users) {
                            // Recursive call for each member
                            await quickProcessUser(memberDid, {
                                ...options,
                                currentDepth: currentDepth + 1,
                                processedDIDs,
                                parentPack: task.rkey
                            });
                        }
                    }
                } else {
                    result.changes.packs.failed = (result.changes.packs.failed || 0) + 1;
                }

                // Mark task as completed and update checkpoint
                await processor.taskManager.markTaskCompleted(task.rkey);
                await processor.taskManager.maybeWriteCheckpoint();
            }
        }

        // Add depth information to result
        result.depth = {
            current: currentDepth,
            max: maxDepth,
            reachedLimit: currentDepth >= maxDepth
        };

        if (debug) {
            logger.debug('Quick process completed:', {
                handle: response.data.handle,
                did: response.data.did,
                depth: result.depth,
                changes: result.changes,
                packsProcessed: result.changes.packs?.processed || 0
            });
        }

        return result;

    } catch (err) {
        logger.error(`Error in quick process user:`, {
            identifier,
            depth: currentDepth,
            error: err.message,
            stack: err.stack
        });
        processor.metrics.recordError('quick_process_user', err);
        throw err;
    }
}

async function quickProcessPack(identifier, options = {}) {
    const { processor, debug = false } = options;

    try {
        if (debug) {
            logger.debug(`Quick processing pack: ${identifier}`);
        }

        // 1. Resolve pack identifier to urlLine format (handle|rkey)
        let urlLine;
        if (identifier.startsWith('at://')) {
            // Handle AT Protocol URI format
            const [_, handle, __, rkey] = identifier.split('/');
            urlLine = `${handle}|${rkey}`;
        } else if (identifier.includes('|')) {
            // Already in correct format
            urlLine = identifier;
        } else {
            // Assume it's an rkey, try to find in existing packs
            const pack = await processor.fileHandler.getPack(identifier);
            if (!pack) {
                throw new Error(`Cannot resolve pack with rkey ${identifier}`);
            }
            urlLine = `${pack.creator}|${identifier}`;
        }

        // 2. Parse into handle and rkey
        const [handle, rkey] = urlLine.split('|');
        if (!handle || !rkey) {
            throw new Error(`Invalid pack identifier format: ${identifier}`);
        }

        // 3. Add as single initial task
        await processor.taskManager.addTask({
            handle,
            rkey,
            priority: 1,
            source: 'quick_process'
        });

        // 4. Process this pack and any discovered related tasks
        const processed = new Set();
        while (processor.taskManager.pendingTasks.size > 0) {
            const success = await processor.taskManager.processNextTask(processor);
            if (success) {
                processed.add(rkey);
                processor.taskManager.markTaskCompleted(rkey);
            }
            await processor.taskManager.maybeWriteCheckpoint();
        }

        if (debug) {
            logger.debug('Pack processing completed:', {
                identifier,
                processed: Array.from(processed)
            });
        }

        return true;

    } catch (err) {
        logger.error(`Error processing pack ${identifier}:`, {
            error: err.message,
            stack: err.stack
        });
        processor.metrics.recordError('quick_process_pack', err);
        throw err;
    }
}

class TaskManager {
    constructor(fileHandler, debug = false, dbManager = null, noMongoDB = false) {
        // Dependencies
        this.fileHandler = fileHandler;
        this.debug = debug;
        this.dbManager = dbManager;
        this.noMongoDB = noMongoDB;

        // Core task tracking - using Maps/Sets for easy size tracking
        this.pendingTasks = new Map();
        this.completedTasks = new Set();
        this.failures = new Map();
        this.packStates = new Map();
        this.discoveredPacksMap = new Map();
        this.packRelationships = new Map();
        this.missingProfiles = new Set();

        this.knownStarterPacks = new Set(); // what is already in mongodb?
        this.knownUsers = new Set();        // what is already in mongodb?
        this.updateAll = false;             // Will be set based on --updateall arg

        // Discovery tracking (only discovery-specific flags)
        this.processingDiscoveredTasks = false;
        this.discoveryDepth = 0;

        // Checkpoint management
        this.lastCheckpoint = Date.now();
        this.CHECKPOINT_INTERVAL = 20 * 60 * 1000;
        this.checkpointDirty = false;

        this.metrics = metrics;
        this.stats = {
            byPriority: new Map(),  // Map of priority -> {total, completed, failed}
            total: 0,
            completed: 0,
            failed: 0,
            notFound: 0,    // 404s
            apiErrors: 0,   // Other API errors
            current: {
                priority: null,
                position: 0
            }
        };

        // Validation
        if (!this.noMongoDB && !dbManager) {
            throw new Error('Database manager is required when MongoDB is enabled');
        }
    }

    async initializeAllData(updateAll = false) {
        this.updateAll = updateAll;
        
        // 1. Get known data from MongoDB first (for prioritization)
        if (!this.noMongoDB && this.dbManager) {
            logger.info('Loading known data from MongoDB...');
            this.knownStarterPacks = await this.dbManager.getKnownStarterPackRkeys();
            this.knownUsers = await this.dbManager.getKnownUserDIDs();
            logger.info(`Loaded ${this.knownStarterPacks.size} packs and ${this.knownUsers.size} users from MongoDB`);
        }

        // 2. Load local file data (with fallback)
        logger.info('Loading local file data...');
        const { packs: existingPacks, users: existingUsers } = 
            await this.loadExistingDataWithFallback();
        
        // Merge knowledge (local files might have data not yet in MongoDB)
        for (const pack of existingPacks.values()) {
            this.knownStarterPacks.add(pack.rkey);
        }
        for (const user of existingUsers.values()) {
            this.knownUsers.add(user.did);
        }

        // Store full data for quick access
        this.existingPacks = existingPacks;
        this.existingUsers = existingUsers;

        // 3. Load checkpoint last (but don't trust it completely)
        try {
            logger.info('Loading checkpoint data...');
            const checkpoint = await this.loadCheckpoint();
            
            // Use checkpoint data only for tasks we know exist
            this.completedTasks = new Set(
                Array.from(checkpoint.completedPacks || [])
                    .filter(rkey => this.knownStarterPacks.has(rkey))
            );
            
            // Only keep valid failure records
            this.failures = new Map(
                Array.from(checkpoint.failures || [])
                    .filter(([rkey]) => this.knownStarterPacks.has(rkey))
            );
            
            logger.info(`Loaded ${this.completedTasks.size} completed tasks from checkpoint`);
        } catch (err) {
            logger.warn('Checkpoint load failed, starting fresh:', err);
            this.completedTasks = new Set();
            this.failures = new Map();
        }

        logger.info('Data initialization complete:', {
            knownPacks: this.knownStarterPacks.size,
            knownUsers: this.knownUsers.size,
            localPacks: existingPacks.size,
            localUsers: existingUsers.size,
            completedTasks: this.completedTasks.size,
            failures: this.failures.size,
            mode: updateAll ? 'update all' : 'new data first'
        });
    }

    updatePackState(rkey, status, reason) {
        this.packStates.set(rkey, {
            status,
            reason,
            timestamp: new Date().toISOString()
        });
        this.markDirty();
    }
        
    async initializeWithKnownData(updateAll = false) {
        this.updateAll = updateAll;
        
        if (!this.noMongoDB && this.dbManager) {
            logger.info('Loading known data from MongoDB...');
            try {
                this.knownStarterPacks = await this.dbManager.getKnownStarterPackRkeys();
                this.knownUsers = await this.dbManager.getKnownUserDIDs();
                
                logger.info('Loaded known data from MongoDB:', {
                    starterPacks: this.knownStarterPacks.size,
                    users: this.knownUsers.size,
                    mode: updateAll ? 'update all' : 'new data first'
                });
            } catch (err) {
                logger.error('Error loading MongoDB data:', err);
                this.knownStarterPacks = new Set();
                this.knownUsers = new Set();
            }
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
            let position = 0;
            for (const line of content.split('\n').filter(Boolean)) {
                const [handle, rkey] = line.split('|').map(s => s.trim());
                if (handle && rkey) {
                    urls.set(rkey, { handle, rkey });
                    this.originalOrder.set(rkey, position++);
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

    shouldProcessUser(did) {
        // Skip known users unless updateAll is true
        if (!this.updateAll && this.knownUsers.has(did)) {
            return false;
        }
        return true;
    }

    async shouldProcessPack(rkey, existingPack, failure, options = {}) {
        const { forceProcess = false } = options;

        // For quick process, bypass recency checks
        if (forceProcess) {
            return { process: true };
        }

        // First check permanent failures
        if (failure?.permanent) {
            return { process: false, reason: 'permanent_failure' };
        }

        try {
            // Check if we have valid MongoDB access
            if (this.dbManager && 
                !this.dbManager.noDBWrites && 
                this.dbManager.db) {
                
                try {
                    // Add timeout to MongoDB operation
                    const mongoPromise = this.dbManager.db.collection('starter_packs')
                        .findOne({ rkey: rkey }, { session: this.dbManager.session });
                    
                    // Set timeout for operation
                    const timeoutPromise = new Promise((_, reject) => 
                        setTimeout(() => reject(new Error('MongoDB operation timed out')), 5000));

                    // Race between MongoDB operation and timeout
                    const mongoDbPack = await Promise.race([mongoPromise, timeoutPromise])
                        .catch(err => {
                            logger.debug(`MongoDB check timed out for pack ${rkey}, proceeding with file check`);
                            return null;
                        });
                    
                    if (mongoDbPack) {
                        const lastUpdate = new Date(mongoDbPack.updated_at);
                        const daysSinceUpdate = (Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60 * 24);
                        
                        if (daysSinceUpdate < 10) {
                            return { process: false, reason: 'recently_processed_in_mongodb' };
                        }
                        
                        return { 
                            process: true, 
                            lowPriority: true,
                            reason: 'mongodb_needs_update' 
                        };
                    }
                } catch (err) {
                    // Log at debug level since this is expected sometimes
                    logger.debug(`MongoDB check skipped for pack ${rkey}, falling back to file check`);
                }
            }

            // If no MongoDB or MongoDB check failed, fall back to file check
            if (existingPack) {
                const lastUpdate = new Date(existingPack.updated_at);
                const daysSinceUpdate = (Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60 * 24);
                
                if (daysSinceUpdate < 7 && !failure) {
                    return { process: false, reason: 'recently_updated_in_files' };
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
                
        } catch (err) {
            // Log at debug level and fall back to processing
            logger.debug(`Error in shouldProcessPack for ${rkey}, defaulting to process: ${err.message}`);
            return { process: true };
        }
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

            // Clear all internal collections
            this.pendingTasks.clear();
            this.completedTasks.clear();
            this.failures.clear();
            this.missingProfiles.clear();
        
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
            
            // Load failures with their state
            this.failures.clear();
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

            // Load pack states
            this.packStates = new Map(checkpoint.packStates || []);
            
            // Load original order
            this.originalOrder = new Map(checkpoint.originalOrder || []);
            
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

    calculateTaskPriority(rkey, options = {}) {
        const {
            source = 'initial',
            isNewlyDiscovered = false,
            existingPack = null,
            lastUpdate = null,
            failure = null
        } = options;
    
        let priority = 0;
    
        // Base priority by known status
        const inMongoDB = this.knownStarterPacks.has(rkey);
        const inFiles = !!existingPack;
    
        if (!inMongoDB && !inFiles) {
            priority = 15;  // Completely new
        } else {
            // Calculate days since last update
            const lastUpdateDate = lastUpdate || 
                (existingPack?.updated_at ? new Date(existingPack.updated_at) : null);
            
            if (lastUpdateDate) {
                const daysSinceUpdate = (Date.now() - lastUpdateDate.getTime()) / (1000 * 60 * 60 * 24);
                
                if (daysSinceUpdate > 14) priority = 10;
                else if (!inMongoDB && inFiles) priority = 8;  // Needs MongoDB sync
                else if (daysSinceUpdate > 7) priority = 5;
                else if (daysSinceUpdate > 3) priority = 3;
                else priority = 1;
            } else {
                priority = 10;  // No update date - treat as old
            }
        }
    
        // Source/discovery adjustments
        if (source === 'associated' || isNewlyDiscovered) {
            priority += 5;
        }
    
        // Penalty for failures
        if (failure) {
            priority = Math.max(1, priority - (failure.attempts * 2));
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

    async markPackStatus(rkey, status, reason) {
        const timestamp = new Date().toISOString();
    
        try {
            // 1. Update internal state
            switch(status) {
                case 'deleted':
                case 'hidden':
                case 'inactive':
                    this.failures.set(rkey, {
                        status,
                        reason,
                        timestamp,
                        attempts: (this.failures.get(rkey)?.attempts || 0) + 1,
                        permanent: status === 'deleted'
                    });
                    this.pendingTasks.delete(rkey);
                    break;
    
                case 'completed':
                    this.completedTasks.add(rkey);
                    this.pendingTasks.delete(rkey);
                    this.failures.delete(rkey);
                    break;
    
                case 'failed':
                    const failure = this.failures.get(rkey) || { attempts: 0 };
                    failure.attempts++;
                    failure.timestamp = timestamp;
                    failure.reason = reason;
                    failure.permanent = failure.attempts >= 3;
                    this.failures.set(rkey, failure);
                    break;
            }
    
            // 2. Update pack state tracking
            this.packStates.set(rkey, {
                status,
                reason,
                timestamp
            });
    
            // 3. Record metrics
            if (this.metrics) {
                this.metrics.recordPackProcessing(
                    status === 'completed',
                    Date.now() - (this.pendingTasks.get(rkey)?.addedAt || Date.now())
                );
            }
    
            // 4. Update database if needed
            if (this.dbManager && !this.noMongoDB && !this.dbManager.noDBWrites) {
                if (status === 'deleted') {
                    await this.dbManager.markPackDeleted(rkey, reason);
                } else {
                    await this.dbManager.markPackStatus(rkey, status, reason);
                }
            }
    
            this.markDirty();
    
        } catch (err) {
            logger.error(`Error marking pack status ${rkey} -> ${status}:`, err);
            throw err;
        }
    }

    markDirty() {
        this.checkpointDirty = true;
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
            this.discoveredPacksMap.clear();
            this.totalTaskCount = 0;  // Add total counter
    
            // Restore completed tasks
            if (checkpoint.completed) {
                for (const rkey of checkpoint.completed) {
                    this.completedTasks.add(rkey);
                }
            }
    
            // Restore failures with their state
            if (checkpoint.failures) {
                for (const {rkey, reason, attempts, timestamp} of checkpoint.failures) {
                    this.failures.set(rkey, {
                        reason,
                        attempts: attempts || 1,
                        lastAttempt: timestamp,
                        permanent: attempts >= 3
                    });
                }
            }
    
            // Restore discovered packs with their metadata
            if (checkpoint.discovered) {
                for (const [rkey, data] of checkpoint.discovered) {
                    this.discoveredPacksMap.set(rkey, {
                        ...data,
                        discoveredAt: data.discoveredAt || data.timestamp || new Date().toISOString(),
                        priority: data.priority || 10  // Default high priority for discovered
                    });
                }
            }
    
            // Restore progress counters if available
            if (checkpoint.progress) {
                this.totalTaskCount = 
                    (checkpoint.progress.total || 0) + 
                    (checkpoint.progress.discovered || 0);
            }
    
            // Log restored state
            logger.info('Restored processing state:', {
                completed: this.completedTasks.size,
                failed: this.failures.size,
                discovered: this.discoveredPacksMap.size,
                total: this.totalTaskCount,
                progress: `${this.completedTasks.size}/${this.totalTaskCount} (${
                    ((this.completedTasks.size / this.totalTaskCount) * 100).toFixed(1)
                }%)`
            });
    
        } catch (err) {
            if (err.code !== 'ENOENT') {
                logger.warn('Error loading checkpoint, starting fresh:', err);
            }
            await this.createInitialCheckpoint();
        }
    }

    async createInitialCheckpoint() {
        const initialState = {
            version: "1.0",
            timestamp: new Date().toISOString(),
            completed: [],
            failures: [],
            discovered: [],
            progress: {
                total: 0,
                completed: 0,
                discovered: 0
            }
        };
        
        await this.writeCheckpoint(initialState);
        this.totalTaskCount = 0;
    }

    async writeCheckpoint(forceWrite = false) {
        if (!forceWrite && !this.checkpointDirty) {
            return;
        }
    
        const checkpoint = {
            version: "1.0",
            timestamp: new Date().toISOString(),
            completed: Array.from(this.completedTasks),
            failures: Array.from(this.failures.entries()).map(([rkey, data]) => ({
                rkey,
                reason: data.reason,
                attempts: data.attempts,
                timestamp: data.lastAttempt
            })),
            discovered: Array.from(this.discoveredPacksMap.entries()),
            progress: {
                total: this.totalTaskCount,
                completed: this.completedTasks.size,
                discovered: this.discoveredPacksMap.size,
                remaining: this.pendingTasks.size
            }
        };
    
        const tempPath = FILE_PATHS.checkpointsBackup;
        try {
            await fs.writeFile(tempPath, JSON.stringify(checkpoint, null, 2));
            await fs.rename(tempPath, FILE_PATHS.checkpoints);
            this.checkpointDirty = false;
    
            logger.debug('Checkpoint written:', {
                completed: checkpoint.progress.completed,
                total: checkpoint.progress.total,
                discovered: checkpoint.progress.discovered
            });
        } catch (err) {
            logger.error('Failed to write checkpoint:', err);
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

    async addTask({ 
        handle, 
        rkey, 
        source = 'initial', 
        parentDid = null, 
        memberCount = 0,
        discoveredAt = null 
    }) {
        // Skip if already completed or permanently failed
        if (this.completedTasks.has(rkey)) {
            logger.debug(`Skipping already completed task: ${rkey}`);
            return false;
        }
        
        const failure = this.failures.get(rkey);
        if (failure?.permanent) {
            logger.debug(`Skipping permanently failed task: ${rkey}`);
            return false;
        }
    
        const existingPack = await this.fileHandler.getPack(rkey);
        const priority = this.calculateTaskPriority(rkey, {
            source,
            isNewlyDiscovered: !!discoveredAt,
            memberCount,
            existingPack,
            failure
        });
    
        this.pendingTasks.set(rkey, {
            handle,
            rkey,
            priority,
            source,
            parentDid,
            memberCount,
            discoveredAt: discoveredAt || new Date().toISOString(),
            addedAt: new Date().toISOString(),
            attempts: failure?.attempts || 0
        });

        // Update total count for new tasks
        if (source === 'initial' || !this.totalTaskCount) {
            this.totalTaskCount++;
        }
    
        logger.debug(`Added task ${rkey}:`, {
            source,
            priority,
            pending: this.pendingTasks.size,
            completed: this.completedTasks.size,
            discovered: this.discoveredPacksMap.size
        });
    
        this.markDirty();
        return true;
    }
    
    async addAssociatedPack(packInfo, parentDid) {
        const { rkey, creator: handle, memberCount } = packInfo;
        
        if (!this.discoveredPacksMap.has(rkey)) {
            this.discoveredPacksMap.set(rkey, {
                discoveredAt: new Date().toISOString(),
                discoveredFrom: parentDid,
                memberCount
            });
    
            const existingPack = await this.fileHandler.getPack(rkey);
            const failure = this.failures.get(rkey);
            
            const priority = this.calculateTaskPriority(rkey, {
                source: 'associated',
                isNewlyDiscovered: true,
                existingPack,
                failure,
                lastUpdate: existingPack?.updated_at
            });
    
            this.pendingTasks.set(rkey, {
                handle,
                rkey,
                priority,
                source: 'associated',
                parentDid,
                memberCount,
                inMongoDB: this.knownStarterPacks.has(rkey),
                inFiles: !!existingPack,
                lastUpdate: existingPack?.updated_at,
                discoveredAt: new Date().toISOString(),
                addedAt: new Date().toISOString(),
                attempts: failure?.attempts || 0
            });
    
            this.recordPackRelationship(rkey, parentDid);
            this.markDirty();
            
            logger.debug(`Added associated pack ${rkey}:`, {
                from: parentDid,
                priority,
                pending: this.pendingTasks.size
            });
            
            return true;
        }
    
        return false;
    }

    async getNextTask() {
        let tasks = Array.from(this.pendingTasks.values());
        tasks.sort((a, b) => {
            // Recalculate current priorities
            const priorityA = this.calculateTaskPriority(a.rkey, {
                source: a.source,
                isNewlyDiscovered: this.discoveredPacksMap.has(a.rkey),
                memberCount: a.memberCount,
                existingPack: this.fileHandler.getPack(a.rkey),
                failure: this.failures.get(a.rkey)
            });

            const priorityB = this.calculateTaskPriority(b.rkey, {
                source: b.source,
                isNewlyDiscovered: this.discoveredPacksMap.has(b.rkey),
                memberCount: b.memberCount,
                existingPack: this.fileHandler.getPack(b.rkey),
                failure: this.failures.get(b.rkey)
            });

            return priorityB - priorityA;
        });

        return tasks[0];
    }
    
    async addAssociatedPack(packInfo, parentDid) {
        const { rkey, creator: handle, memberCount } = packInfo;
        
        // Only process if not already discovered
        if (!this.discoveredPacksMap.has(rkey)) {
            // Add to discovered map first
            this.discoveredPacksMap.set(rkey, {
                discoveredAt: new Date().toISOString(),
                discoveredFrom: parentDid,
                memberCount
            });
    
            // Add to tasks
            const added = await this.addTask({
                handle,
                rkey,
                source: 'associated',
                parentDid,
                memberCount,
                discoveredAt: new Date().toISOString()
            });
    
            if (added) {
                this.recordPackRelationship(rkey, parentDid);
                this.markDirty();
                
                logger.debug(`Added associated pack ${rkey}:`, {
                    from: parentDid,
                    pending: this.pendingTasks.size,
                    discovered: this.discoveredPacksMap.size
                });
                
                return true;
            }
        }
    
        return false;
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

        // Update current position tracking
        if (task.priority !== this.stats.current.priority) {
            this.stats.current.priority = task.priority;
            this.stats.current.position = 0;
        }
        this.stats.current.position++;

        // Log progress with detailed stats
        logger.info('Processing status:', {
            currentPriority: this.stats.current.priority,
            position: `${this.stats.current.position}/${this.stats.byPriority.get(task.priority)?.total || 0}`,
            overall: `${this.stats.completed}/${this.stats.total}`,
            success: `${((this.stats.completed / (this.stats.completed + this.stats.failed)) * 100).toFixed(1)}%`,
            notFound: this.stats.notFound,
            apiErrors: this.stats.apiErrors
        });

        try {
            const success = await processor.processStarterPack(`${task.handle}|${task.rkey}`);
            
            if (success) {
                this.stats.completed++;
                const priorityStats = this.stats.byPriority.get(task.priority);
                if (priorityStats) {
                    priorityStats.completed++;
                }
            }

            return success;
        } catch (err) {
            logger.error(`Error processing task ${task.rkey}:`, err);
            return false;
        }
    }

    hasMoreWork() {
        return this.pendingTasks.size > 0 || 
               this.discoveredTasksQueue.size > 0 || 
               this.processingDiscoveredTasks;
    }

    recordNewlyDiscoveredTask() {
        this.newlyDiscoveredTasks = (this.newlyDiscoveredTasks || 0) + 1;
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

    async loadKnownData() {
        if (!this.noMongoDB && this.dbManager) {
            logger.info('Loading known data from MongoDB...');
            try {
                this.knownStarterPacks = await this.dbManager.getKnownStarterPackRkeys();
                this.knownUsers = await this.dbManager.getKnownUserDIDs();
                
                logger.info('Loaded known data from MongoDB:', {
                    starterPacks: this.knownStarterPacks.size,
                    users: this.knownUsers.size
                });
            } catch (err) {
                logger.error('Error loading MongoDB data:', err);
                this.knownStarterPacks = new Set();
                this.knownUsers = new Set();
            }
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

        // Update stats
        this.stats.failed++;
        if (reason.includes('not found') || reason.includes('404')) {
            this.stats.notFound++;
        } else if (reason.includes('API')) {
            this.stats.apiErrors++;
        }

        const task = this.pendingTasks.get(rkey);
        if (task) {
            const priorityStats = this.stats.byPriority.get(task.priority);
            if (priorityStats) {
                priorityStats.failed++;
            }
        }
    }

    isRecentlyProcessed(timestamp, daysThreshold = 10) {
        const lastUpdate = new Date(timestamp);
        const daysSinceUpdate = (Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60 * 24);
        return daysSinceUpdate < daysThreshold;
    }

    async buildTaskList(urlsContent) {
        logger.debug("buildTaskList starting...");
        const tasks = [];
        const skipped = [];
        
        for (const line of urlsContent.split('\n').filter(Boolean)) {
            const [handle, rkey] = line.split('|').map(s => s.trim());
            if (!handle || !rkey) continue;
    
            // Get all known states
            const inMongoDB = this.knownStarterPacks.has(rkey);
            const existingPack = await this.fileHandler.getPack(rkey);
            const failure = this.failures.get(rkey);
    
            // Skip permanent failures
            if (failure?.permanent) {
                skipped.push({ rkey, reason: 'permanent_failure' });
                continue;
            }
    
            // Calculate priority
            const priority = this.calculateTaskPriority(rkey, {
                existingPack,
                failure,
                lastUpdate: existingPack?.updated_at
            });
    
            // Add to tasks with metadata
            tasks.push({
                handle,
                rkey,
                priority,
                source: 'initial',
                lastUpdate: existingPack?.updated_at,
                inMongoDB,
                inFiles: !!existingPack,
                attempts: failure?.attempts || 0,
                addedAt: new Date().toISOString()
            });
        }
    
        // Sort by priority (high to low)
        tasks.sort((a, b) => b.priority - a.priority);
    
        logger.debug('Task list built:', {
            total: tasks.length,
            priorities: tasks.reduce((acc, t) => {
                acc[t.priority] = (acc[t.priority] || 0) + 1;
                return acc;
            }, {}),
            inMongoDB: tasks.filter(t => t.inMongoDB).length,
            inFiles: tasks.filter(t => t.inFiles).length,
            attempts: tasks.filter(t => t.attempts > 0).length
        });

        await this.initializeStats(tasks);
    
        return { tasks, skipped };
    }

    async initializeStats(tasks) {
        this.stats = {
            byPriority: new Map(),
            total: tasks.length,
            completed: 0,
            failed: 0,
            notFound: 0,
            apiErrors: 0,
            current: {
                priority: null,
                position: 0
            }
        };

        // Group by priority
        for (const task of tasks) {
            if (!this.stats.byPriority.has(task.priority)) {
                this.stats.byPriority.set(task.priority, {
                    total: 0,
                    completed: 0,
                    failed: 0
                });
            }
            this.stats.byPriority.get(task.priority).total++;
        }

        // Log initial stats
        logger.info('Task distribution:', {
            total: this.stats.total,
            byPriority: Object.fromEntries(
                Array.from(this.stats.byPriority.entries())
                    .sort((a, b) => b[0] - a[0])  // Sort by priority desc
                    .map(([priority, stats]) => [priority, stats.total])
            )
        });
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

}

async function handlePurgeFiles(context) {
    const { logger, FILE_PATHS } = context;
    logger.info('Starting file purge operation...');
    const startTime = Date.now();

    try {
        // Create backup timestamp
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        
        // Create backups
        for (const file of [FILE_PATHS.users, FILE_PATHS.packs]) {
            try {
                await fs.copyFile(file, `${file}.${timestamp}.bak`);
                logger.info(`Backed up ${file}`);
            } catch (err) {
                logger.warn(`Could not backup ${file}:`, err);
            }
        }

        // Process files one at a time using streams
        async function processFileInChunks(filePath, type) {
            logger.info(`Processing ${filePath}...`);
            
            const dataMap = new Map();
            let processedCount = 0;
            let skippedCount = 0;
            let chunkSize = 0;
            let buffer = '';
        
            // Use createReadStream from the imported version
            const readStream = createReadStream(filePath, {
                encoding: 'utf8',
                highWaterMark: 1024 * 1024 // 1MB chunks
            });

            for await (const chunk of readStream) {
                buffer += chunk;
                chunkSize += chunk.length;

                // Process complete lines
                let newlineIndex;
                while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
                    const line = buffer.slice(0, newlineIndex);
                    buffer = buffer.slice(newlineIndex + 1);

                    if (!line.trim()) continue;

                    try {
                        const item = JSON.parse(line);
                        const id = type === 'users' ? item.did : item.rkey;
                        const timestamp = type === 'users' ? item.last_updated : item.updated_at;

                        if (!id || !timestamp) {
                            logger.warn(`Invalid ${type} entry, missing id or timestamp`);
                            continue;
                        }

                        const existing = dataMap.get(id);
                        if (!existing || new Date(timestamp) > new Date(existing.timestamp)) {
                            dataMap.set(id, {
                                data: item,
                                timestamp: timestamp
                            });
                            processedCount++;
                        } else {
                            skippedCount++;
                        }

                        // Log progress every 10k items
                        if ((processedCount + skippedCount) % 10000 === 0) {
                            logger.info(`${type} processing progress:`, {
                                processed: processedCount,
                                skipped: skippedCount,
                                unique: dataMap.size,
                                memoryUsage: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB'
                            });
                        }
                    } catch (err) {
                        logger.warn(`Error processing ${type} line:`, err);
                    }
                }
            }

            // Write processed data back to file
            logger.info(`Writing processed ${type} back to file...`);
            const writeStream = createWriteStream(filePath);
            
            for (const { data } of dataMap.values()) {
                writeStream.write(JSON.stringify(data) + '\n');
            }

            await new Promise((resolve, reject) => {
                writeStream.end(err => {
                    if (err) reject(err);
                    else resolve();
                });
            });

            // Write YAML backup (in chunks to handle large datasets)
            const yamlPath = type === 'users' ? FILE_PATHS.usersBackup : FILE_PATHS.packsBackup;
            const yamlWriteStream = createWriteStream(yamlPath);
            
            let count = 0;
            for (const { data } of dataMap.values()) {
                yamlWriteStream.write('---\n');
                yamlWriteStream.write(yaml.dump(data));
                
                count++;
                if (count % 1000 === 0) {
                    // Allow event loop to process
                    await new Promise(resolve => setTimeout(resolve, 0));
                }
            }

            await new Promise((resolve, reject) => {
                yamlWriteStream.end(err => {
                    if (err) reject(err);
                    else resolve();
                });
            });

            return {
                processed: processedCount,
                skipped: skippedCount,
                unique: dataMap.size
            };
        }

        // Process files sequentially
        const usersStats = await processFileInChunks(FILE_PATHS.users, 'users');
        const packsStats = await processFileInChunks(FILE_PATHS.packs, 'packs');

        const duration = (Date.now() - startTime) / 1000;
        logger.info('File purge completed:', {
            duration: `${duration.toFixed(1)} seconds`,
            users: {
                processed: usersStats.processed,
                skipped: usersStats.skipped,
                unique: usersStats.unique,
                reductionPercent: ((usersStats.skipped / (usersStats.processed + usersStats.skipped)) * 100).toFixed(1)
            },
            packs: {
                processed: packsStats.processed,
                skipped: packsStats.skipped,
                unique: packsStats.unique,
                reductionPercent: ((packsStats.skipped / (packsStats.processed + packsStats.skipped)) * 100).toFixed(1)
            }
        });

    } catch (err) {
        logger.error('Error during file purge:', err);
        throw err;
    }
}

// main function
async function main() {
    const args = parseArgs();
    const debug = args.debug;
    logger.level = debug ? 'debug' : 'info';

    logger.debug('CLI args:', args);
    const startTime = Date.now();

    let processor;
    try {
        metrics.recordStartup();

        // 1. Handle standalone modes first (no processor needed)
        if (args.purgefiles) {
            await handlePurgeFiles({
                debug: args.debug,
                logger,
                FILE_PATHS
            });
            return;
        }

        // 2. Validate environment for all other modes
        validateEnv(args);

        // 3. Create processor with appropriate mode
        const mode = determineProcessingMode(args);
        processor = await MainProcessor.create({
            noMongoDB: args.noMongoDB,
            noDBWrites: args.noDBWrites,
            fromApi: args.fromApi,
            debug: args.debug || process.env.DEBUG,
            mode
        });

        // 4. Handle different processing modes
        switch (mode) {
            case 'cleanup':
                await processor.dbManager.performPackIdsCleanup();
                return;
        
            case 'quick':
                const result = await handleQuickProcess(args, processor);
                return result;
        
            case 'normal':
                // Set up shutdown handlers early
                process.on('SIGINT', () => handleShutdown('SIGINT', processor));
                process.on('SIGTERM', () => handleShutdown('SIGTERM', processor));
            
                // Initialize in correct order
                logger.info('Starting normal processing...');
                
                // 1. Load MongoDB data (fast operation)
                if (!args.noMongoDB) {
                    await processor.taskManager.loadKnownData();
                }
            
                // 2. Load URLs list (small file)
                const urlsContent = await fs.readFile(FILE_PATHS.urls, 'utf8');
                
                // 3. Build initial task list
                const { tasks, skipped } = await processor.taskManager.buildTaskList(urlsContent);
            
                // Log detailed initial state
                logger.info('Processing setup complete:', {
                    totalTasks: tasks.length,
                    skipped: skipped.length,
                    byPriority: tasks.reduce((acc, t) => {
                        acc[t.priority] = (acc[t.priority] || 0) + 1;
                        return acc;
                    }, {}),
                    mode: args.updateAll ? 'update all' : 'new data first'
                });
            
                // 4. Process tasks
                let stats = await processor.processTasks(tasks);
            
                break;
        }

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

// Helper functions
function determineProcessingMode(args) {
    if (args.cleanpackids) return 'cleanup';
    if (args.addUser || args.addPack) return 'quick';
    if (args.purge || args.cleanFiles) return 'maintenance';
    return 'normal';
}

function logFinalStats(stats, startTime) {
    const duration = Math.floor((Date.now() - startTime) / 60000);
    const { processedCount, failedCount } = stats;
    
    logger.info('Processing completed:', {
        duration: `${duration} minutes`,
        processed: processedCount,
        failed: failedCount,
        successRate: `${((processedCount / (processedCount + failedCount)) * 100).toFixed(1)}%`,
        averageRate: `${(processedCount / duration).toFixed(1)} packs/minute`
    });

    logger.debug('Final metrics:', metrics.getMetrics());
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

// Run the program
main().catch(err => {
    logger.error('Fatal error in main:', err);
    process.exit(1);
});
