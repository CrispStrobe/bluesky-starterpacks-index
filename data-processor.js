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
