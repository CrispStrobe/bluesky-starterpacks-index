#!/usr/bin/env node
import { MongoClient } from 'mongodb';
import fs from 'fs';
import yaml from 'js-yaml';
import chalk from 'chalk';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import * as dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

dotenv.config();

// CLI arguments setup
const argv = yargs(hideBin(process.argv))
    .usage('Usage: $0 [options] <search term>')
    .option('source', {
        alias: 's',
        describe: 'Data source to search (mongodb, json, yaml)',
        choices: ['mongodb', 'json', 'yaml'],
        default: 'mongodb'
    })
    .option('file', {
        alias: 'f',
        describe: 'Input file path for JSON/YAML',
        type: 'string',
        default: ''
    })
    .option('exact', {
        alias: 'e',
        describe: 'Perform exact match instead of partial match',
        type: 'boolean',
        default: false
    })
    .option('case-sensitive', {
        alias: 'c',
        describe: 'Make search case-sensitive',
        type: 'boolean',
        default: false
    })
    .option('packs', {
        describe: 'Show results organized by packs (default)',
        type: 'boolean',
        default: false
    })
    .option('users', {
        describe: 'Show results organized by users',
        type: 'boolean',
        default: false
    })
    .demandCommand(1, 'Please provide a search term')
    .help()
    .argv;

async function searchMongoDB(searchTerm, isExact, isCaseSensitive) {
    const client = new MongoClient(process.env.MONGODB_URI);
    try {
        await client.connect();
        const db = client.db('starterpacks');

        let searchRegex;
        if (isExact) {
            searchRegex = `^${searchTerm}$`;
        } else {
            searchRegex = searchTerm;
        }
        const regexOptions = isCaseSensitive ? '' : 'i';

        // Find matching users
        const userQuery = {
            $or: [
                { handle: { $regex: searchRegex, $options: regexOptions } },
                { display_name: { $regex: searchRegex, $options: regexOptions } }
            ]
        };

        const matchingUsers = await db.collection('users').find(userQuery).toArray();
        console.log(`Found ${matchingUsers.length} matching users`);

        // Clean pack_ids by filtering out $each objects and flattening
        const packIds = [...new Set(matchingUsers.flatMap(user => 
            (user.pack_ids || []).filter(id => 
                typeof id === 'string' && !id.startsWith('$')
            )
        ))];

        console.log(`Found ${packIds.length} unique pack IDs from users:`, packIds);

        // Get all users from matching packs if we have any pack IDs
        const allPackUsers = packIds.length > 0 ?
            await db.collection('users').find({ 
                pack_ids: { 
                    $in: packIds  // Now packIds only contains valid strings
                } 
            }).toArray() : [];

        // Group users by pack ID
        const usersByPack = new Map();
        allPackUsers.forEach(user => {
            user.pack_ids.forEach(packId => {
                if (!usersByPack.has(packId)) {
                    usersByPack.set(packId, []);
                }
                usersByPack.get(packId).push(user);
            });
        });

        // Create pack information with accurate user counts
        const packs = await Promise.all(packIds.map(async packId => {
            // Try to find pack in MongoDB first
            const pack = await db.collection('starter_packs').findOne({ rkey: packId });

            // Get all users for this pack
            const packUsers = usersByPack.get(packId) || [];
            const matchingPackUsers = matchingUsers.filter(u => u.pack_ids.includes(packId));

            // If we found the pack in MongoDB, use that info
            if (pack) {
                return {
                    rkey: packId,
                    name: pack.name,
                    creator: pack.creator,
                    description: pack.description || '',
                    users: packUsers.map(u => ({
                        did: u.did,
                        handle: u.handle,
                        display_name: u.display_name,
                        pack_ids: u.pack_ids
                    }))
                };
            }

            // If not found in MongoDB, construct from user data
            // Try to find a matching user's document that might have pack metadata
            const usersWithMetadata = await db.collection('users').find({
                pack_ids: packId,
                $or: [
                    { 'pack_metadata.rkey': packId }
                ]
            }).toArray();

            let packMetadata = usersWithMetadata.find(u => u.pack_metadata?.rkey === packId)?.pack_metadata;

            return {
                rkey: packId,
                name: packMetadata?.name || `Pack ${packId}`,
                creator: packMetadata?.creator || "Unknown",
                description: packMetadata?.description || '',
                users: packUsers.map(u => ({
                    did: u.did,
                    handle: u.handle,
                    display_name: u.display_name,
                    pack_ids: u.pack_ids
                }))
            };
        }));

        // Also search for packs directly by name/description
        const directPackQuery = {
            $or: [
                { name: { $regex: searchRegex, $options: regexOptions } },
                { description: { $regex: searchRegex, $options: regexOptions } }
            ]
        };

        const directMatchingPacks = await db.collection('starter_packs')
            .find(directPackQuery)
            .toArray();

        // Add any direct matching packs that weren't already included
        const existingPackIds = new Set(packs.map(p => p.rkey));
        for (const pack of directMatchingPacks) {
            if (!existingPackIds.has(pack.rkey)) {
                const packUsers = await db.collection('users')
                    .find({ pack_ids: pack.rkey })
                    .toArray();

                packs.push({
                    rkey: pack.rkey,
                    name: pack.name,
                    creator: pack.creator,
                    description: pack.description || '',
                    users: packUsers.map(u => ({
                        did: u.did,
                        handle: u.handle,
                        display_name: u.display_name,
                        pack_ids: u.pack_ids
                    }))
                });
            }
        }

        return {
            users: matchingUsers.map(user => ({
                did: user.did,
                handle: user.handle,
                display_name: user.display_name,
                pack_ids: user.pack_ids || []
            })),
            packs,
            packIds: [...new Set([...packIds, ...directMatchingPacks.map(p => p.rkey)])]
        };

    } finally {
        await client.close();
    }
}

function searchJSON(filePath, searchTerm, isExact, isCaseSensitive) {
    try {
        const fileContent = fs.readFileSync(filePath, 'utf8');
        let jsonData;

        try {
            jsonData = JSON.parse(fileContent);
        } catch (parseError) {
            // Handle incomplete JSON file
            const cleanContent = fileContent.trimEnd();
            if (cleanContent.endsWith(',')) {
                jsonData = JSON.parse(cleanContent.slice(0, -1) + ']');
            } else if (!cleanContent.endsWith(']')) {
                jsonData = JSON.parse(cleanContent + ']');
            } else {
                throw parseError;
            }
        }

        return processFileSearchResults(jsonData, searchTerm, isExact, isCaseSensitive);
    } catch (error) {
        throw new Error(`Error reading JSON file (file may be incomplete due to ongoing processing): ${error.message}. Try using YAML source instead.`);
    }
}

function searchYAML(filePath, searchTerm, isExact, isCaseSensitive) {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    const documents = yaml.loadAll(fileContent);
    const data = documents.flat();
    return processFileSearchResults(data, searchTerm, isExact, isCaseSensitive);
}

function processFileSearchResults(data, searchTerm, isExact, isCaseSensitive) {
    const regex = isExact ?
        new RegExp(`^${searchTerm}$`, isCaseSensitive ? '' : 'i') :
        new RegExp(searchTerm, isCaseSensitive ? '' : 'i');

    // First, collect all users from all packs
    const allUsers = new Map();
    const matchingUsers = new Set();
    const matchingPacks = new Set();

    data.forEach(pack => {
        // Check if pack matches
        if (regex.test(pack.name) || (pack.description && regex.test(pack.description))) {
            matchingPacks.add(pack);
        }

        // Process all users
        pack.users.forEach(user => {
            if (!allUsers.has(user.did)) {
                allUsers.set(user.did, {
                    ...user,
                    pack_ids: [pack.rkey]
                });
            } else {
                // Add pack reference to existing user
                const existingUser = allUsers.get(user.did);
                if (!existingUser.pack_ids.includes(pack.rkey)) {
                    existingUser.pack_ids.push(pack.rkey);
                }
            }

            // Check if user matches search criteria
            if (regex.test(user.handle) || (user.display_name && regex.test(user.display_name))) {
                matchingUsers.add(user.did);
                matchingPacks.add(pack);
            }
        });
    });

    return {
        users: Array.from(matchingUsers).map(did => allUsers.get(did)),
        packs: Array.from(matchingPacks),
        packIds: [...new Set(Array.from(matchingPacks).map(pack => pack.rkey))]
    };
}

function searchInData(data, searchTerm, isExact, isCaseSensitive) {
    const regex = isExact ?
        new RegExp(`^${searchTerm}$`, isCaseSensitive ? '' : 'i') :
        new RegExp(searchTerm, isCaseSensitive ? '' : 'i');

    return data.filter(pack => {
        // Search in pack name and description
        if (regex.test(pack.name) || (pack.description && regex.test(pack.description))) {
            // If pack metadata matches, keep track of matching users
            pack.matchingUsers = pack.users.filter(user =>
                regex.test(user.handle) ||
                (user.display_name && regex.test(user.display_name))
            );
            return true;
        }

        // Search in users
        const matchingUsers = pack.users.filter(user =>
            regex.test(user.handle) ||
            (user.display_name && regex.test(user.display_name))
        );

        if (matchingUsers.length > 0) {
            pack.matchingUsers = matchingUsers;
            return true;
        }

        return false;
    }).map(pack => ({
        _id: {
            pack_id: pack.rkey,
            pack_name: pack.name,
            pack_creator: pack.creator,
            pack_description: pack.description
        },
        matching_users: pack.matchingUsers || [],
        total_users: pack.users.length
    }));
}

function printUserResults(data) {
    const { users, packs, packIds } = data;

    if (!users || users.length === 0) {
        console.log(chalk.yellow('\nNo matching users found.'));
        return;
    }

    console.log(chalk.green(`\nFound ${users.length} matching user(s):`));

    users.forEach((user, index) => {
        console.log(chalk.bold('\n' + '='.repeat(80)));
        console.log(chalk.blue(`${index + 1}. ${user.handle}`));
        if (user.display_name) {
            console.log(chalk.dim(`Display Name: ${user.display_name}`));
        }
        console.log(chalk.dim(`DID: ${user.did}`));

        // Get packs this user belongs to
        const userPacks = packs.filter(pack =>
            user.pack_ids && user.pack_ids.includes(pack.rkey)
        );

        if (userPacks.length > 0) {
            console.log(chalk.yellow('\nMember of the following packs:'));
            userPacks.forEach(pack => {
                console.log(chalk.dim(`- ${pack.name} (${pack.rkey})`));
                if (pack.description) {
                    console.log(chalk.dim(`  Description: ${pack.description}`));
                }
                console.log(chalk.dim(`  URL: https://bsky.app/profile/${pack.creator}/lists/${pack.rkey}`));
            });
        } else if (user.pack_ids && user.pack_ids.length > 0) {
            console.log(chalk.yellow('\nMember of packs (IDs only):'));
            user.pack_ids.forEach(packId => {
                console.log(chalk.dim(`- ${packId}`));
            });
        }
    });
}

function printPackResults(data) {
    const { users, packs, packIds } = data;

    if (!packs || (packs.length === 0 && (!packIds || packIds.length === 0))) {
        console.log(chalk.yellow('\nNo matches found.'));
        return;
    }

    console.log(chalk.green(`\nFound ${packs.length} matching starter pack(s):`));

    packs.forEach((pack, index) => {
        console.log(chalk.bold('\n' + '='.repeat(80)));
        console.log(chalk.blue(`${index + 1}. ${pack.name}`));
        console.log(chalk.dim(`Creator: ${pack.creator}`));
        if (pack.description) {
            console.log(chalk.dim(`Description: ${pack.description}`));
        }

        const url = `https://bsky.app/profile/${pack.creator}/lists/${pack.rkey}`;
        console.log(chalk.cyan(`URL: ${url}`));

        // Find matching users in this pack
        const matchingUsers = users.filter(user =>
            user.pack_ids && user.pack_ids.includes(pack.rkey)
        );

        console.log(chalk.dim(`Total users in pack: ${pack.users.length}`));

        if (matchingUsers && matchingUsers.length > 0) {
            console.log(chalk.yellow('\nMatching users:'));
            matchingUsers.forEach(user => {
                console.log(chalk.dim(`- ${user.handle}${user.display_name ? ` (${user.display_name})` : ''}`));
            });
        }
    });
}

async function main() {
    const searchTerm = argv._[0];
    const source = argv.source;

    console.log(chalk.blue(`Searching for: "${searchTerm}" in ${source}`));
    console.log(chalk.dim(`Mode: ${argv.exact ? 'Exact match' : 'Partial match'}, Case ${argv.caseSensitive ? 'sensitive' : 'insensitive'}`));

    try {
        let searchResults;

        switch (source) {
            case 'mongodb':
                searchResults = await searchMongoDB(searchTerm, argv.exact, argv.caseSensitive);
                break;
            case 'json':
                const jsonPath = argv.file || 'starter_packs.json';
                if (!fs.existsSync(jsonPath)) {
                    throw new Error(`JSON file not found: ${jsonPath}`);
                }
                searchResults = searchJSON(jsonPath, searchTerm, argv.exact, argv.caseSensitive);
                break;
            case 'yaml':
                const yamlPath = argv.file || 'starter_packs.yaml';
                if (!fs.existsSync(yamlPath)) {
                    throw new Error(`YAML file not found: ${yamlPath}`);
                }
                searchResults = searchYAML(yamlPath, searchTerm, argv.exact, argv.caseSensitive);
                break;
        }

        // If neither --packs nor --users is specified, default to --packs
        if (!argv.users && !argv.packs) {
            argv.packs = true;
        }

        if (argv.users) {
            printUserResults(searchResults);
        } else {
            printPackResults(searchResults);
        }

    } catch (error) {
        console.error(chalk.red(`Error: ${error.message}`));
        process.exit(1);
    }
}

main();
