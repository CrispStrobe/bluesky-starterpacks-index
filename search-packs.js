#!/usr/bin/env node
require('dotenv').config();
const { MongoClient } = require('mongodb');
const fs = require('fs');
const yaml = require('js-yaml');
const chalk = require('chalk');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

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
    .demandCommand(1, 'Please provide a search term')
    .help()
    .argv;

async function searchMongoDB(searchTerm, isExact, isCaseSensitive) {
    const client = new MongoClient(process.env.MONGODB_URI);
    try {
        await client.connect();
        const db = client.db('starterpacks');
        const collection = db.collection('users');

        // Build search query
        let searchRegex;
        if (isExact) {
            searchRegex = `^${searchTerm}$`;
        } else {
            searchRegex = searchTerm;
        }
        const regexOptions = isCaseSensitive ? '' : 'i';

        // Search in handle, display_name, and pack related fields
        const query = {
            $or: [
                { handle: { $regex: searchRegex, $options: regexOptions } },
                { display_name: { $regex: searchRegex, $options: regexOptions } },
                { pack_name: { $regex: searchRegex, $options: regexOptions } },
                { pack_description: { $regex: searchRegex, $options: regexOptions } }
            ]
        };

        // Group by pack to avoid duplicates
        const results = await collection.aggregate([
            { $match: query },
            {
                $group: {
                    _id: {
                        pack_id: "$pack_id",
                        pack_name: "$pack_name",
                        pack_creator: "$pack_creator",
                        pack_description: "$pack_description"
                    },
                    matching_users: {
                        $push: {
                            handle: "$handle",
                            display_name: "$display_name",
                            did: "$did"
                        }
                    },
                    total_users: { $sum: 1 }
                }
            }
        ]).toArray();

        return results;
    } finally {
        await client.close();
    }
}

function searchJSON(filePath, searchTerm, isExact, isCaseSensitive) {
    const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return searchInData(data, searchTerm, isExact, isCaseSensitive);
}

function searchYAML(filePath, searchTerm, isExact, isCaseSensitive) {
    const data = yaml.load(fs.readFileSync(filePath, 'utf8'));
    return searchInData(data, searchTerm, isExact, isCaseSensitive);
}

function searchInData(data, searchTerm, isExact, isCaseSensitive) {
    const regex = isExact ? 
        new RegExp(`^${searchTerm}$`, isCaseSensitive ? '' : 'i') : 
        new RegExp(searchTerm, isCaseSensitive ? '' : 'i');

    return data.filter(pack => {
        // Search in pack name and description
        if (regex.test(pack.name) || (pack.description && regex.test(pack.description))) {
            return true;
        }
        // Search in users
        return pack.users.some(user => 
            regex.test(user.handle) || (user.display_name && regex.test(user.display_name))
        );
    }).map(pack => ({
        _id: {
            pack_id: pack.rkey,
            pack_name: pack.name,
            pack_creator: pack.creator,
            pack_description: pack.description
        },
        matching_users: pack.users,
        total_users: pack.users.length
    }));
}

function printResults(results) {
    if (results.length === 0) {
        console.log(chalk.yellow('\nNo matches found.'));
        return;
    }

    console.log(chalk.green(`\nFound ${results.length} matching starter pack(s):`));
    
    results.forEach((pack, index) => {
        console.log(chalk.bold('\n' + '='.repeat(80)));
        console.log(chalk.blue(`${index + 1}. ${pack._id.pack_name}`));
        console.log(chalk.dim(`Creator: ${pack._id.pack_creator}`));
        if (pack._id.pack_description) {
            console.log(chalk.dim(`Description: ${pack._id.pack_description}`));
        }
        
        // Create clickable URL
        const url = `https://bsky.app/profile/${pack._id.pack_creator}/lists/${pack._id.pack_id}`;
        console.log(chalk.cyan(`URL: ${url}`));
        
        console.log(chalk.dim(`Total users in pack: ${pack.total_users}`));
        
        if (pack.matching_users.length > 0) {
            console.log(chalk.yellow('\nMatching users:'));
            pack.matching_users.forEach(user => {
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
        let results;
        
        switch (source) {
            case 'mongodb':
                results = await searchMongoDB(searchTerm, argv.exact, argv.caseSensitive);
                break;
            case 'json':
                const jsonPath = argv.file || 'starter_packs.json';
                if (!fs.existsSync(jsonPath)) {
                    throw new Error(`JSON file not found: ${jsonPath}`);
                }
                results = searchJSON(jsonPath, searchTerm, argv.exact, argv.caseSensitive);
                break;
            case 'yaml':
                const yamlPath = argv.file || 'starter_packs.yaml';
                if (!fs.existsSync(yamlPath)) {
                    throw new Error(`YAML file not found: ${yamlPath}`);
                }
                results = searchYAML(yamlPath, searchTerm, argv.exact, argv.caseSensitive);
                break;
        }

        printResults(results);

    } catch (error) {
        console.error(chalk.red(`Error: ${error.message}`));
        process.exit(1);
    }
}

main();