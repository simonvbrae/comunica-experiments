"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const node_path_1 = require("node:path");
const node_fs_1 = require("node:fs");
const Query_1 = require("../lib/Query");
function loadConfiguration(path) {
    const configData = JSON.parse((0, node_fs_1.readFileSync)(path, 'utf8'));
    for (const config of configData) {
        config.config = (0, node_path_1.resolve)(config.config);
        config.query = (0, node_path_1.resolve)(config.query);
    }
    return configData;
}
async function runQueryBasedOnConfiguration(config) {
    await (0, Query_1.executeQuery)(config.config, config.query, config.seed);
    console.log('Finished running');
}
const configPath = (0, node_path_1.join)('templates', 'config-query-runner.json');
const configs = loadConfiguration(configPath);
runQueryBasedOnConfiguration(configs[0])
    .then(() => console.log('Success!'))
    .catch((reason) => console.log('Fail!', reason));
//# sourceMappingURL=query-runner.js.map