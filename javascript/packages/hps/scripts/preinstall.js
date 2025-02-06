const { spawn } = require("node:child_process");
const path = require("node:path");
const version = process.version;
const fs = require('fs-extra');

async function downloadDeps(urls) {
    await Promise.all(urls.map(({url, dist}) => {
        new Promise((resolve, reject) => {
            fs.ensureDirSync(path.dirname(dist));
            const process = spawn('curl', [url, '-o', dist]);
            process.on("close", () => {
                console.log("finish")
                resolve();
            });
            process.on("error", () => {
                console.error(`download from ${url} failed`);
                reject();
            });
        })
    }))
}

async function main() {
    await downloadDeps([
        {
            url: `https://raw.githubusercontent.com/nodejs/node/refs/tags/${version}/deps/v8/include/v8-fast-api-calls.h`,
            dist: path.join(__dirname, "..", "includes/v8-fast-api-calls.h")
        }
    ]);
}

main();