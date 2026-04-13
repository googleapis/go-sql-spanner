const { bench, run } = require('mitata');
const fs = require('fs');
const path = require('path');

// Helper to read DB name from any of the POC directories
function getDbName() {
    const paths = [
        path.resolve(__dirname, '../koffi/test-db.txt'),
        path.resolve(__dirname, '../napi/test-db.txt'),
        path.resolve(__dirname, '../ipc/test-db.txt')
    ];
    for (const p of paths) {
        try {
            if (fs.existsSync(p)) {
                return fs.readFileSync(p, 'utf8').trim();
            }
        } catch (e) {
            // Ignore
        }
    }
    return "dummy-testing-db";
}

const dbName = getDbName();
const dbPath = `projects/span-cloud-testing/instances/gargsurbhi-testing/databases/${dbName}`;
const sqlQuery = "SELECT SingerId, FirstName, LastName, IsActive, Balance FROM Singers WHERE SingerId = 1";

console.log(`Using database: ${dbPath}`);

// Import wrappers
let KoffiPool, KoffiCleanup;
try {
    const koffi = require('../koffi');
    KoffiPool = koffi.Pool;
    KoffiCleanup = koffi.cleanup;
} catch (e) {
    console.warn("Could not load Koffi wrapper:", e.message);
}

let NapiPool, NapiCleanup;
try {
    const napi = require('../napi');
    NapiPool = napi.Pool;
    NapiCleanup = napi.cleanup;
} catch (e) {
    console.warn("Could not load N-API wrapper:", e.message);
}

let IpcPool;
try {
    const ipc = require('../ipc');
    IpcPool = ipc.Pool;
} catch (e) {
    console.warn("Could not load IPC wrapper:", e.message);
}

/**
 * Helper for manual measurement
 */
async function measure(name, conn, executeMethod, iterations = 500, runs = 5, sleepMs = 2000) {
    console.log(`\nRunning benchmark for ${name} (${runs} runs of ${iterations} iterations)...`);
    
    const runResults = [];
    
    for (let r = 0; r < runs; r++) {
        if (r > 0) {
            console.log(`  Sleeping for ${sleepMs}ms to let DB cool down...`);
            await new Promise(resolve => setTimeout(resolve, sleepMs));
        }
        
        console.log(`  Run ${r + 1}/${runs}...`);
        if (global.gc) global.gc();
        
        const startTimeStr = new Date().toISOString();
        const startMemory = process.memoryUsage().heapUsed;
        const startTime = performance.now();
        
        let min = Infinity;
        let max = 0;
        let total = 0;
        
        for (let i = 0; i < iterations; i++) {
            const opStart = performance.now();
            const rows = await conn[executeMethod](sqlQuery);
            await rows.next();
            await rows.close();
            const opEnd = performance.now();
            
            const opDiff = opEnd - opStart;
            if (opDiff < min) min = opDiff;
            if (opDiff > max) max = opDiff;
            total += opDiff;
        }
        
        const endTime = performance.now();
        const endTimeStr = new Date().toISOString();
        const endMemory = process.memoryUsage().heapUsed;
        
        const timeTaken = endTime - startTime;
        const opsPerSec = (iterations / timeTaken) * 1000;
        const memoryUsed = (endMemory - startMemory) / 1024 / 1024;
        const avgLatency = total / iterations;
        
        console.log(`    Throughput: ${opsPerSec.toFixed(2)} ops/sec`);
        console.log(`    Latency: Avg ${avgLatency.toFixed(2)} ms, Min ${min.toFixed(2)} ms, Max ${max.toFixed(2)} ms`);
        console.log(`    Heap Diff: ${memoryUsed.toFixed(2)} MB`);
        
        runResults.push({
            opsPerSec,
            avgLatency,
            min,
            max,
            memoryUsed,
            startTime: startTimeStr,
            endTime: endTimeStr
        });
    }
    
    const avg = {
        opsPerSec: runResults.reduce((acc, r) => acc + r.opsPerSec, 0) / runs,
        avgLatency: runResults.reduce((acc, r) => acc + r.avgLatency, 0) / runs,
        min: runResults.reduce((acc, r) => acc + r.min, 0) / runs,
        max: runResults.reduce((acc, r) => acc + r.max, 0) / runs,
        memoryUsed: runResults.reduce((acc, r) => acc + r.memoryUsed, 0) / runs
    };
    
    console.log(`\n  Average for ${name}:`);
    console.log(`    Throughput: ${avg.opsPerSec.toFixed(2)} ops/sec`);
    console.log(`    Latency: Avg ${avg.avgLatency.toFixed(2)} ms, Min ${avg.min.toFixed(2)} ms, Max ${avg.max.toFixed(2)} ms`);
    console.log(`    Heap Diff: ${avg.memoryUsed.toFixed(2)} MB`);
    
    return { runResults, avg };
}

/**
 * Initialize all available wrappers
 */
async function initialize() {
    const connections = {};
    
    if (KoffiPool) {
        try {
            console.log("Initializing Koffi...");
            const pool = await KoffiPool.create("nodejs-koffi-poc", dbPath);
            const conn = await pool.createConnection();
            const rows = await conn.executeSql(sqlQuery);
            await rows.next();
            await rows.close();
            connections.koffi = { pool, conn, cleanup: KoffiCleanup };
        } catch (e) {
            console.error("Failed to initialize Koffi:", e.message);
        }
    }
    
    if (NapiPool) {
        try {
            console.log("Initializing N-API...");
            const pool = await NapiPool.create("nodejs-napi-poc", dbPath);
            const conn = await pool.createConnection();
            const rows = await conn.executeSql(sqlQuery);
            await rows.next();
            await rows.close();
            connections.napi = { pool, conn, cleanup: NapiCleanup };
        } catch (e) {
            console.error("Failed to initialize N-API:", e.message);
        }
    }
    
    if (IpcPool) {
        try {
            console.log("Initializing IPC (Requires gRPC server running)...");
            const pool = await IpcPool.create(dbPath, "nodejs-ipc-poc");
            const conn = await pool.createConnection();
            const rows = await conn.execute(sqlQuery);
            await rows.next();
            await rows.close();
            connections.ipc = { pool, conn };
        } catch (e) {
            console.error("Failed to initialize IPC. Is the gRPC server running on localhost:50051?", e.message);
        }
    }
    
    return connections;
}

/**
 * Run Mitata Latency Benchmark
 */
async function runMitataBenchmark(connections) {
    console.log("\nStarting Mitata Latency Benchmark...");
    
    if (connections.koffi) {
        bench('Koffi Single Point Read', async () => {
            const rows = await connections.koffi.conn.executeSql(sqlQuery);
            await rows.next();
            await rows.close();
        });
    }

    if (connections.napi) {
        bench('N-API Single Point Read', async () => {
            const rows = await connections.napi.conn.executeSql(sqlQuery);
            await rows.next();
            await rows.close();
        });
    }

    if (connections.ipc) {
        bench('IPC Single Point Read', async () => {
            const rows = await connections.ipc.conn.execute(sqlQuery);
            await rows.next();
            await rows.close();
        });
    }

    const originalLog = console.log;
    let mitataOutput = '';
    console.log = (...args) => {
        mitataOutput += args.join(' ') + '\n';
        originalLog.apply(console, args);
    };

    await run();

    console.log = originalLog;
    return mitataOutput;
}

/**
 * Run Manual Throughput & Memory Benchmark
 */
async function runManualBenchmark(connections, iterations = 500) {
    console.log("\nStarting Manual Throughput & Memory Benchmark...");
    const results = {};
    
    const wrappers = [];
    if (connections.koffi) wrappers.push({ key: 'Koffi', name: 'Koffi', conn: connections.koffi.conn, method: 'executeSql' });
    if (connections.napi) wrappers.push({ key: 'Napi', name: 'N-API', conn: connections.napi.conn, method: 'executeSql' });
    if (connections.ipc) wrappers.push({ key: 'Ipc', name: 'IPC', conn: connections.ipc.conn, method: 'execute' });

    for (let i = 0; i < wrappers.length; i++) {
        if (i > 0) {
            console.log(`\nSleeping for 1 minute before ${wrappers[i].name} to let DB cool down...`);
            await new Promise(resolve => setTimeout(resolve, 60000));
        }
        const w = wrappers[i];
        results[w.key] = await measure(w.name, w.conn, w.method, iterations);
    }
    
    return results;
}

/**
 * Save results to RESULTS.md
 */
function saveResults(mitataOutput, results, iterations) {
    console.log("\nSaving results to RESULTS.md...");
    let report = `# Benchmark Results\n\n`;
    report += `## Environment\n`;
    report += `- **Database**: \`${dbPath}\`\n\n`;
    
    report += `## Mitata Latency Benchmark\n\n`;
    report += `\`\`\`\n${mitataOutput}\`\`\`\n\n`;
    
    report += `## Summary of Manual Benchmark (Average of 5 Runs)\n\n`;
    report += `| Wrapper | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff (Avg) |\n`;
    report += `| :--- | :--- | :--- | :--- | :--- | :--- |\n`;

    if (results.Koffi) {
        const avg = results.Koffi.avg;
        report += `| **Koffi** | ${avg.opsPerSec.toFixed(2)} ops/sec | ${avg.avgLatency.toFixed(2)} ms | ${avg.min.toFixed(2)} ms | ${avg.max.toFixed(2)} ms | ${avg.memoryUsed.toFixed(2)} MB |\n`;
    }
    if (results.Napi) {
        const avg = results.Napi.avg;
        report += `| **N-API** | ${avg.opsPerSec.toFixed(2)} ops/sec | ${avg.avgLatency.toFixed(2)} ms | ${avg.min.toFixed(2)} ms | ${avg.max.toFixed(2)} ms | ${avg.memoryUsed.toFixed(2)} MB |\n`;
    }
    if (results.Ipc) {
        const avg = results.Ipc.avg;
        report += `| **IPC** | ${avg.opsPerSec.toFixed(2)} ops/sec | ${avg.avgLatency.toFixed(2)} ms | ${avg.min.toFixed(2)} ms | ${avg.max.toFixed(2)} ms | ${avg.memoryUsed.toFixed(2)} MB |\n`;
    }

    report += `\n## Detailed Runs\n\n`;

    function appendDetails(name, data) {
        if (!data) return;
        report += `### ${name}\n\n`;
        report += `| Run | Start Time | End Time | Throughput | Avg Latency | Min Latency | Max Latency | Heap Diff |\n`;
        report += `| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n`;
        data.runResults.forEach((r, i) => {
            report += `| Run ${i + 1} | ${r.startTime} | ${r.endTime} | ${r.opsPerSec.toFixed(2)} ops/sec | ${r.avgLatency.toFixed(2)} ms | ${r.min.toFixed(2)} ms | ${r.max.toFixed(2)} ms | ${r.memoryUsed.toFixed(2)} MB |\n`;
        });
        const avg = data.avg;
        report += `| **Average** | - | - | ${avg.opsPerSec.toFixed(2)} ops/sec | ${avg.avgLatency.toFixed(2)} ms | ${avg.min.toFixed(2)} ms | ${avg.max.toFixed(2)} ms | ${avg.memoryUsed.toFixed(2)} MB |\n\n`;
    }

    appendDetails('Koffi', results.Koffi);
    appendDetails('N-API', results.Napi);
    appendDetails('IPC', results.Ipc);

    fs.writeFileSync(path.resolve(__dirname, 'RESULTS.md'), report);
    console.log("Results saved to RESULTS.md");
}

async function main() {
    const connections = await initialize();
    
    const mitataOutput = await runMitataBenchmark(connections);
    
    const iterations = 500;
    const results = await runManualBenchmark(connections, iterations);
    
    saveResults(mitataOutput, results, iterations);
    
    // Cleanup
    console.log("\nCleaning up...");
    if (connections.koffi) {
        await connections.koffi.conn.close();
        await connections.koffi.pool.close();
        if (connections.koffi.cleanup) connections.koffi.cleanup();
    }
    if (connections.napi) {
        await connections.napi.conn.close();
        await connections.napi.pool.close();
        if (connections.napi.cleanup) connections.napi.cleanup();
    }
    if (connections.ipc) {
        await connections.ipc.conn.close();
        await connections.ipc.pool.close();
    }
}

main().catch(console.error);
