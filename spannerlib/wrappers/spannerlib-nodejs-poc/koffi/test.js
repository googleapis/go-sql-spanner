const { Pool, cleanup } = require('./index.js');
const { performance } = require('perf_hooks');
const fs = require('fs');

async function runTest() {
    console.log("==================================================");
    console.log("   Spanner Shared Library POC (PRODUCTION ASYNC)  ");
    console.log("==================================================\n");

    let pool, connection, rows;

    // Read the database name created by setup_db.js
    let dbName = "dummy-testing-db";
    try {
        dbName = fs.readFileSync('./test-db.txt', 'utf8').trim();
    } catch (e) { }

    const dbPath = `projects/span-cloud-testing/instances/gargsurbhi-testing/databases/${dbName}`;

    try {
        console.log(`[JS-App] 1. Creating Pool attached to: \n -> ${dbPath}`);
        pool = await Pool.create("nodejs-koffi-poc", dbPath);
        console.log(`Pool created: OID ${pool.oid}`);

        console.log("\n[JS-App] 2. Creating Database Connection...");
        connection = await pool.createConnection();
        console.log(`Connection created: OID ${connection.oid}`);

        console.log("\n[JS-App] 3. Executing SQL using pure Nodejs @google-cloud/spanner Protobufs...");
        const sqlQuery = "SELECT SingerId, FirstName, Balance, IsActive FROM Singers";
        console.log(` -> Serializing FFI payload for: "${sqlQuery}"`);

        const startTime = performance.now();
        rows = await connection.executeSql(sqlQuery);
        console.log(`Executed SQL successfully in ${(performance.now() - startTime).toFixed(3)}ms (Rows OID: ${rows.oid})`);

        console.log("\n[JS-App] 4. Fetching ResultSet Native Types (Int, String, Float, Bool)...");
        let nextRow;
        while ((nextRow = await rows.next()) !== null) {
            // nextRow is an array of Value protobuf objects (Google Protobuf Structs)
            console.log(" - Fetched native row chunk ->");
            console.log("    " + JSON.stringify(nextRow));
        }

    } catch (err) {
        console.error("Test execution caught an expected Native Error:");
        console.error(" -> " + err.message);
    } finally {
        console.log("\n[JS-App] 5. Graceful Async Cleanup...");
        if (rows) await rows.close();
        if (connection) await connection.close();
        if (pool) await pool.close();
        cleanup();
        console.log("Cleaned up gracefully.");
    }
}

runTest().catch(console.error);
