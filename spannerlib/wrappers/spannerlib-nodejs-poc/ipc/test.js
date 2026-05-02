const { Pool } = require('./index.js');
const { performance } = require('perf_hooks');
const fs = require('fs');

async function runTest() {
    console.log("==================================================");
    console.log("   Spanner NodeJS gRPC/IPC Wrapper Test           ");
    console.log("==================================================\n");

    let pool, connection, rows;

    // Read the database name created by setup_db.js
    let dbName = "dummy-testing-db";
    try {
        dbName = fs.readFileSync('./test-db.txt', 'utf8').trim();
    } catch (e) { 
        console.log("test-db.txt not found, using default. Run `npm run setup` to create a new test database.");
    }

    const dbPath = `projects/span-cloud-testing/instances/gargsurbhi-testing/databases/${dbName}`;

    try {
        console.log(`[JS-App] 1. Creating Pool attached to: \n -> ${dbPath}`);
        pool = await Pool.create(dbPath, "nodejs-ipc-poc");
        console.log(`Pool created: ID ${pool.poolId}`);

        console.log("\n[JS-App] 2. Creating Database Connection...");
        connection = await pool.createConnection();
        console.log(`Connection created: ID ${connection.connectionId}`);

        console.log("\n[JS-App] 3. Executing SQL...");
        const sqlQuery = "SELECT SingerId, FirstName, Balance, IsActive FROM Singers";
        console.log(` -> Query: "${sqlQuery}"`);

        const startTime = performance.now();
        rows = await connection.execute(sqlQuery);
        console.log(`Executed SQL successfully in ${(performance.now() - startTime).toFixed(3)}ms (Rows ID: ${rows.rowsId})`);

        console.log("\n[JS-App] 4. Fetching result set...");
        let row;
        const results = [];
        while ((row = await rows.next()) !== null) {
            results.push(row);
        }
        
        console.log(" - Fetched rows ->");
        console.log(JSON.stringify(results, null, 2));
        
        // Example of accessing a property on the first row
        if (results.length > 0) {
            console.log(`\nExample access: results[0].FirstName is "${results[0].FirstName}"`);
        }

    } catch (err) {
        console.error("Test execution failed:");
        console.error(" -> " + err.message);
        console.error(err.stack);
    } finally {
        console.log("\n[JS-App] 5. Graceful Cleanup...");
        if (rows) await rows.close();
        if (connection) await connection.close();
        if (pool) await pool.close();
        console.log("Cleaned up gracefully.");
    }
}

runTest().catch(console.error);
