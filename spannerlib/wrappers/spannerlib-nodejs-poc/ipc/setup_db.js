const { Spanner } = require('@google-cloud/spanner');

const projectId = 'span-cloud-testing';
const instanceId = 'gargsurbhi-testing';
const databaseId = `test-db-ipc-${Math.floor(Math.random() * 1000)}`;

const spanner = new Spanner({ projectId });
const instance = spanner.instance(instanceId);

async function setupDatabase() {
  console.log(`Setting up Spanner Database: ${databaseId}...`);
  try {
    const [database, operation] = await instance.createDatabase(databaseId, {
      schema: [
        `CREATE TABLE Singers (
          SingerId   INT64 NOT NULL,
          FirstName  STRING(1024),
          LastName   STRING(1024),
          IsActive   BOOL,
          Balance    FLOAT64
        ) PRIMARY KEY (SingerId)`
      ],
    });

    console.log(`Database creation initiated. Waiting for operation...`);
    await operation.promise();
    console.log(`Database ${database.id} created with Schema.`);

    console.log('Inserting dummy data...');
    const table = database.table('Singers');
    await table.insert([
      { SingerId: 1, FirstName: 'Marc', LastName: 'Richards', IsActive: true, Balance: 100.50 },
      { SingerId: 2, FirstName: 'Catalina', LastName: 'Smith', IsActive: false, Balance: 250.75 },
      { SingerId: 3, FirstName: 'Alice', LastName: 'Trentor', IsActive: true, Balance: 0.10 }
    ]);
    console.log('Dummy data inserted successfully!');

    // Save DB name to file so the test can pick it up
    require('fs').writeFileSync('./test-db.txt', databaseId);

  } catch (err) {
    console.error('ERROR:', err);
  }
}

setupDatabase().catch(console.error);
