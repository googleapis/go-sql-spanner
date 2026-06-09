// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import * as assert from 'assert';
import { Pool } from '../src/index.js';

describe('Production Test', () => {
  it('should execute query on production in a loop for memory check', async function () {
    this.timeout(120000);
    const connectionString =
      'projects/span-cloud-testing/instances/alka-testing/databases/db-testing';
    console.log(`Connecting to: ${connectionString}`);

    const initialMemory = process.memoryUsage().rss;
    console.log(
      `Initial Memory: ${(initialMemory / 1024 / 1024).toFixed(2)} MB`
    );

    const iterations = 10;

    try {
      for (let i = 0; i < iterations; i++) {
        const pool = await Pool.create(connectionString);
        const connection = await pool.createConnection();

        const rows = await connection.executeSql('SELECT 1');
        await rows.next();

        await rows.close();
        await connection.close();
        await pool.close();

        if ((i + 1) % 100 === 0) {
          const currentMemory = process.memoryUsage().rss;
          console.log(
            `Iteration ${i + 1}: ${(currentMemory / 1024 / 1024).toFixed(2)} MB`
          );
        }
        if (typeof global.gc === 'function') {
          global.gc();
        }
      }

      if (typeof global.gc === 'function') {
        global.gc();
      }

      const finalMemory = process.memoryUsage().rss;
      console.log(`Final Memory: ${(finalMemory / 1024 / 1024).toFixed(2)} MB`);
      const diff = (finalMemory - initialMemory) / 1024 / 1024;
      console.log(`Memory Growth: ${diff.toFixed(2)} MB`);

      assert.ok(
        diff < 30,
        `Memory growth should be reasonable, got ${diff.toFixed(2)} MB`
      );
    } catch (e: unknown) {
      console.error('Execution failed:', (e as Error).message || e);
      throw e;
    }
  });
});
