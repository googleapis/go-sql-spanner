// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc.loader;
using Google.Cloud.Spanner.V1;
using Google.Rpc;
using SpannerException = Google.Cloud.SpannerLib.SpannerException;

namespace Google.Cloud.Spanner.DataProvider.Benchmarks.tpcc;

internal class TpccRunner : AbstractRunner
{
    internal enum TransactionType
    {
        Unknown,
        NewOrder,
        Payment,
        OrderStatus,
        Delivery,
        StockLevel,
    }
    
    private readonly double _delayBetweenTransactions;
    private readonly Stats _stats;
    private readonly DbConnection _connection;
    private readonly int _numWarehouses;
    private readonly int _numDistrictsPerWarehouse;
    private readonly int _numCustomersPerDistrict;
    private readonly int _numItems;
    private readonly bool _isClientLib;
    
    private DbTransaction? _currentTransaction;
    
    internal TpccRunner(
        long transactionsPerSecond,
        Stats stats,
        DbConnection connection,
        int numWarehouses,
        int numDistrictsPerWarehouse = 10,
        int numCustomersPerDistrict = 3000,
        int numItems = 100_000)
    {
        if (transactionsPerSecond > 0)
        {
            _delayBetweenTransactions = 1000d / transactionsPerSecond;
        }
        else
        {
            _delayBetweenTransactions = 0;
        }
        _stats = stats;
        _connection = connection;
        _numWarehouses = numWarehouses;
        _numDistrictsPerWarehouse = numDistrictsPerWarehouse;
        _numCustomersPerDistrict = numCustomersPerDistrict;
        _numItems = numItems;
        _isClientLib = connection is Data.SpannerConnection;
    }

    public override async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var stopwatch = Stopwatch.StartNew();
            await RunTransactionAsync(cancellationToken);
            stopwatch.Stop();
            int delay;
            if (_delayBetweenTransactions > 0)
            {
                delay = (int) (_delayBetweenTransactions - stopwatch.ElapsedMilliseconds);
            }
            else
            {
                delay = Random.Shared.Next(10, 100);
            }
            delay = Math.Max(delay, 0);
            await Task.Delay(delay, cancellationToken);
        }
    }

    public override async Task RunTransactionAsync(CancellationToken cancellationToken)
    {
        var watch = Stopwatch.StartNew();
        var transaction = Random.Shared.Next(23);
        var transactionType = TransactionType.Unknown;
        var attempts = 0;
        while (true)
        {
            attempts++;
            try
            {
                if (transaction < 10)
                {
                    transactionType = TransactionType.NewOrder;
                    await NewOrderAsync(cancellationToken);
                }
                else if (transaction < 20)
                {
                    transactionType = TransactionType.Payment;
                    await PaymentAsync(cancellationToken);
                }
                else if (transaction < 21)
                {
                    transactionType = TransactionType.OrderStatus;
                    await OrderStatusAsync(cancellationToken);
                }
                else if (transaction < 22)
                {
                    transactionType = TransactionType.Delivery;
                    await DeliveryAsync(cancellationToken);
                }
                else if (transaction < 23)
                {
                    transactionType = TransactionType.StockLevel;
                    await StockLevelAsync(cancellationToken);
                }
                else
                {
                    throw new ArgumentException($"Invalid transaction type {transaction}");
                }

                _stats.RegisterTransaction(transactionType, watch.Elapsed);
                break;
            }
            catch (Exception exception)
            {
                await SilentRollbackTransactionAsync(cancellationToken);
                if (attempts < 10)
                {
                    if (exception is SpannerException { Code: Code.Aborted })
                    {
                        _stats.RegisterAbortedTransaction(transactionType, watch.Elapsed, exception);
                        continue;
                    }
                    if (exception is SpannerDbException { Status.Code: (int)Code.Aborted })
                    {
                        _stats.RegisterAbortedTransaction(transactionType, watch.Elapsed, exception);
                        continue;
                    }
                    if (exception is Data.SpannerException { ErrorCode: ErrorCode.Aborted })
                    {
                        _stats.RegisterAbortedTransaction(transactionType, watch.Elapsed, exception);
                        continue;
                    }
                }
                else
                {
                    await Console.Error.WriteLineAsync($"Giving up after {attempts} attempts");
                }

                _stats.RegisterFailedTransaction(transactionType, watch.Elapsed, exception);
                break;
            }
            finally
            {
                if (_currentTransaction != null)
                {
                    await Console.Error.WriteLineAsync("Transaction still open!");
                    await _currentTransaction.DisposeAsync();
                    _currentTransaction = null;
                }
            }
        }
    }

    private async Task NewOrderAsync(CancellationToken cancellationToken)
    {
        var warehouseId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numWarehouses));
        var districtId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numDistrictsPerWarehouse));
        var customerId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numCustomersPerDistrict));

        var orderLineCount = Random.Shared.Next(5, 16);
        var itemIds = new long[orderLineCount];
        var supplyWarehouses = new long[orderLineCount];
        var quantities = new int[orderLineCount];
        var rollback = Random.Shared.Next(100);
        var allLocal = 1;

        for (var line = 0; line < orderLineCount; line++)
        {
            if (rollback == 1 && line == orderLineCount - 1)
            {
                itemIds[line] = DataLoader.ReverseBitsUnsigned(long.MaxValue);
            }
            else
            {
                // TODO: Make sure that the chosen item IDs are unique.
                itemIds[line] = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numItems));
            }

            if (Random.Shared.Next(100) == 50)
            {
                supplyWarehouses[line] = GetOtherWarehouseId(warehouseId);
                allLocal = 0;
            }
            else
            {
                supplyWarehouses[line] = warehouseId;
            }

            quantities[line] = Random.Shared.Next(1, 10);
        }

        await BeginTransactionAsync("new_order", cancellationToken);

        // TODO: These queries can run in parallel.
        var row = await ExecuteRowAsync(
            "SELECT c_discount, c_last, c_credit, w_tax " +
            "FROM customer c, warehouse w " +
            "WHERE w.w_id = $1 AND c.w_id = w.w_id AND c.d_id = $2 AND c.c_id = $3 " +
            "FOR UPDATE", cancellationToken,
            warehouseId, districtId, customerId);
        var discount = ToDecimal(row[0]);
        var last = (string)row[1];
        var credit = (string)row[2];
        var warehouseTax = ToDecimal(row[3]);

        row = await ExecuteRowAsync(
            "SELECT d_next_o_id, d_tax " +
            "FROM district " +
            "WHERE w_id = $1 AND d_id = $2 FOR UPDATE", cancellationToken,
            warehouseId, districtId);
        var districtNextOrderId = row[0] is DBNull ? 0L : (long)row[0];
        var districtTax = ToDecimal(row[1]);

        object batch = _isClientLib ? (_currentTransaction as Data.SpannerTransaction)!.CreateBatchDmlCommand() : _connection.CreateBatch();
        CreateBatchCommand(
            batch,
            "UPDATE district SET d_next_o_id = $1 WHERE d_id = $2 AND w_id= $3",
            districtNextOrderId + 1L, districtId, warehouseId);
        CreateBatchCommand(
            batch,
            "INSERT INTO orders (o_id, d_id, w_id, c_id, o_entry_d, o_ol_cnt, o_all_local) " +
            "VALUES ($1,$2,$3,$4,CURRENT_TIMESTAMP,$5,$6)",
            districtNextOrderId, districtId, warehouseId, customerId, orderLineCount, allLocal);
        CreateBatchCommand(
            batch,
            "INSERT INTO new_orders (o_id, c_id, d_id, w_id) VALUES ($1,$2,$3,$4)",
            districtNextOrderId, customerId, districtId, warehouseId);

        for (var line = 0; line < orderLineCount; line++)
        {
            var orderLineSupplyWarehouseId = supplyWarehouses[line];
            var orderLineItemId = itemIds[line];
            var orderLineQuantity = quantities[line];
            try
            {
                row = await ExecuteRowAsync(
                    "SELECT i_price, i_name, i_data FROM item WHERE i_id = $1",
                    cancellationToken,
                    orderLineItemId);
            }
            catch (RowNotFoundException)
            {
                // TODO: Record deliberate rollback
                await RollbackTransactionAsync(cancellationToken);
                return;
            }

            var itemPrice = ToDecimal(row[0]);
            var itemName = (string)row[1];
            var itemData = (string)row[2];

            row = await ExecuteRowAsync(
                "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 " +
                "FROM stock " +
                "WHERE s_i_id = $1 AND w_id= $2 FOR UPDATE",
                cancellationToken,
                orderLineItemId, orderLineSupplyWarehouseId);
            var stockQuantity = (long)row[0];
            var stockData = (string)row[1];
            var stockDistrict = new string[10];
            for (int i = 2; i < stockDistrict.Length + 2; i++)
            {
                stockDistrict[i - 2] = (string)row[i];
            }

            var orderLineDistrictInfo =
                stockDistrict[(int)(DataLoader.ReverseBitsUnsigned((ulong)districtId) % stockDistrict.Length)];
            if (stockQuantity > orderLineQuantity)
            {
                stockQuantity = stockQuantity - orderLineQuantity;
            }
            else
            {
                stockQuantity = stockQuantity - orderLineQuantity + 91;
            }

            CreateBatchCommand(batch, "UPDATE stock SET s_quantity=$1 WHERE s_i_id=$2 AND w_id=$3",
                stockQuantity, orderLineItemId, orderLineSupplyWarehouseId);

            var totalTax = 1m + warehouseTax + districtTax;
            var discountFactor = 1m - discount;
            var orderLineAmount = orderLineQuantity * itemPrice * totalTax * discountFactor;
            CreateBatchCommand(batch,
                "INSERT INTO order_line (o_id, c_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                districtNextOrderId,
                customerId,
                districtId,
                warehouseId,
                line,
                orderLineItemId,
                orderLineSupplyWarehouseId,
                orderLineQuantity,
                orderLineAmount,
                orderLineDistrictInfo);
        }

        if (batch is Data.SpannerBatchCommand spannerBatchCommand)
        {
            await spannerBatchCommand.ExecuteNonQueryAsync(cancellationToken);
        }
        else if (batch is DbBatch dbBatch)
        {
            await dbBatch.ExecuteNonQueryAsync(cancellationToken);
        }
        else
        {
            throw new NotSupportedException("Batch type not supported");
        }
        await CommitTransactionAsync(cancellationToken);
    }

    private async Task PaymentAsync(CancellationToken cancellationToken)
    {
        var warehouseId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numWarehouses));
        var districtId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numDistrictsPerWarehouse));
        var customerId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numCustomersPerDistrict));
        var amount = Random.Shared.Next(1, 500000) / 100m;
        
        long customerWarehouseId;
        long customerDistrictId;
        var lastName = LastNameGenerator.GenerateLastName(long.MaxValue);
        bool byName;
        object[] row;
        if (Random.Shared.Next(100) < 60)
        {
            byName = true;
        }
        else
        {
            byName = false;
        }
        if (Random.Shared.Next(100) < 85)
        {
            customerWarehouseId = warehouseId;
            customerDistrictId = districtId;
        }
        else
        {
            customerWarehouseId = GetOtherWarehouseId(warehouseId);
            customerDistrictId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numDistrictsPerWarehouse));
        }
        await BeginTransactionAsync("payment", cancellationToken);
        await ExecuteNonQueryAsync("UPDATE warehouse SET w_ytd = w_ytd + $1 WHERE w_id = $2",
            cancellationToken, amount, warehouseId);

        row = await ExecuteRowAsync(
                "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name "
                + "FROM warehouse "
                + "WHERE w_id = $1",
                cancellationToken, warehouseId);
        var warehouseStreet1 = (string) row[0];
        var warehouseStreet2 = (string) row[1];
        var warehouseCity = (string) row[2];
        var warehouseState = (string) row[3];
        var warehouseZip = (string) row[4];
        var warehouseName = (string) row[5];
        
        await ExecuteNonQueryAsync(
            "UPDATE district SET d_ytd = d_ytd + $1 WHERE w_id = $2 AND d_id= $3",
            cancellationToken, amount, warehouseId, districtId);

        row = await ExecuteRowAsync(
                "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name "
                + "FROM district "
                + "WHERE w_id = $1 AND d_id = $2",
                cancellationToken, warehouseId, districtId);
        var districtStreet1 = (string) row[0];
        var districtStreet2 = (string) row[1];
        var districtCity = (string) row[2];
        var districtState = (string) row[3];
        var districtZip = (string) row[4];
        var districtName = (string) row[5];

        if (byName)
        {
            row = await ExecuteRowAsync(
                    "SELECT count(c_id) namecnt "
                    + "FROM customer "
                    + "WHERE w_id = $1 AND d_id= $2 AND c_last=$3",
                    cancellationToken, customerWarehouseId, customerDistrictId, lastName);
            var nameCount = (int) (long) row[0];
            if (nameCount % 2 == 0)
            {
                nameCount++;
            }
            var resultSet = await ExecuteQueryAsync(
                    "SELECT c_id "
                    + "FROM customer "
                    + "WHERE w_id=$1 AND d_id=$2 AND c_last=$3 "
                    + "ORDER BY c_first",
                    cancellationToken, customerWarehouseId, customerDistrictId, lastName);
            for (var counter = 0; counter < Math.Min(nameCount, resultSet.Count); counter++)
            {
                customerId = (long) resultSet[counter][0];
            }
        }
        row = await ExecuteRowAsync(
                "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since "
                + "FROM customer "
                + "WHERE w_id=$1 AND d_id=$2 AND c_id=$3 FOR UPDATE",
                cancellationToken, customerWarehouseId, customerDistrictId, customerId);
        var firstName = (string) row[0];
        var middleName = (string) row[1];
        lastName = (string) row[2];
        var street1 = (string) row[3];
        var street2 = (string) row[4];
        var city = (string) row[5];
        var state = (string) row[6];
        var zip = (string) row[7];
        var phone = (string) row[8];
        var credit = (string) row[9];
        var creditLimit = (long) row[10];
        var discount = ToDecimal(row[11]);
        var balance = ToDecimal(row[12]);
        var ytdPayment = ToDecimal(row[13]);
        var since = (DateTime) row[14];
        
        // TODO: Use batching from here
        balance = balance - amount;
        ytdPayment = ytdPayment + amount;
        if ("BC".Equals(credit))
        {
            row = await ExecuteRowAsync(
                "SELECT c_data FROM customer WHERE w_id=$1 AND d_id=$2 AND c_id=$3",
                cancellationToken, customerWarehouseId, customerDistrictId, customerId);
            var customerData = (string)row[0];
            var newCustomerData =
                $"| {customerId} {customerDistrictId} {customerWarehouseId} {districtId} {warehouseId} {amount} {DateTime.Now} {customerData}";
            if (newCustomerData.Length > 500)
            {
                newCustomerData = newCustomerData.Substring(0, 500);
            }
            await ExecuteNonQueryAsync(
                "UPDATE customer "
                + "SET c_balance=$1, c_ytd_payment=$2, c_data=$3 "
                + "WHERE w_id = $4 AND d_id=$5 AND c_id=$6",
                cancellationToken,
                balance,
                ytdPayment,
                newCustomerData,
                customerWarehouseId,
                customerDistrictId,
                customerId
            );
        }
        else
        {
            await ExecuteNonQueryAsync(
                "UPDATE customer "
                + "SET c_balance=$1, c_ytd_payment=$2 "
                + "WHERE w_id = $3 AND d_id=$4 AND c_id=$5",
                cancellationToken, balance, ytdPayment, customerWarehouseId, customerDistrictId, customerId);
        }

        var data = $"{warehouseName} {districtName}";
        if (data.Length > 24)
        {
            data = data.Substring(0, 24);
        }
        await ExecuteNonQueryAsync(
            "INSERT INTO history (d_id, w_id, c_id, h_d_id,  h_w_id, h_date, h_amount, h_data) " +
            "VALUES ($1,$2,$3,$4,$5,CURRENT_TIMESTAMP,$6,$7)",
            cancellationToken,
            customerDistrictId,
            customerWarehouseId,
            customerId,
            districtId,
            warehouseId,
            amount,
            data);

        await CommitTransactionAsync(cancellationToken);
    }

    private async Task OrderStatusAsync(CancellationToken cancellationToken)
    {
        var warehouseId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numWarehouses));
        var districtId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numDistrictsPerWarehouse));
        var customerId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numCustomersPerDistrict));
        
        var lastName = LastNameGenerator.GenerateLastName(long.MaxValue);
        object[] row;
        var byName = Random.Shared.Next(100) < 60;

        decimal balance;
        string first, middle, last;

        await BeginTransactionAsync("order_status", cancellationToken);
        if (byName)
        {
            row = await ExecuteRowAsync(
                    "SELECT count(c_id) namecnt "
                    + "FROM customer "
                    + "WHERE w_id=$1 AND d_id=$2 AND c_last=$3",
                    cancellationToken, warehouseId, districtId, lastName);
            int nameCount = (int) (long) row[0];
            if (nameCount % 2 == 0)
            {
                nameCount++;
            }
            var resultSet = await ExecuteQueryAsync(
                    "SELECT c_balance, c_first, c_middle, c_id "
                    + "FROM customer WHERE w_id = $1 AND d_id=$2 AND c_last=$3 "
                    + "ORDER BY c_first",
                    cancellationToken, warehouseId, districtId, lastName);
            for (int counter = 0; counter < Math.Min(nameCount, resultSet.Count); counter++)
            {
                balance = ToDecimal(resultSet[counter][0]);
                first = (string) resultSet[counter][1];
                middle = (string) resultSet[counter][2];
                customerId = (long) resultSet[counter][3];
            }
        }
        else
        {
            row = await ExecuteRowAsync(
                    "SELECT c_balance, c_first, c_middle, c_last "
                    + "FROM customer "
                    + "WHERE w_id = $1 AND d_id=$2 AND c_id=$3",
                    cancellationToken, warehouseId, districtId, customerId);
            balance = ToDecimal(row[0]);
            first = (string) row[1];
            middle = (string) row[2];
            last = (string) row[3];
        }
        
        var maybeRow = await ExecuteRowAsync(false,
                "SELECT o_id, o_carrier_id, o_entry_d "
                + "FROM orders "
                + "WHERE w_id = $1 AND d_id = $2 AND c_id = $3 "
                + "ORDER BY o_id DESC",
                cancellationToken, warehouseId, districtId, customerId);
        var orderId = maybeRow == null ? 0L : (long) maybeRow[0];

        long item_id, supply_warehouse_id, quantity;
        decimal amount;
        DateTime? delivery_date;
        var results = await ExecuteQueryAsync(
                "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
                + "FROM order_line "
                + "WHERE w_id = $1 AND d_id = $2  AND o_id = $3",
                cancellationToken, warehouseId, districtId, orderId);
        for (var counter = 0; counter < results.Count; counter++)
        {
            item_id = (long) results[counter][0]; // item_id
            supply_warehouse_id = (long) results[counter][1]; // supply_warehouse_id
            quantity = (long) results[counter][2]; // quantity
            amount = ToDecimal(results[counter][3]); // amount
            delivery_date = results[counter][4] is DBNull ? null : (DateTime) results[counter][4]; // delivery_date
        }
        await CommitTransactionAsync(cancellationToken);
    }

    private async Task DeliveryAsync(CancellationToken cancellationToken)
    {
        var warehouseId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numWarehouses));
        var carrierId = Random.Shared.Next(10);

        await BeginTransactionAsync("delivery", cancellationToken);

        for (var district = 0L; district < _numDistrictsPerWarehouse; district++)
        {
            var districtId = DataLoader.ReverseBitsUnsigned((ulong)district);
            var row = await ExecuteRowAsync(false,
                "SELECT o_id, c_id "
                + "FROM new_orders "
                + "WHERE d_id = $1 AND w_id = $2 "
                + "ORDER BY o_id ASC "
                + "LIMIT 1 FOR UPDATE",
                cancellationToken, districtId, warehouseId);
            if (row != null)
            {
                var newOrderId = (long)row[0];
                var customerId = (long)row[1];
                await ExecuteNonQueryAsync(
                    "DELETE "
                    + "FROM new_orders "
                    + "WHERE o_id = $1 AND c_id = $2 AND d_id = $3 AND w_id = $4",
                    cancellationToken, newOrderId, customerId, districtId, warehouseId);
                row = await ExecuteRowAsync(
                    "SELECT c_id FROM orders WHERE o_id = $1 AND d_id = $2 AND w_id = $3",
                    cancellationToken, newOrderId, districtId, warehouseId);
                await ExecuteNonQueryAsync(
                    "UPDATE orders "
                    + "SET o_carrier_id = $1 "
                    + "WHERE o_id = $2 AND c_id = $3 AND d_id = $4 AND w_id = $5",
                    cancellationToken, carrierId, newOrderId, customerId, districtId, warehouseId);
                await ExecuteNonQueryAsync(
                    "UPDATE order_line "
                    + "SET ol_delivery_d = CURRENT_TIMESTAMP "
                    + "WHERE o_id = $1 AND c_id = $2 AND d_id = $3 AND w_id = $4",
                    cancellationToken, newOrderId, customerId, districtId, warehouseId);
                row = await ExecuteRowAsync(
                    "SELECT SUM(ol_amount) sm "
                    + "FROM order_line "
                    + "WHERE o_id = $1 AND c_id = $2 AND d_id = $3 AND w_id = $4",
                    cancellationToken, newOrderId, customerId, districtId, warehouseId);
                var sumOrderLineAmount = ToDecimal(row[0]);
                await ExecuteNonQueryAsync(
                    "UPDATE customer "
                    + "SET c_balance = c_balance + $1, c_delivery_cnt = c_delivery_cnt + 1 "
                    + "WHERE c_id = $2 AND d_id = $3 AND w_id = $4",
                    cancellationToken, sumOrderLineAmount, customerId, districtId, warehouseId);
            }
        }
        await CommitTransactionAsync(cancellationToken);
    }

    private async Task StockLevelAsync(CancellationToken cancellationToken)
    {
        var warehouseId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numWarehouses));
        var districtId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numDistrictsPerWarehouse));
        var level = Random.Shared.Next(10, 21);
        
        await BeginTransactionAsync("stock_level", cancellationToken);
        String stockLevelQueries = "case1";
        Object[] row;

        row = await ExecuteRowAsync(
                "SELECT d_next_o_id FROM district WHERE d_id = $1 AND w_id= $2",
                cancellationToken, districtId, warehouseId);
        var nextOrderId = row[0] is DBNull ? 0L : (long) row[0];
        var resultSet =
            await ExecuteQueryAsync(
                "SELECT COUNT(DISTINCT (s_i_id)) "
                + "FROM order_line ol, stock s "
                + "WHERE ol.w_id = $1 "
                + "AND ol.d_id = $2 "
                + "AND ol.o_id < $3 "
                + "AND ol.o_id >= $4 "
                + "AND s.w_id= $5 "
                + "AND s_i_id=ol_i_id "
                + "AND s_quantity < $6",
                cancellationToken,
                warehouseId, districtId, nextOrderId, nextOrderId - 20, warehouseId, level);
        for (var counter = 0; counter < resultSet.Count; counter++) {
            var orderLineItemId = (long) resultSet[counter][0];
            row = await ExecuteRowAsync(
                    "SELECT count(1) FROM stock "
                    + "WHERE w_id = $1 AND s_i_id = $2 "
                    + "AND s_quantity < $3",
                    cancellationToken, warehouseId, orderLineItemId, level);
            var stockCount = (long) row[0];
        }

        await CommitTransactionAsync(cancellationToken);
    }

    private decimal ToDecimal(object value)
    {
        return _isClientLib ? ((PgNumeric) value).ToDecimal(LossOfPrecisionHandling.Truncate) : (decimal) value;
    }

    private async Task BeginTransactionAsync(string tag, CancellationToken cancellationToken = default)
    {
        if (_connection is Data.SpannerConnection spannerConnection)
        {
            _currentTransaction = await spannerConnection.BeginTransactionAsync(
                SpannerTransactionCreationOptions.ReadWrite.WithIsolationLevel(IsolationLevel.RepeatableRead),
                new SpannerTransactionOptions
                {
                    Tag = tag + "_client_lib",
                },
                cancellationToken);
        }
        else if (_connection is SpannerConnection connection)
        {
            _currentTransaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
            await ExecuteNonQueryAsync($"set local transaction_tag = '{tag}_spanner_lib'", cancellationToken);
        }
    }

    private async Task CommitTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (_currentTransaction != null)
        {
            await _currentTransaction.CommitAsync(cancellationToken);
            await _currentTransaction.DisposeAsync();
            _currentTransaction = null;
        }
    }

    private async Task SilentRollbackTransactionAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (_currentTransaction != null)
            {
                await RollbackTransactionAsync(cancellationToken);
            }
            else
            {
                await ExecuteNonQueryAsync("rollback", cancellationToken);
            }
        }
        catch (Exception)
        {
            if (_currentTransaction != null)
            {
                await _currentTransaction.DisposeAsync();
                _currentTransaction = null;
            }
        }
    }

    private async Task RollbackTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (_currentTransaction != null)
        {
            await _currentTransaction.RollbackAsync(cancellationToken);
            await  _currentTransaction.DisposeAsync();
            _currentTransaction = null;
        }
    }

    private void CreateBatchCommand(object batch, string commandText, params object[] parameters)
    {
        if (batch is Data.SpannerBatchCommand command)
        {
            CreateBatchCommand(command, commandText, parameters);
        }
        else if (batch is DbBatch dbBatch)
        {
            CreateBatchCommand(dbBatch, commandText, parameters);
        }
        else
        {
            throw new ArgumentException("unknown batch type");
        }
    }

    private void CreateBatchCommand(Data.SpannerBatchCommand batch, string commandText, params object[] parameters)
    {
        var paramCollection = new Data.SpannerParameterCollection();
        for (var i=0; i < parameters.Length; i++)
        {
            var value = parameters[i];
            if (value is decimal d)
            {
                value = PgNumeric.FromDecimal(d);
            }
            paramCollection.Add(new Data.SpannerParameter {ParameterName = $"p{i+1}", Value = value});
        }
        batch.Add(commandText, paramCollection);
    }

    private void CreateBatchCommand(DbBatch batch, string commandText, params object[] parameters)
    {
        var batchCommand = batch.CreateBatchCommand();
        batchCommand.CommandText = commandText;
        for (var i = 0; i < parameters.Length; i++)
        {
            CreateParameter(batchCommand, $"p{i+1}", parameters[i]);
        }
        batch.BatchCommands.Add(batchCommand);
    }
    
    private void CreateParameter(DbBatchCommand cmd, string parameterName, object parameterValue)
    {
        var parameter = cmd.CreateParameter();
        parameter.ParameterName = parameterName;
        parameter.Value = parameterValue;
        cmd.Parameters.Add(parameter);
    }

    private async Task ExecuteNonQueryAsync(string commandText,
        CancellationToken cancellationToken, params object[] parameters)
    {
        using var command = CreateCommand(commandText, parameters);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private Task<object[]> ExecuteRowAsync(string commandText,
        CancellationToken cancellationToken, params object[] parameters)
    {
        return ExecuteRowAsync(true, commandText, cancellationToken, parameters)!;
    }

    private async Task<object[]?> ExecuteRowAsync(bool mustFindRow, string commandText,
        CancellationToken cancellationToken, params object[] parameters)
    {
        using var command = CreateCommand(commandText, parameters);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            if (mustFindRow)
            {
                throw new RowNotFoundException("Row not found");
            }
            return null;
        }
        var result = new object[reader.FieldCount];
        for (var i = 0; i < reader.FieldCount; i++)
        {
            result[i] =  reader.GetValue(i);
        }
        return result;
    }

    private async Task<List<object[]>> ExecuteQueryAsync(string commandText,
        CancellationToken cancellationToken, params object[] parameters)
    {
        using var command = CreateCommand(commandText, parameters);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<object[]>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new object[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[i] = reader.GetValue(i);
            }
            result.Add(row);
        }
        return result;
    }

    private DbCommand CreateCommand(string commandText, params object[] parameters)
    {
        var command = _connection.CreateCommand();
        command.CommandText = commandText;
        command.Transaction = _currentTransaction;
        for (var i = 0; i < parameters.Length; i++)
        {
            CreateParameter(command, $"p{i+1}", parameters[i]);
        }
        return command;
    }

    private void CreateParameter(DbCommand cmd, string parameterName, object parameterValue)
    {
        var parameter = cmd.CreateParameter();
        parameter.ParameterName = parameterName;
        if (_isClientLib)
        {
            var value = parameterValue;
            if (value is decimal d)
            {
                value = PgNumeric.FromDecimal(d);
            }
            parameter.Value = value;
        }
        else
        {
            parameter.Value = parameterValue;
        }
        cmd.Parameters.Add(parameter);
    }
    
    private long GetOtherWarehouseId(long currentId) {
        if (_numWarehouses == 1) {
            return currentId;
        }
        while (true) {
            var otherId = DataLoader.ReverseBitsUnsigned((ulong)Random.Shared.Next(_numWarehouses));
            if (otherId != currentId) {
                return otherId;
            }
        }
    }
}
