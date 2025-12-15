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

using Google.Cloud.SpannerLib.MockServer;

namespace Google.Cloud.Spanner.DataProvider.Samples.Tests;

public abstract class AbstractMockServerTests
{
    static AbstractMockServerTests()
    {
        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
        {
            SpannerPool.CloseSpannerLib();
        };
    }
    
    protected SpannerMockServerFixture Fixture;
    
    protected SpannerDataSource DataSource { get; private set; }

    
    protected string ConnectionString =>  $"Host={Fixture.Host};Port={Fixture.Port};Data Source=projects/p1/instances/i1/databases/d1;UsePlainText=true";
    
    [OneTimeSetUp]
    public void Setup()
    {
        Fixture = new SpannerMockServerFixture();
        DataSource = SpannerDataSource.Create(ConnectionString);
    }
    
    [OneTimeTearDown]
    public void Teardown()
    {
        DataSource.Dispose();
        Fixture.Dispose();
    }

    [SetUp]
    public void SetupResults()
    {
        Fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
    }

    [TearDown]
    public void Reset()
    {
        Fixture.SpannerMock.Reset();
        Fixture.DatabaseAdminMock.Reset();
    }

    protected SpannerConnection OpenConnection()
    {
        var connection = new SpannerConnection(ConnectionString);
        connection.Open();
        return connection;
    }

    protected async Task<SpannerConnection> OpenConnectionAsync()
    {
        var connection = new SpannerConnection(ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    protected SpannerDataSource CreateDataSource()
    {
        return CreateDataSource(_ => { });
    }

    protected SpannerDataSource CreateDataSource(string connectionString)
    {
        return CreateDataSource(csb => { csb.ConnectionString = connectionString; });
    }
    
    protected SpannerDataSource CreateDataSource(Action<SpannerConnectionStringBuilder> connectionStringBuilderAction)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder(ConnectionString);
        connectionStringBuilderAction(connectionStringBuilder);
        return SpannerDataSource.Create(connectionStringBuilder);
    }

}
