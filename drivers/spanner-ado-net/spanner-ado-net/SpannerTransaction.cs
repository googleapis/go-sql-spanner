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

using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.Spanner.DataProvider;

public class SpannerTransaction : DbTransaction
{
    private SpannerConnection? _spannerConnection;

    protected override DbConnection? DbConnection
    {
        get
        {
            CheckDisposed();
            return _spannerConnection;
        }
    }
    public override IsolationLevel IsolationLevel { get; }

    private string? _tag;
    public string? Tag
    {
        get => _tag;
        set
        {
            GaxPreconditions.CheckState(!_used, "Tag cannot be changed once the transaction has been used");
            _tag = value;
        }
    }

    // TODO: Implement savepoint support in the shared library.
    public override bool SupportsSavepoints => false;

    private SpannerLib.Connection LibConnection { get; }

    private bool _used;
    
    internal bool IsCompleted => _spannerConnection == null;
    
    internal bool IsDisposed => _disposed;
        
    private bool _disposed;

    internal static SpannerTransaction CreateTransaction(
        SpannerConnection connection, SpannerLib.Connection libConnection, TransactionOptions options)
    {
        // This call to BeginTransaction does not trigger an RPC. It only registers the transaction on the connection.
        libConnection.BeginTransaction(options);
        return new SpannerTransaction(connection, libConnection, options);
    }

    internal static async ValueTask<SpannerTransaction> CreateTransactionAsync(
        SpannerConnection connection,
        SpannerLib.Connection libConnection,
        TransactionOptions options,
        CancellationToken cancellationToken)
    {
        // This call to BeginTransaction does not trigger an RPC. It only registers the transaction on the connection.
        await libConnection.BeginTransactionAsync(options,  cancellationToken).ConfigureAwait(false);
        return new SpannerTransaction(connection, libConnection, options);
    }

    private SpannerTransaction(SpannerConnection connection, SpannerLib.Connection libConnection, TransactionOptions options)
    {
        _spannerConnection = connection;
        IsolationLevel = TranslateIsolationLevel(options.IsolationLevel);
        LibConnection = libConnection;
    }

    internal static TransactionOptions.Types.IsolationLevel TranslateIsolationLevel(IsolationLevel isolationLevel)
    {
        return isolationLevel switch
        {
            IsolationLevel.Chaos => throw new NotSupportedException(),
            IsolationLevel.ReadUncommitted  => throw new NotSupportedException(),
            IsolationLevel.ReadCommitted  => throw new NotSupportedException(),
            IsolationLevel.RepeatableRead => TransactionOptions.Types.IsolationLevel.RepeatableRead,
            IsolationLevel.Snapshot => TransactionOptions.Types.IsolationLevel.RepeatableRead,
            IsolationLevel.Serializable => TransactionOptions.Types.IsolationLevel.Serializable,
            _ => TransactionOptions.Types.IsolationLevel.Unspecified
        };
    }

    private static IsolationLevel TranslateIsolationLevel(TransactionOptions.Types.IsolationLevel isolationLevel)
    {
        switch (isolationLevel)
        {
            case TransactionOptions.Types.IsolationLevel.Unspecified:
                return IsolationLevel.Unspecified;
            case TransactionOptions.Types.IsolationLevel.RepeatableRead:
                return IsolationLevel.RepeatableRead;
            case TransactionOptions.Types.IsolationLevel.Serializable:
                return IsolationLevel.Serializable;
            default:
                throw new ArgumentOutOfRangeException(nameof(isolationLevel), isolationLevel,
                    "unsupported isolation level");
        }
    }

    internal void MarkUsed()
    {
        _used = true;
    }

    protected override void Dispose(bool disposing)
    {
        if (!IsCompleted)
        {
            // Do a shoot-and-forget rollback.
            Rollback();
        }
        _disposed = true;
    }

    public override async ValueTask DisposeAsync()
    {
        if (!IsCompleted)
        {
            // Do a shoot-and-forget rollback.
            await RollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }
        _disposed = true;
    }

    private void CheckDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    public override void Commit()
    {
        EndTransaction(() => LibConnection.Commit());
    }

    public override Task CommitAsync(CancellationToken cancellationToken = default)
    {
        return EndTransactionAsync(() => LibConnection.CommitAsync(cancellationToken));
    }

    public override void Rollback()
    {
        EndTransaction(() => LibConnection.Rollback());
    }

    public override Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        return EndTransactionAsync(() => LibConnection.RollbackAsync(cancellationToken));
    }
        
    private void EndTransaction(Action endTransactionMethod)
    {
        CheckDisposed();
        GaxPreconditions.CheckState(!IsCompleted, "This transaction is no longer active");
        try
        {
            endTransactionMethod();
        }
        finally 
        {
            _spannerConnection?.ClearTransaction();
            _spannerConnection = null;
        }
    }

    private Task EndTransactionAsync(Func<Task> endTransactionMethod)
    {
        CheckDisposed();
        GaxPreconditions.CheckState(!IsCompleted, "This transaction is no longer active");
        try
        {
            return SpannerDbException.TranslateException(endTransactionMethod());
        }
        finally
        {
            _spannerConnection?.ClearTransaction();
            _spannerConnection = null;
        }
    }
}