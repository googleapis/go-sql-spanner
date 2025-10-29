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
        
    protected override DbConnection? DbConnection => _spannerConnection;
    public override IsolationLevel IsolationLevel { get; }
    private SpannerLib.Connection LibConnection { get; }
    
    internal bool IsCompleted => _spannerConnection == null;
        
    private bool _disposed;

    internal SpannerTransaction(SpannerConnection connection, TransactionOptions options)
    {
        _spannerConnection = connection;
        IsolationLevel = TranslateIsolationLevel(options.IsolationLevel);
        LibConnection = connection.LibConnection!;
        LibConnection.BeginTransaction(options);
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

    protected override void Dispose(bool disposing)
    {
        if (!IsCompleted)
        {
            // Do a shoot-and-forget rollback.
            RollbackAsync(CancellationToken.None);
        }
        _disposed = true;
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync()
    {
        if (!IsCompleted)
        {
            // Do a shoot-and-forget rollback.
            RollbackAsync(CancellationToken.None);
        }
        _disposed = true;
        return base.DisposeAsync();
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
            return endTransactionMethod();
        }
        finally 
        {
            _spannerConnection?.ClearTransaction();
            _spannerConnection = null;
        }
    }
}