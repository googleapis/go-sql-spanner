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
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace Google.Cloud.SpannerLib.Grpc;

public class Server : IDisposable
{
    public enum AddressType
    {
        UnixDomainSocket,
        Tcp,
    }
    
    private Process? _process;
    private string? _host;
    private bool _disposed;
    
    public bool IsRunning => _process is { HasExited: false };

    public Server()
    {
    }

    public string Start(AddressType addressType = AddressType.UnixDomainSocket)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(Server));
        }
        if (IsRunning)
        {
            throw new InvalidOperationException("The server is already started.");
        }
        (_host, _process) = StartGrpcServer(addressType, TimeSpan.FromSeconds(5));
        return _host;
    }
    
    private static Tuple<string, Process> StartGrpcServer(AddressType addressType, TimeSpan timeout)
    {
        string arguments;
        if (addressType == AddressType.UnixDomainSocket)
        {
            // Generate a random temp file name that will be used for the Unix domain socket communication.
            arguments = Path.GetTempPath() + Guid.NewGuid();
        }
        else if (addressType == AddressType.Tcp)
        {
            arguments = "localhost:0 tcp";
        }
        else
        {
            arguments = "localhost:0 tcp";
        }

        var binaryFileName = GetBinaryFileName().Replace('/', Path.DirectorySeparatorChar);
        var info = new ProcessStartInfo
        {
            Arguments = arguments,
            UseShellExecute = false,
            FileName = binaryFileName,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        // Start the process as a child process. The process will automatically stop when the
        // parent process stops.
        var process = Process.Start(info);
        if (process == null)
        {
            throw new InvalidOperationException("Failed to start spanner");
        }
        if (addressType == AddressType.UnixDomainSocket)
        {
            var watch = new Stopwatch();
            while (!File.Exists(arguments))
            {
                if (watch.Elapsed > timeout)
                {
                    throw new TimeoutException($"Attempt to start gRPC server timed out after {timeout}");
                }
                Thread.Sleep(1);
            }
        }
        if (addressType == AddressType.UnixDomainSocket)
        {
            // Return the name of the Unix domain socket.
            return Tuple.Create(arguments, process);
        }
        // Read the dynamically assigned port.
        var address = process.StandardError.ReadLine();
        if (address?.Contains("Starting gRPC server on") ?? false)
        {
            var lastSpace = address.LastIndexOf(" ",  StringComparison.Ordinal);
            return Tuple.Create(address.Substring(lastSpace + 1), process);
        }
        throw new InvalidOperationException("Failed to read gRPC address");
    }
    
    private static string GetBinaryFileName()
    {
        string? fileName = null;
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            switch (RuntimeInformation.OSArchitecture)
            {
                case Architecture.X64:
                    fileName = "runtimes/win-x64/native/grpc_server.exe";
                    break;
            }
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            switch (RuntimeInformation.OSArchitecture)
            {
                case Architecture.X64:
                    fileName = "runtimes/linux-x64/native/grpc_server";
                    break;
            }
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            switch (RuntimeInformation.ProcessArchitecture)
            {
                case Architecture.Arm64:
                    fileName = "runtimes/osx-arm64/native/grpc_server";
                    break;
            }
        }
        if (fileName != null && File.Exists(fileName))
        {
            return fileName;
        }
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            if (File.Exists("runtimes/any/native/grpc_server.exe"))
            {
                return "runtimes/any/native/grpc_server.exe";
            }
        }
        if (File.Exists("runtimes/any/native/grpc_server"))
        {
            return "runtimes/any/native/grpc_server";
        }
        
        throw new PlatformNotSupportedException();
    }
    
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public void Stop()
    {
        if (_process == null || _process.HasExited)
        {
            return;
        }
        _process.Kill();
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }
        try
        {
            Stop();
            _process?.Dispose();
        }
        finally
        {
            _disposed = true;
        }
    }    
}