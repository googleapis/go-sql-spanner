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
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;

namespace Google.Cloud.SpannerLib.Grpc;

public class Server : IDisposable
{
    private const string BaseFileName = "spannerlib_grpc_server";
    
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

        // Start background Task to consume standard output stream to prevent blocking/hanging.
        DrainStream(process.StandardOutput);

        if (addressType == AddressType.UnixDomainSocket)
        {
            // Start background Task to consume standard error stream.
            DrainStream(process.StandardError);

            var watch = new Stopwatch();
            while (!File.Exists(arguments))
            {
                if (watch.Elapsed > timeout)
                {
                    throw new TimeoutException($"Attempt to start gRPC server timed out after {timeout}");
                }
                Thread.Sleep(1);
            }

            // Return the name of the Unix domain socket.
            return Tuple.Create(arguments, process);
        }
        // Read the dynamically assigned port.
        var address = process.StandardError.ReadLine();

        // Start background Task to consume remaining standard error stream.
        DrainStream(process.StandardError);

        if (address?.Contains("Starting gRPC server on") ?? false)
        {
            var lastSpace = address.LastIndexOf(" ",  StringComparison.Ordinal);
            return Tuple.Create(address.Substring(lastSpace + 1), process);
        }
        throw new InvalidOperationException("Failed to read gRPC address");
    }
    
    private static string GetBinaryFileName()
    {
        var tried = new List<string>();
        string? fileName = null;
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            switch (RuntimeInformation.OSArchitecture)
            {
                case Architecture.X64:
                    fileName = $"runtimes/win-x64/native/{BaseFileName}.exe";
                    break;
            }
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            switch (RuntimeInformation.OSArchitecture)
            {
                case Architecture.X64:
                    fileName = $"runtimes/linux-x64/native/{BaseFileName}";
                    break;
                case Architecture.Arm64:
                    fileName = $"runtimes/linux-arm64/native/{BaseFileName}";
                    break;
            }
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            switch (RuntimeInformation.ProcessArchitecture)
            {
                case Architecture.Arm64:
                    fileName = $"runtimes/osx-arm64/native/{BaseFileName}";
                    break;
            }
        }
        if (TryExists(fileName, tried))
        {
            return fileName!;
        }

        var executing = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ?? "";
        var code = Path.GetDirectoryName(typeof(Server).Assembly.Location) ?? "";
        var assemblyLocations = executing == code ? new [] {code} : [executing, code];
        foreach (var assemblyLocation in assemblyLocations)
        {
            if (fileName != null)
            {
                var combined = Path.Combine(assemblyLocation, fileName);
                if (TryExists(combined, tried))
                {
                    return combined;
                }
            }
        }

        const string anyArchFileNameWindows = $"runtimes/any/native/{BaseFileName}.exe";
        const string anyArchFileName = $"runtimes/any/native/{BaseFileName}";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            if (TryExists(anyArchFileNameWindows, tried))
            {
                return anyArchFileNameWindows;
            }
        }
        else
        {
            if (TryExists(anyArchFileName, tried))
            {
                return anyArchFileName;
            }
        }

        foreach (var assemblyLocation in assemblyLocations)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var combinedWindows = Path.Combine(assemblyLocation, anyArchFileNameWindows);
                if (TryExists(combinedWindows, tried))
                {
                    return combinedWindows;
                }
            }
            else
            {
                var combinedAnyArch = Path.Combine(assemblyLocation, anyArchFileName);
                if (TryExists(combinedAnyArch, tried))
                {
                    return combinedAnyArch;
                }
            }
        }

        throw new PlatformNotSupportedException("Could not find gRPC server executable for SpannerLib. Tried: " + string.Join("\n", tried));
    }

    private static bool TryExists(string? fileName, List<string> tried)
    {
        if (fileName == null)
        {
            return false;
        }
        tried.Add(fileName);
        return File.Exists(fileName);
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
            if (disposing)
            {
                try
                {
                    Stop();
                }
                catch (Exception)
                {
                    // Ignore exceptions during shutdown/dispose
                }
                _process?.Dispose();
            }
        }
        finally
        {
            _disposed = true;
        }
    }

    /// <summary>
    /// Asynchronously drains the redirected output or error stream of the child process.
    /// Redirecting standard streams without consuming them can cause the child process to block
    /// and hang indefinitely when the OS pipe buffer size limit (typically 64KB) is reached.
    /// This method uses non-blocking asynchronous reading (ReadAsync) with a shared buffer to continuously drain
    /// the stream without keeping a ThreadPool thread blocked/pinned and with zero memory allocation overhead,
    /// preventing thread pool starvation and GC pressure.
    /// </summary>
    private static void DrainStream(StreamReader reader)
    {
        Task.Run(async () =>
        {
            try
            {
                var buffer = new char[4096];
                while (await reader.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false) > 0)
                {
                }
            }
            catch
            {
                // Discard exceptions during shutdown/dispose
            }
        });
    }
}