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
            arguments = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        }
        else if (addressType == AddressType.Tcp)
        {
            arguments = "localhost:0 tcp";
        }
        else
        {
            arguments = "localhost:0 tcp";
        }

        var binaryFileName = GetBinaryFileName();
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

    internal static string GetBinaryFileName()
    {
        var searchDirectories = GetDefaultSearchDirectories(typeof(Server).Assembly.Location, AppContext.BaseDirectory);
        return GetBinaryFileName(searchDirectories, GetRuntimeIdentifier());
    }

    internal static string GetBinaryFileName(IEnumerable<string?> searchDirectories, string? runtimeIdentifier)
    {
        if (searchDirectories == null)
        {
            throw new ArgumentNullException(nameof(searchDirectories));
        }
        var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
        var directories = NormalizeSearchDirectories(searchDirectories, isWindows);
        var executableName = isWindows ? $"{BaseFileName}.exe" : BaseFileName;

        var candidateRelativePaths = new List<string>(3);
        if (!string.IsNullOrWhiteSpace(runtimeIdentifier))
        {
            ValidateRuntimeIdentifier(runtimeIdentifier!);
            candidateRelativePaths.Add(Path.Combine("runtimes", runtimeIdentifier, "native", executableName));
        }
        candidateRelativePaths.Add(Path.Combine("runtimes", "any", "native", executableName));
        candidateRelativePaths.Add(executableName);

        var tried = new List<string>();
        foreach (var relativePath in candidateRelativePaths)
        {
            foreach (var directory in directories)
            {
                var candidatePath = Path.Combine(directory, relativePath);
                if (TryExists(candidatePath, tried))
                {
                    return candidatePath;
                }
            }
        }

        throw new PlatformNotSupportedException("Could not find gRPC server executable for SpannerLib. Tried: " + string.Join("\n", tried));
    }

    private static void ValidateRuntimeIdentifier(string runtimeIdentifier)
    {
        if (Path.IsPathRooted(runtimeIdentifier)
            || runtimeIdentifier.IndexOf(Path.DirectorySeparatorChar) >= 0
            || runtimeIdentifier.IndexOf(Path.AltDirectorySeparatorChar) >= 0
            || runtimeIdentifier.Contains(".."))
        {
            throw new ArgumentException($"Invalid runtime identifier: {runtimeIdentifier}", nameof(runtimeIdentifier));
        }
    }

    internal static IReadOnlyList<string> GetDefaultSearchDirectories(string? assemblyLocation, string? baseDirectory)
    {
        var directories = new List<string>(2);
        if (!string.IsNullOrWhiteSpace(assemblyLocation))
        {
            var assemblyDirectory = Path.GetDirectoryName(assemblyLocation);
            if (!string.IsNullOrWhiteSpace(assemblyDirectory))
            {
                directories.Add(assemblyDirectory);
            }
        }
        if (!string.IsNullOrWhiteSpace(baseDirectory))
        {
            directories.Add(baseDirectory);
        }
        return directories;
    }

    internal static IReadOnlyList<string> NormalizeSearchDirectories(IEnumerable<string?> searchDirectories, bool isWindows)
    {
        var result = new List<string>();
        var seen = new HashSet<string>(isWindows ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
        foreach (var directory in searchDirectories)
        {
            // Never allow empty or relative/drive-relative paths, which would resolve against CWD.
            if (string.IsNullOrWhiteSpace(directory) || !Path.IsPathFullyQualified(directory))
            {
                continue;
            }
            var normalized = TrimEndingDirectorySeparators(Path.GetFullPath(directory));
            if (seen.Add(normalized))
            {
                result.Add(normalized);
            }
        }
        return result;
    }

    private static string TrimEndingDirectorySeparators(string path)
    {
        var rootLength = Path.GetPathRoot(path)?.Length ?? 0;
        var end = path.Length;
        while (end > rootLength && (path[end - 1] == Path.DirectorySeparatorChar || path[end - 1] == Path.AltDirectorySeparatorChar))
        {
            end--;
        }
        return end == path.Length ? path : path.Substring(0, end);
    }

    internal static string? GetRuntimeIdentifier()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            switch (RuntimeInformation.OSArchitecture)
            {
                case Architecture.X64:
                    return "win-x64";
            }
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            switch (RuntimeInformation.OSArchitecture)
            {
                case Architecture.X64:
                    return "linux-x64";
                case Architecture.Arm64:
                    return "linux-arm64";
            }
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            switch (RuntimeInformation.ProcessArchitecture)
            {
                case Architecture.Arm64:
                    return "osx-arm64";
            }
        }
        return null;
    }

    private static bool TryExists(string fileName, List<string> tried)
    {
        if (!Path.IsPathFullyQualified(fileName))
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