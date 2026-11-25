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

using System.Diagnostics;
using System.Runtime.InteropServices;
using Google.Cloud.SpannerLib.V1;

namespace Google.Cloud.SpannerLib.Grpc;

public class Tests
{
    private const string BaseExecutableName = "spannerlib_grpc_server";

    private Server _server = null!;
    
    [SetUp]
    public void Setup()
    {
        _server = new Server();
    }

    [TearDown]
    public void TearDown()
    {
        _server.Dispose();
    }

    [Test]
    public void TestStartStop()
    {
        Assert.That(_server.IsRunning, Is.False);
        var file = _server.Start();
        Assert.That(_server.IsRunning, Is.True);
        
        using var channel = GrpcLibSpanner.ForUnixSocket(file);
        var client = new V1.SpannerLib.SpannerLibClient(channel);
        var info = client.Info(new InfoRequest());
        Assert.That(info, Is.Not.Null);

        StopServerAndWait();
        Assert.That(_server.IsRunning, Is.False);
    }

    [Test]
    [NonParallelizable]
    public void TestGetBinaryFileName_IgnoresCurrentWorkingDirectoryDecoy()
    {
        WithDecoyCurrentDirectory(markerFilePath: null, decoyDirectory =>
        {
            var binaryFileName = Server.GetBinaryFileName();
            Assert.That(Path.IsPathFullyQualified(binaryFileName), Is.True);
            Assert.That(File.Exists(binaryFileName), Is.True);
            Assert.That(IsPathWithinDirectory(binaryFileName, decoyDirectory), Is.False);
        });
    }

    [Test]
    [NonParallelizable]
    public void TestStart_IgnoresCurrentWorkingDirectoryDecoy()
    {
        var markerDirectory = CreateTemporaryDirectory();
        var markerFilePath = Path.Combine(markerDirectory, "executed.marker");
        try
        {
            WithDecoyCurrentDirectory(markerFilePath, _ =>
            {
                try
                {
                    var socketAddress = _server.Start();
                    Assert.That(_server.IsRunning, Is.True);
                    Assert.That(File.Exists(markerFilePath), Is.False);

                    using var channel = GrpcLibSpanner.ForUnixSocket(socketAddress);
                    var client = new V1.SpannerLib.SpannerLibClient(channel);
                    var info = client.Info(new InfoRequest());
                    Assert.That(info, Is.Not.Null);
                }
                finally
                {
                    StopServerAndWait();
                }
            });
        }
        finally
        {
            DeleteDirectoryIfExists(markerDirectory);
        }
    }

    private void StopServerAndWait()
    {
        _server.Stop();
        var watch = Stopwatch.StartNew();
        // Wait for the child process to fully exit and release handles (e.g. CWD lock on Windows).
        while (_server.IsRunning && watch.ElapsedMilliseconds < 5000)
        {
            Thread.Sleep(10);
        }
    }

    [Test]
    [NonParallelizable]
    public void TestGetBinaryFileName_IgnoresEmptyAndRelativeSearchDirectories()
    {
        var emptyDirectory = CreateTemporaryDirectory();
        try
        {
            WithDecoyCurrentDirectory(markerFilePath: null, decoyDirectory =>
            {
                var searchDirectories = new string?[]
                {
                    null,
                    string.Empty,
                    "   ",
                    ".",
                    "..",
                    "runtimes",
                    "C:relative",
                    "\\root-relative",
                    emptyDirectory,
                };

                var exception = Assert.Throws<PlatformNotSupportedException>(
                    () => Server.GetBinaryFileName(searchDirectories, "custom-rid"))!;
                Assert.That(exception.Message, Does.Contain(emptyDirectory));
                Assert.That(exception.Message, Does.Not.Contain(decoyDirectory));

                var triedPaths = ExtractTriedPaths(exception.Message);
                Assert.That(triedPaths, Is.Not.Empty);
                foreach (var triedPath in triedPaths)
                {
                    Assert.That(Path.IsPathFullyQualified(triedPath), Is.True, $"Expected fully-qualified path, got: {triedPath}");
                    Assert.That(IsPathWithinDirectory(triedPath, emptyDirectory), Is.True, $"Unexpected search outside trusted directory: {triedPath}");
                }
            });
        }
        finally
        {
            DeleteDirectoryIfExists(emptyDirectory);
        }
    }

    [Test]
    [NonParallelizable]
    public void TestGetBinaryFileName_PrefersRuntimeSpecificOverAnyAndFlat()
    {
        var trustedDirectory = CreateTemporaryDirectory();
        try
        {
            WithDecoyCurrentDirectory(markerFilePath: null, _ =>
            {
                var executableName = GetExecutableName();
                var flatPath = CreateDummyFile(Path.Combine(trustedDirectory, executableName));
                var anyPath = CreateDummyFile(Path.Combine(trustedDirectory, "runtimes", "any", "native", executableName));
                var runtimeSpecificPath = CreateDummyFile(Path.Combine(trustedDirectory, "runtimes", "custom-rid", "native", executableName));

                var resolvedPath = Server.GetBinaryFileName([trustedDirectory], "custom-rid");
                Assert.That(resolvedPath, Is.EqualTo(runtimeSpecificPath));
                Assert.That(resolvedPath, Is.Not.EqualTo(anyPath));
                Assert.That(resolvedPath, Is.Not.EqualTo(flatPath));
            });
        }
        finally
        {
            DeleteDirectoryIfExists(trustedDirectory);
        }
    }

    [Test]
    public void TestGetRuntimeIdentifier_MatchesHostPlatform()
    {
        var hostRuntimeIdentifier = Server.GetRuntimeIdentifier();
        if (OperatingSystem.IsMacOS() && RuntimeInformation.ProcessArchitecture == Architecture.Arm64)
        {
            Assert.That(hostRuntimeIdentifier, Is.EqualTo("osx-arm64"));
        }
        else if (OperatingSystem.IsLinux() && RuntimeInformation.OSArchitecture == Architecture.X64)
        {
            Assert.That(hostRuntimeIdentifier, Is.EqualTo("linux-x64"));
        }
        else if (OperatingSystem.IsLinux() && RuntimeInformation.OSArchitecture == Architecture.Arm64)
        {
            Assert.That(hostRuntimeIdentifier, Is.EqualTo("linux-arm64"));
        }
        else if (OperatingSystem.IsWindows() && RuntimeInformation.OSArchitecture == Architecture.X64)
        {
            Assert.That(hostRuntimeIdentifier, Is.EqualTo("win-x64"));
        }
    }

    [Test]
    [NonParallelizable]
    public void TestGetBinaryFileName_PrefersAnyOverFlat()
    {
        var trustedDirectory = CreateTemporaryDirectory();
        try
        {
            WithDecoyCurrentDirectory(markerFilePath: null, _ =>
            {
                var executableName = GetExecutableName();
                var flatPath = CreateDummyFile(Path.Combine(trustedDirectory, executableName));
                var anyPath = CreateDummyFile(Path.Combine(trustedDirectory, "runtimes", "any", "native", executableName));

                var resolvedPath = Server.GetBinaryFileName([trustedDirectory], "custom-rid");
                Assert.That(resolvedPath, Is.EqualTo(anyPath));
                Assert.That(resolvedPath, Is.Not.EqualTo(flatPath));
            });
        }
        finally
        {
            DeleteDirectoryIfExists(trustedDirectory);
        }
    }

    [Test]
    [NonParallelizable]
    public void TestGetBinaryFileName_FallsBackToFlat()
    {
        var flatOnlyDirectory = CreateTemporaryDirectory();
        try
        {
            WithDecoyCurrentDirectory(markerFilePath: null, _ =>
            {
                var executableName = GetExecutableName();
                var flatPath = CreateDummyFile(Path.Combine(flatOnlyDirectory, executableName));
                var resolvedFlatPath = Server.GetBinaryFileName([flatOnlyDirectory], "custom-rid");
                Assert.That(resolvedFlatPath, Is.EqualTo(flatPath));
            });
        }
        finally
        {
            DeleteDirectoryIfExists(flatOnlyDirectory);
        }
    }

    [Test]
    [NonParallelizable]
    public void TestGetBinaryFileName_HandlesNullRuntimeIdentifier()
    {
        var anyAndFlatDirectory = CreateTemporaryDirectory();
        try
        {
            WithDecoyCurrentDirectory(markerFilePath: null, _ =>
            {
                var executableName = GetExecutableName();
                CreateDummyFile(Path.Combine(anyAndFlatDirectory, executableName));
                CreateDummyFile(Path.Combine(anyAndFlatDirectory, "runtimes", "custom-rid", "native", executableName));
                var anyPath = CreateDummyFile(Path.Combine(anyAndFlatDirectory, "runtimes", "any", "native", executableName));

                var resolvedWithNullRuntimeIdentifier = Server.GetBinaryFileName([anyAndFlatDirectory], runtimeIdentifier: null);
                Assert.That(resolvedWithNullRuntimeIdentifier, Is.EqualTo(anyPath));
            });
        }
        finally
        {
            DeleteDirectoryIfExists(anyAndFlatDirectory);
        }
    }

    [Test]
    public void TestGetBinaryFileName_MultipleSearchDirectoriesPrecedence()
    {
        var primaryDirectory = CreateTemporaryDirectory();
        var secondaryDirectory = CreateTemporaryDirectory();
        try
        {
            var executableName = GetExecutableName();
            var primaryAnyPath = CreateDummyFile(Path.Combine(primaryDirectory, "runtimes", "any", "native", executableName));
            var secondaryAnyPath = CreateDummyFile(Path.Combine(secondaryDirectory, "runtimes", "any", "native", executableName));

            // At the same tier ('any'), primaryDirectory takes precedence over secondaryDirectory.
            var resolvedSameTier = Server.GetBinaryFileName([primaryDirectory, secondaryDirectory], "custom-rid");
            Assert.That(resolvedSameTier, Is.EqualTo(primaryAnyPath));
            Assert.That(resolvedSameTier, Is.Not.EqualTo(secondaryAnyPath));

            // Across tiers, RID-specific in secondaryDirectory takes precedence over 'any' in primaryDirectory.
            var secondaryRuntimeSpecificPath = CreateDummyFile(Path.Combine(secondaryDirectory, "runtimes", "custom-rid", "native", executableName));
            var resolvedCrossTier = Server.GetBinaryFileName([primaryDirectory, secondaryDirectory], "custom-rid");
            Assert.That(resolvedCrossTier, Is.EqualTo(secondaryRuntimeSpecificPath));
        }
        finally
        {
            DeleteDirectoryIfExists(primaryDirectory);
            DeleteDirectoryIfExists(secondaryDirectory);
        }
    }

    [Test]
    public void TestGetBinaryFileName_DeduplicatesSearchDirectories()
    {
        var emptyDirectory = CreateTemporaryDirectory();
        try
        {
            var directoryWithTrailingSeparator = emptyDirectory + Path.DirectorySeparatorChar + Path.DirectorySeparatorChar;
            var exception = Assert.Throws<PlatformNotSupportedException>(
                () => Server.GetBinaryFileName([emptyDirectory, directoryWithTrailingSeparator], "custom-rid"))!;

            var triedPaths = ExtractTriedPaths(exception.Message);
            Assert.That(triedPaths, Is.Unique);
        }
        finally
        {
            DeleteDirectoryIfExists(emptyDirectory);
        }
    }

    [Test]
    public void TestGetBinaryFileName_NullSearchDirectoriesThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => Server.GetBinaryFileName(null!, "custom-rid"));
    }

    [TestCase("../evil")]
    [TestCase("..\\evil")]
    [TestCase("win/x64")]
    [TestCase("/absolute")]
    public void TestGetBinaryFileName_InvalidRuntimeIdentifierThrowsArgumentException(string invalidRuntimeIdentifier)
    {
        var emptyDirectory = CreateTemporaryDirectory();
        try
        {
            Assert.Throws<ArgumentException>(() => Server.GetBinaryFileName([emptyDirectory], invalidRuntimeIdentifier));
        }
        finally
        {
            DeleteDirectoryIfExists(emptyDirectory);
        }
    }

    [Test]
    public void TestGetDefaultSearchDirectories_IncludesAssemblyAndBaseDirectories()
    {
        var assemblyLocation = Path.Combine(Path.GetTempPath(), "assembly-dir", "Server.dll");
        var baseDirectory = Path.Combine(Path.GetTempPath(), "base-dir");

        var bothDirectories = Server.GetDefaultSearchDirectories(assemblyLocation, baseDirectory);
        Assert.That(bothDirectories, Is.EqualTo(new[] { Path.GetDirectoryName(assemblyLocation)!, baseDirectory }));

        // Single-file publish sets Assembly.Location to empty string; AppContext.BaseDirectory must still be used.
        var singleFileDirectories = Server.GetDefaultSearchDirectories(string.Empty, baseDirectory);
        Assert.That(singleFileDirectories, Is.EqualTo(new[] { baseDirectory }));

        var emptyDirectories = Server.GetDefaultSearchDirectories(string.Empty, null);
        Assert.That(emptyDirectories, Is.Empty);
    }

    [Test]
    public void TestNormalizeSearchDirectories_FiltersRelativeAndDeduplicates()
    {
        var rootDirectory = Path.GetPathRoot(Path.GetTempPath())!;
        var sampleDirectory = Path.Combine(Path.GetTempPath(), "normalize-sample");
        var sampleWithTrailingSeparators = sampleDirectory + Path.DirectorySeparatorChar + Path.DirectorySeparatorChar;

        var normalizedUnix = Server.NormalizeSearchDirectories(
            [null, "", "   ", ".", "..", "relative/path", "C:relative", "\\root-relative", rootDirectory, sampleDirectory, sampleWithTrailingSeparators, sampleDirectory.ToUpperInvariant()],
            isWindows: false);

        Assert.That(normalizedUnix, Is.EqualTo(new[] { rootDirectory, sampleDirectory, sampleDirectory.ToUpperInvariant() }));

        var normalizedWindowsComparison = Server.NormalizeSearchDirectories(
            [sampleDirectory, sampleDirectory.ToUpperInvariant()],
            isWindows: true);
        Assert.That(normalizedWindowsComparison, Is.EqualTo(new[] { sampleDirectory }));
    }

    private static string GetExecutableName() =>
        OperatingSystem.IsWindows() ? $"{BaseExecutableName}.exe" : BaseExecutableName;

    private static string CreateTemporaryDirectory()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"spannerlib-server-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        return directory;
    }

    private static void DeleteDirectoryIfExists(string directory)
    {
        if (Directory.Exists(directory))
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static void WithDecoyCurrentDirectory(string? markerFilePath, Action<string> action)
    {
        var previousDirectory = Environment.CurrentDirectory;
        var decoyDirectory = CreateTemporaryDirectory();
        try
        {
            CreateDecoyBinaries(decoyDirectory, markerFilePath);
            Environment.CurrentDirectory = decoyDirectory;
            action(decoyDirectory);
        }
        finally
        {
            Environment.CurrentDirectory = previousDirectory;
            DeleteDirectoryIfExists(decoyDirectory);
        }
    }

    private static void CreateDecoyBinaries(string rootDirectory, string? markerFilePath)
    {
        var executableName = GetExecutableName();
        var candidatePaths = new[]
        {
            Path.Combine(rootDirectory, executableName),
            Path.Combine(rootDirectory, "runtimes", "any", "native", executableName),
            Path.Combine(rootDirectory, "runtimes", "win-x64", "native", executableName),
            Path.Combine(rootDirectory, "runtimes", "linux-x64", "native", executableName),
            Path.Combine(rootDirectory, "runtimes", "linux-arm64", "native", executableName),
            Path.Combine(rootDirectory, "runtimes", "osx-arm64", "native", executableName),
        };
        foreach (var candidatePath in candidatePaths)
        {
            CreateExecutableDecoy(candidatePath, markerFilePath);
        }
    }

    private static string CreateDummyFile(string filePath)
    {
        EnsureParentDirectoryExists(filePath);
        File.WriteAllText(filePath, string.Empty);
        return Path.GetFullPath(filePath);
    }

    private static void CreateExecutableDecoy(string filePath, string? markerFilePath)
    {
        EnsureParentDirectoryExists(filePath);
        if (OperatingSystem.IsWindows())
        {
            File.WriteAllText(filePath, string.Empty);
            return;
        }
        var scriptContent = markerFilePath != null
            ? $"#!/bin/sh\ntouch \"{markerFilePath}\"\n"
            : "#!/bin/sh\nexit 0\n";
        File.WriteAllText(filePath, scriptContent);
        File.SetUnixFileMode(
            filePath,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
    }

    private static void EnsureParentDirectoryExists(string filePath)
    {
        var parentDirectory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(parentDirectory))
        {
            Directory.CreateDirectory(parentDirectory);
        }
    }

    private static bool IsPathWithinDirectory(string filePath, string directoryPath)
    {
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        var fullFilePath = Path.GetFullPath(filePath);
        var fullDirectoryPath = Path.GetFullPath(directoryPath).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            + Path.DirectorySeparatorChar;
        return fullFilePath.StartsWith(fullDirectoryPath, comparison);
    }

    private static string[] ExtractTriedPaths(string exceptionMessage)
    {
        const string prefix = "Tried: ";
        var prefixIndex = exceptionMessage.IndexOf(prefix, StringComparison.Ordinal);
        if (prefixIndex < 0)
        {
            return [];
        }
        return exceptionMessage
            .Substring(prefixIndex + prefix.Length)
            .Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
    }
}