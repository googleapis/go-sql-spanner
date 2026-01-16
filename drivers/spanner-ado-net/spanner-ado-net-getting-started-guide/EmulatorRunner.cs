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

using System.Net.Sockets;

namespace Google.Cloud.Spanner.DataProvider;

using Docker.DotNet;
using Docker.DotNet.Models;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// This class can be used to programmatically start and stop an instance of the Cloud Spanner emulator.
/// </summary>
internal class EmulatorRunner
{
    private static readonly string SEmulatorImageName = "gcr.io/cloud-spanner-emulator/emulator";
    private readonly DockerClient _dockerClient;
    private string? _containerId;

    internal static DockerClient CreateDockerClient()
    {
        return new DockerClientConfiguration(new Uri(GetDockerApiUri())).CreateClient();
    }

    internal EmulatorRunner()
    {
        _dockerClient = CreateDockerClient();
    }

    internal static bool IsEmulatorRunning()
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            socket.Connect("localhost", 9010);
        }
        catch (SocketException ex)
        {
            if (ex.SocketErrorCode == SocketError.ConnectionRefused)
            {
                return false;
            }
            throw;
        }
        return true;
    }

    /// <summary>
    /// Downloads the latest Spanner emulator docker image and starts the emulator on port 9010.
    /// </summary>
    internal async Task<PortBinding> StartEmulator()
    {
        await PullEmulatorImage();
        var response = await _dockerClient.Containers.CreateContainerAsync(new CreateContainerParameters
        {
            Image = SEmulatorImageName,
            ExposedPorts = new Dictionary<string, EmptyStruct>
            {
                {
                    "9010", default
                }
            },
            HostConfig = new HostConfig
            {
                PortBindings = new Dictionary<string, IList<PortBinding>?>
                {
                    {"9010", null}
                },
            }
        });
        _containerId = response.ID;
        await _dockerClient.Containers.StartContainerAsync(_containerId, null);
        var inspectResponse = await _dockerClient.Containers.InspectContainerAsync(_containerId);
        Thread.Sleep(500);
        return inspectResponse.NetworkSettings.Ports["9010/tcp"][0];
    }

    /// <summary>
    /// Stops the currently running emulator. Fails if no emulator has been started.
    /// </summary>
    internal async Task StopEmulator()
    {
        if (_containerId != null)
        {
            await _dockerClient.Containers.KillContainerAsync(_containerId, new ContainerKillParameters());
            await _dockerClient.Containers.RemoveContainerAsync(_containerId, new ContainerRemoveParameters());
        }
    }

    private async Task PullEmulatorImage()
    {
        await _dockerClient.Images.CreateImageAsync(new ImagesCreateParameters
        {
            FromImage = SEmulatorImageName,
            Tag = "latest"
        }, new AuthConfig(), new Progress<JSONMessage>());
    }

    private static string GetDockerApiUri()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return "npipe://./pipe/docker_engine";
        }
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return "unix:/var/run/docker.sock";
        }
        throw new Exception("Was unable to determine what OS this is running on, does not appear to be Windows or Linux!?");
    }
}
