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

using Google.Cloud.SpannerLib.V1;

namespace Google.Cloud.SpannerLib.Grpc;

public class Tests
{
    private Server _server;
    
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
        
        _server.Stop();
        Assert.That(_server.IsRunning, Is.False);
    }
}