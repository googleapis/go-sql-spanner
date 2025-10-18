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

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Google.Cloud.SpannerLib.MockServer;

/// <summary>
/// Helper class for starting an in-memory mock Spanner server that is used for testing.
/// </summary>
public class MockServerStartup(MockSpannerService mockSpannerService, MockDatabaseAdminService mockDatabaseAdminService)
{
    /// <summary>
    /// The in-mem Spanner service.
    /// </summary>
    private MockSpannerService MockSpannerService { get; } = mockSpannerService;

    /// <summary>
    /// The in-mem Spanner database admin service for executing DDL operations.
    /// </summary>
    private MockDatabaseAdminService MockDatabaseAdminService { get; } = mockDatabaseAdminService;

    /// <summary>
    /// Configures the services that will be available on this gRPC server.
    /// This method is called by reflection when the tests are started.
    /// </summary>
    /// <param name="services">The services collection where the services should be added</param>
    public void ConfigureServices(IServiceCollection services)
    {
        services.AddGrpc();
        services.AddSingleton(MockSpannerService);
        services.AddSingleton(MockDatabaseAdminService);
    }

    /// <summary>
    /// Configures the gRPC server. This method is called by reflection when the tests are started.
    /// </summary>
    /// <param name="app">The builder for the application that will be hosting the service</param>
    /// <param name="env">The webhost environment that is hosting the service</param>
    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }

        app.UseRouting();
        app.UseEndpoints(endpoints =>
        {
            endpoints.MapGrpcService<MockSpannerService>();
            endpoints.MapGrpcService<MockDatabaseAdminService>();
        });
    }
}