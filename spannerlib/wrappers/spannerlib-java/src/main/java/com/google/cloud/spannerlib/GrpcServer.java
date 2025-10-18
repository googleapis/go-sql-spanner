/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spannerlib;

import com.sun.jna.SpannerLibPlatformDetector;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Scanner;

/** This class starts a gRPC server containing SpannerLib as a child process. */
class GrpcServer {
  private Process process;

  GrpcServer() throws IOException {}

  /** Starts the SpannerLib gRPC server as a child process and returns the address of the server. */
  String start() throws IOException {
    String prefix = SpannerLibPlatformDetector.getNativeLibraryResourcePrefix();
    String name = String.format("%s/grpc_server", prefix);
    URL resource = ClassLoader.getSystemClassLoader().getResource(name);
    if (resource == null) {
      throw new IOException(String.format("Unable to find %s", name));
    }
    String file = resource.getFile();
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.command(file, "localhost:0", "tcp");
    processBuilder.redirectErrorStream(true);
    process = processBuilder.start();
    try (Scanner scanner = new Scanner(new InputStreamReader(process.getInputStream()))) {
      // Read the first line that the gRPC server prints out.
      String log = scanner.nextLine();
      if (log.contains("Starting gRPC server on")) {
        int lastSpace = log.lastIndexOf(" ");
        return log.substring(lastSpace + 1);
      } else {
        throw new RuntimeException("Failed to read gRPC address");
      }
    }
  }

  /** Stops the child process. */
  void stop() {
    if (process != null && process.isAlive()) {
      process.destroy();
      process = null;
    }
  }
}
