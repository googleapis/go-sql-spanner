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

import com.google.cloud.spanner.connection.AbstractMockServerTest;
import com.google.common.collect.ImmutableList;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class AbstractSpannerLibTest extends AbstractMockServerTest {
  public enum LibraryType {
    SHARED,
    GRPC,
  }

  private static GrpcServer grpcServer;
  private static String grpcServerAddress;
  protected static SpannerLibrary library;

  @BeforeClass
  public static void startGrpcServer() throws Exception {
    grpcServer = new GrpcServer();
    grpcServerAddress = grpcServer.start();
  }

  @AfterClass
  public static void cleanupSpannerLibrary() throws Exception {
    if (library != null) {
      library.close();
    }
    if (grpcServer != null) {
      grpcServer.stop();
    }
  }

  @Parameters(name = "library = {0}")
  public static Collection<Object> parameters() {
    return ImmutableList.copyOf(PoolTest.LibraryType.values());
  }

  @Parameter public LibraryType libraryType;

  @Before
  public void maybeCreateLibrary() throws Exception {
    if (libraryType == PoolTest.LibraryType.GRPC && library instanceof NativeSpannerLibraryImpl) {
      library.close();
      library = null;
    } else if (libraryType == PoolTest.LibraryType.SHARED
        && library instanceof GrpcSpannerLibraryImpl) {
      library.close();
      library = null;
    }
    if (library == null) {
      library = createLibrary();
    }
  }

  private SpannerLibrary createLibrary() {
    if (libraryType == PoolTest.LibraryType.GRPC) {
      int numChannels = 20;
      List<Channel> channels = new ArrayList<>(numChannels);
      for (int i = 0; i < numChannels; i++) {
        channels.add(ManagedChannelBuilder.forTarget(grpcServerAddress).usePlaintext().build());
      }
      return new GrpcSpannerLibraryImpl(channels);

      //      ManagedChannel channel =
      //          ManagedChannelBuilder.forTarget(grpcServerAddress).usePlaintext().build();
      //      return new GrpcSpannerLibraryImpl(channel, true);
    } else if (libraryType == PoolTest.LibraryType.SHARED) {
      return NativeSpannerLibraryImpl.getInstance();
    } else {
      throw new UnsupportedOperationException("Unsupported library type: " + libraryType);
    }
  }

  @After
  public void cleanup() {
    mockSpanner.removeAllExecutionTimes();
  }
}
