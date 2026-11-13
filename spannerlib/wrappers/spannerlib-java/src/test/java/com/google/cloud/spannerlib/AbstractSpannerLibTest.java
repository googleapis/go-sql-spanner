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

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractMockServerTest;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
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
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "select option_value from information_schema.database_options"
                    + " where option_name='database_dialect'"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    Field.newBuilder()
                                        .setName("option_value")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                        .build())
                                .build())
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("GOOGLE_STANDARD_SQL").build())
                        .build())
                .build()));
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
