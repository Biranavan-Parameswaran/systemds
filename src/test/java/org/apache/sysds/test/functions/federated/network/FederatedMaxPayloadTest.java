/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.functions.federated.network;

import org.apache.sysds.runtime.controlprogram.federated.FederatedData;
import org.apache.sysds.runtime.controlprogram.federated.FederatedRequest;
import org.apache.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FederatedMaxPayloadTest extends AutomatedTestBase {

	private final static String TEST_NAME = "FederatedMaxPayloadTest";
	private final static String TEST_DIR = "functions/federated/network/";
	private final static String TEST_CLASS_DIR = TEST_DIR + FederatedMaxPayloadTest.class.getSimpleName() + "/";

	// sweep bounds (override on the CLI, e.g. -DfedMaxPayload.endRows=9000). cols=30000 dense -> one row is
	// 240KB, so the default 8850..8950 bracket (raw 2.124..2.148GB) straddles the Integer.MAX_VALUE cliff.
	private final static int COLS = Integer.getInteger("fedMaxPayload.cols", 30000);
	private final static int START_ROWS = Integer.getInteger("fedMaxPayload.startRows", 8850);
	private final static int END_ROWS = Integer.getInteger("fedMaxPayload.endRows", 8950);
	private final static int STEPS = Integer.getInteger("fedMaxPayload.steps", 10);

	private final static Pattern WRITER_INDEX = Pattern.compile("writerIndex\\((\\d+)\\)");

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {""}));
	}

	/**
	 * Ramp a dense PUT_VAR payload up in STEPS increments until the legacy ObjectEncoder overflows its
	 * Integer.MAX_VALUE ByteBuf, and report the last size that passed and the first that crashed. That
	 * bracket is the real wire-size threshold to calibrate STREAM_THRESHOLD against.
	 */
	@Test
	public void testMaxNettyPayloadThresholdSweep() {
		int port = getRandomAvailablePort();
		startLocalFedWorkerThread(port, 10);
		InetSocketAddress address = new InetSocketAddress("localhost", port);

		log("==== FED MAX PAYLOAD SWEEP ====");
		log(String.format("rows %d..%d in %d steps (%d/step), cols=%d dense | maxCapacity=%,d | heap max=%s",
			START_ROWS, END_ROWS, STEPS, STEPS == 0 ? 0 : (END_ROWS - START_ROWS) / STEPS, COLS,
			Integer.MAX_VALUE, gib(Runtime.getRuntime().maxMemory())));
		log("worker started on port " + port);

		long lastPassBytes = -1;
		int lastPassRows = -1;
		long firstCrashBytes = -1;
		int firstCrashRows = -1;
		long crashWriterIndex = -1;

		try {
			for(int i = 0; i <= STEPS; i++) {
				int rows = START_ROWS + (int) ((long) (END_ROWS - START_ROWS) * i / STEPS);
				long rawBytes = (long) rows * COLS * 8;
				String sz = String.format("%d x %d dense = %,d raw bytes (%.4f GiB)", rows, COLS, rawBytes,
					rawBytes / (1024.0 * 1024 * 1024));
				long t0 = System.nanoTime();

				log(String.format(">>> step %d/%d: %s | %s", i, STEPS, sz, heap()));
				MatrixBlock mb = new MatrixBlock(rows, COLS, false);
				mb.allocateDenseBlock();
				mb.setNonZeros((long) rows * COLS);
				FederatedRequest request = new FederatedRequest(FederatedRequest.RequestType.PUT_VAR, 1, mb);
				log(String.format("    block allocated (%.1fs), sending PUT_VAR ... | %s", secs(t0), heap()));

				try {
					Future<FederatedResponse> f = FederatedData.executeFederatedOperation(address, request);
					FederatedResponse response = f.get();
					Assert.assertTrue("Network send was not successful @ " + sz, response.isSuccessful());
					log(String.format("[sweep %d] PASS  %s (round-trip %.1fs)", i, sz, secs(t0)));
					lastPassBytes = rawBytes;
					lastPassRows = rows;
				}
				catch(ExecutionException e) {
					String msg = (e.getMessage() != null ? e.getMessage() : "") + " | "
						+ (e.getCause() != null && e.getCause().getMessage() != null ? e.getCause().getMessage() : "");
					if(msg.contains("OutOfMemoryError"))
						Assert.fail("OOM before the encoder overflow @ " + sz
							+ " - raise the test-fork heap (pom argLine -Xmx) and rerun. " + msg);
					if(msg.contains("exceeds maxCapacity")) {
						log(String.format("[sweep %d] CRASH %s (%.1fs)  -> %s", i, sz, secs(t0), msg.trim()));
						firstCrashBytes = rawBytes;
						firstCrashRows = rows;
						crashWriterIndex = parseWriterIndex(msg);
						break;
					}
					Assert.fail("Unexpected execution exception @ " + sz + ": " + msg);
				}
				finally {
					mb = null;
					request = null;
					FederatedData.clearFederatedWorkers();
					System.gc();
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			Assert.fail("Sweep failed due to network send exception: " + e.getMessage());
		}
		finally {
			FederatedData.clearFederatedWorkers();
		}

		log("==== FED MAX PAYLOAD THRESHOLD (cols=" + COLS + ", maxCapacity=" + Integer.MAX_VALUE + ") ====");
		log("  last  PASS : "
			+ (lastPassRows < 0 ? "none" : lastPassRows + " rows, " + String.format("%,d", lastPassBytes) + " raw bytes"));
		log("  first CRASH: "
			+ (firstCrashRows < 0 ? "none" : firstCrashRows + " rows, " + String.format("%,d", firstCrashBytes) + " raw bytes"));
		if(crashWriterIndex > 0)
			log("  crash writerIndex: " + String.format("%,d", crashWriterIndex)
				+ " (over cap by " + String.format("%,d", crashWriterIndex - Integer.MAX_VALUE) + " bytes at +1024 write)");
		if(lastPassBytes > 0 && firstCrashBytes > 0)
			log("  threshold raw-byte bracket: (" + String.format("%,d", lastPassBytes) + " , "
				+ String.format("%,d", firstCrashBytes) + "]");

		Assert.assertTrue("Start size " + START_ROWS + " rows already crashed - lower fedMaxPayload.startRows",
			lastPassRows > 0 || firstCrashRows < 0);
		Assert.assertTrue("No overflow observed up to " + END_ROWS + " rows x " + COLS
			+ " - raise fedMaxPayload.endRows", firstCrashRows > 0);
	}

	private static long parseWriterIndex(String msg) {
		Matcher m = WRITER_INDEX.matcher(msg);
		return m.find() ? Long.parseLong(m.group(1)) : -1;
	}

	private static void log(String s) {
		System.out.println(String.format("%tT [FedMaxPayload] %s", System.currentTimeMillis(), s));
	}

	private static String heap() {
		Runtime rt = Runtime.getRuntime();
		long used = rt.totalMemory() - rt.freeMemory();
		return "heap used=" + gib(used) + "/" + gib(rt.maxMemory());
	}

	private static String gib(long bytes) {
		return String.format("%.2fGiB", bytes / (1024.0 * 1024 * 1024));
	}

	private static double secs(long t0Nanos) {
		return (System.nanoTime() - t0Nanos) / 1e9;
	}
}
