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

public class FederatedMaxPayloadTest extends AutomatedTestBase {

	private final static String TEST_NAME = "FederatedMaxPayloadTest";
	private final static String TEST_DIR = "functions/federated/network/";
	private final static String TEST_CLASS_DIR = TEST_DIR + FederatedMaxPayloadTest.class.getSimpleName() + "/";

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[] {""}));
	}

	@Test
	public void testMaxNettyPayloadCrash() {
		int port = getRandomAvailablePort();
		Thread worker = startLocalFedWorkerThread(port, 10);

		try {
			// 30000 x 8950 dense doubles = 2,148,000,000 raw bytes, ~516 KB over Integer.MAX_VALUE,
			// so the legacy ObjectEncoder ByteBuf overflows while packing this single PUT_VAR.
			int rows = 30000;
			int cols = 8950;
			MatrixBlock mb = new MatrixBlock(rows, cols, false);
			mb.allocateDenseBlock();
			mb.setNonZeros((long) rows * cols);

			InetSocketAddress address = new InetSocketAddress("localhost", port);
			FederatedRequest request = new FederatedRequest(FederatedRequest.RequestType.PUT_VAR, 1, mb);

			Future<FederatedResponse> responseFuture = FederatedData.executeFederatedOperation(address, request);

			FederatedResponse response = responseFuture.get();
			Assert.assertTrue("Network send was not successful.", response.isSuccessful());
		}
		catch(ExecutionException e) {
			String errorMsg = e.getMessage() != null ? e.getMessage() : "";
			Throwable cause = e.getCause();
			String causeMsg = cause != null && cause.getMessage() != null ? cause.getMessage() : "";
			if(errorMsg.contains("exceeds maxCapacity") || causeMsg.contains("exceeds maxCapacity")) {
				Assert.fail("Test failing: Max capacity of encoder exceeded." + errorMsg);
			}
			else {
				Assert.fail("Test failed due to an unexpected execution exception: " + errorMsg);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed due to network send exception: " + e.getMessage());
		}
		finally {
			FederatedData.clearFederatedWorkers();
		}
	}
}
