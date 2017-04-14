/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

public class AddingECPolicyResponse {
  private boolean succeed;
  private ErasureCodingPolicy policy;
  private IllegalECPolicyException exception;

  public AddingECPolicyResponse(ErasureCodingPolicy policy, boolean succeed) {
    this.policy = policy;
    this.succeed = succeed;
  }

  public AddingECPolicyResponse(ErasureCodingPolicy policy,
      IllegalECPolicyException e) {
    this.policy = policy;
    this.exception = e;
    this.succeed = false;
  }

  public boolean isSucceed() {
    return succeed;
  }

  public ErasureCodingPolicy getPolicy() {
    return policy;
  }

  public IllegalECPolicyException getException() {
    return exception;
  }
}
