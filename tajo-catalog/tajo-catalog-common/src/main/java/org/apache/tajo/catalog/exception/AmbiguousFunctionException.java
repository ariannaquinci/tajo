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

package org.apache.tajo.catalog.exception;

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.error.Errors;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;

import static org.apache.tajo.function.FunctionUtil.buildSimpleFunctionSignature;

public class AmbiguousFunctionException extends CatalogException {

  public AmbiguousFunctionException(PrimitiveProtos.ReturnState state) {
    super(state);
  }

  public AmbiguousFunctionException(String funcName, DataType[] parameters) {
    super(Errors.ResultCode.AMBIGUOUS_FUNCTION, buildSimpleFunctionSignature(funcName, parameters));
  }
}