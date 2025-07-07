// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.system;

import org.apache.doris.thrift.TStorageMedium;

import java.util.Collections;
import java.util.List;

/**
 * Result encapsulation for backend selection operations.
 * Follows Unix philosophy of clear success/failure semantics.
 */
public class BackendSelectionResult {
    private final boolean success;
    private final List<Long> backendIds;
    private final TStorageMedium actualMedium;
    private final String errorMessage;

    private BackendSelectionResult(boolean success, List<Long> backendIds, 
                                  TStorageMedium actualMedium, String errorMessage) {
        this.success = success;
        this.backendIds = backendIds == null ? Collections.emptyList() : Collections.unmodifiableList(backendIds);
        this.actualMedium = actualMedium;
        this.errorMessage = errorMessage;
    }

    /**
     * Create a successful result.
     * 
     * @param backendIds the selected backend IDs
     * @param actualMedium the actual storage medium used (may differ from requested if fallback occurred)
     * @return successful BackendSelectionResult
     */
    public static BackendSelectionResult success(List<Long> backendIds, TStorageMedium actualMedium) {
        return new BackendSelectionResult(true, backendIds, actualMedium, null);
    }

    /**
     * Create a failure result.
     * 
     * @param errorMessage detailed error message explaining the failure
     * @return failed BackendSelectionResult
     */
    public static BackendSelectionResult failure(String errorMessage) {
        return new BackendSelectionResult(false, null, null, errorMessage);
    }

    /**
     * @return true if backend selection was successful
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * @return the selected backend IDs (empty list if failed)
     */
    public List<Long> getBackendIds() {
        return backendIds;
    }

    /**
     * @return the actual storage medium used for selection
     */
    public TStorageMedium getActualMedium() {
        return actualMedium;
    }

    /**
     * @return error message if selection failed, null otherwise
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * @return number of selected backends
     */
    public int getSelectedCount() {
        return backendIds.size();
    }

    @Override
    public String toString() {
        if (success) {
            return String.format("BackendSelectionResult{success=true, count=%d, medium=%s}", 
                               backendIds.size(), actualMedium);
        } else {
            return String.format("BackendSelectionResult{success=false, error='%s'}", errorMessage);
        }
    }
} 