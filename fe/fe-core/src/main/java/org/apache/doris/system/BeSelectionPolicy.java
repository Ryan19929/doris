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

import org.apache.doris.common.Config;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.resource.Tag;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Selection policy for building BE nodes
 */
public class BeSelectionPolicy {
    private static final Logger LOG = LogManager.getLogger(BeSelectionPolicy.class);

    public boolean needScheduleAvailable = false;
    public boolean needQueryAvailable = false;
    public boolean needLoadAvailable = false;
    // Resource tag. Empty means no need to consider resource tag.
    public Set<Tag> resourceTags = Sets.newHashSet();
    // storage medium. null means no need to consider storage medium.
    public TStorageMedium storageMedium = null;
    // Check if disk usage reaches limit. false means no need to check.
    public boolean checkDiskUsage = false;
    // If set to false, do not select backends on same host.
    public boolean allowOnSameHost = false;

    public boolean preferComputeNode = false;
    public int expectBeNum = 0;

    public boolean enableRoundRobin = false;
    // if enable round robin, choose next be from nextRoundRobinIndex
    // call SystemInfoService::selectBackendIdsByPolicy will update nextRoundRobinIndex
    public int nextRoundRobinIndex = -1;

    public List<String> preferredLocations = new ArrayList<>();

    public boolean requireAliveBe = false;
    
    // Strict storage medium mode: if true, no fallback to other storage medium
    public boolean strictStorageMedium = false;
    
    // Whether storage medium was explicitly specified by user
    public boolean isStorageMediumSpecified = false;
    
    // Whether this is only for checking availability (no actual selection)
    public boolean isOnlyForCheck = false;

    private BeSelectionPolicy() {

    }

    public static class Builder {
        private BeSelectionPolicy policy;

        public Builder() {
            policy = new BeSelectionPolicy();
        }

        public Builder needScheduleAvailable() {
            policy.needScheduleAvailable = true;
            return this;
        }

        public Builder needQueryAvailable() {
            policy.needQueryAvailable = true;
            return this;
        }

        public Builder needLoadAvailable() {
            policy.needLoadAvailable = true;
            return this;
        }

        public Builder addTags(Set<Tag> tags) {
            policy.resourceTags.addAll(tags);
            return this;
        }

        public Builder setStorageMedium(TStorageMedium medium) {
            policy.storageMedium = medium;
            return this;
        }

        public Builder needCheckDiskUsage() {
            policy.checkDiskUsage = true;
            return this;
        }

        public Builder allowOnSameHost() {
            policy.allowOnSameHost = true;
            return this;
        }

        public Builder preferComputeNode(boolean prefer) {
            policy.preferComputeNode = prefer;
            return this;
        }

        public Builder assignExpectBeNum(int expectBeNum) {
            policy.expectBeNum = expectBeNum;
            return this;
        }

        public Builder addPreLocations(List<String> preferredLocations) {
            policy.preferredLocations.addAll(preferredLocations);
            return this;
        }

        public Builder setEnableRoundRobin(boolean enableRoundRobin) {
            policy.enableRoundRobin = enableRoundRobin;
            return this;
        }

        public Builder setNextRoundRobinIndex(int nextRoundRobinIndex) {
            policy.nextRoundRobinIndex = nextRoundRobinIndex;
            return this;
        }

        public Builder setRequireAliveBe() {
            policy.requireAliveBe = true;
            return this;
        }

        public Builder setStrictStorageMedium(boolean strictStorageMedium) {
            policy.strictStorageMedium = strictStorageMedium;
            return this;
        }

        public Builder setStorageMediumSpecified(boolean isStorageMediumSpecified) {
            policy.isStorageMediumSpecified = isStorageMediumSpecified;
            return this;
        }

        public Builder setOnlyForCheck(boolean isOnlyForCheck) {
            policy.isOnlyForCheck = isOnlyForCheck;
            return this;
        }

        public BeSelectionPolicy build() {
            return policy;
        }
    }

    private boolean isMatch(Backend backend) {
        // Compute node is only used when preferComputeNode is set.
        if (!preferComputeNode && backend.isComputeNode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Backend [{}] is not match by ComputeNode rule, policy: [{}]", backend.getHost(), this);
            }
            return false;
        }

        if (needScheduleAvailable && !backend.isScheduleAvailable()
                || needQueryAvailable && !backend.isQueryAvailable()
                || needLoadAvailable && !backend.isLoadAvailable()
                || (!resourceTags.isEmpty() && !resourceTags.contains(backend.getLocationTag()))
                || storageMedium != null && !backend.hasSpecifiedStorageMedium(storageMedium)
                || (requireAliveBe && !backend.isAlive())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Backend [{}] is not match by Other rules, policy: [{}]", backend.getHost(), this);
            }
            return false;
        }

        if (checkDiskUsage) {
            if (storageMedium == null && backend.diskExceedLimit()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Backend [{}] is not match by diskExceedLimit rule, policy: [{}]", backend.getHost(),
                            this);
                }
                return false;
            }
            if (storageMedium != null && backend.diskExceedLimitByStorageMedium(storageMedium)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Backend [{}] is not match by diskExceedLimitByStorageMedium rule, policy: [{}]",
                            backend.getHost(), this);
                }
                return false;
            }
        }
        return true;
    }

    public List<Backend> getCandidateBackends(Collection<Backend> backends) {
        List<Backend> filterBackends = backends.stream().filter(this::isMatch)
                .collect(Collectors.toList());
        List<Backend> preLocationFilterBackends = filterBackends.stream()
                .filter(iterm -> preferredLocations.contains(iterm.getHost())).collect(Collectors.toList());
        // If preLocations were chosen, use the preLocation backends. Otherwise we just ignore this filter.
        if (!preLocationFilterBackends.isEmpty()) {
            filterBackends = preLocationFilterBackends;
        }
        Collections.shuffle(filterBackends);
        int numComputeNode = filterBackends.stream().filter(Backend::isComputeNode).collect(Collectors.toList()).size();
        List<Backend> candidates = new ArrayList<>();
        if (preferComputeNode && numComputeNode > 0) {
            int realExpectBeNum = expectBeNum == -1 ? numComputeNode : expectBeNum;
            int num = 0;
            // pick compute node first
            for (Backend backend : filterBackends) {
                if (backend.isComputeNode()) {
                    candidates.add(backend);
                    num++;
                }
            }
            // fill with some mix node.
            if (num < realExpectBeNum) {
                for (Backend backend : filterBackends) {
                    if (backend.isMixNode()) {
                        if (num >= realExpectBeNum) {
                            break;
                        }
                        candidates.add(backend);
                        num++;
                    }
                }
            }
        } else {
            candidates.addAll(filterBackends);
        }
        // filter out backends in black list
        if (!Config.disable_backend_black_list) {
            candidates = candidates.stream().filter(b -> SimpleScheduler.isAvailable(b)).collect(Collectors.toList());
        }
        Collections.shuffle(candidates);
        return candidates;
    }

    /**
     * Select backends with integrated fallback logic and strict mode support.
     * This method encapsulates the complete backend selection strategy in one place,
     * following Unix philosophy of "do one thing well".
     *
     * @param backends the available backend pool
     * @param requiredNum number of backends required
     * @return BackendSelectionResult containing success/failure info and selected backends
     */
    public BackendSelectionResult selectBackends(Collection<Backend> backends, int requiredNum) {
        // Special case: no storage medium specified
        if (storageMedium == null) {
            List<Backend> candidates = getCandidateBackends(backends);
            if (candidates.size() >= requiredNum) {
                List<Long> selectedIds = candidates.subList(0, requiredNum).stream()
                        .map(Backend::getId)
                        .collect(Collectors.toList());
                return BackendSelectionResult.success(selectedIds, null);
            } else {
                String errorMsg = candidates.isEmpty() 
                    ? String.format("No suitable backends found. Required: %d, Available: 0", requiredNum)
                    : String.format("Insufficient backends. Required: %d, Available: %d", requiredNum, candidates.size());
                return BackendSelectionResult.failure(errorMsg);
            }
        }
        
        // Storage medium specified: use optimized dual collection for single-pass efficiency
        DualCandidateLists dualLists = getDualCandidateBackends(backends);
        List<Backend> primaryCandidates = dualLists.getPrimaryCandidates();
        List<Backend> fallbackCandidates = dualLists.getFallbackCandidates();
        
        // Try primary candidates first
        if (primaryCandidates.size() >= requiredNum) {
            List<Long> selectedIds = primaryCandidates.subList(0, requiredNum).stream()
                    .map(Backend::getId)
                    .collect(Collectors.toList());
            return BackendSelectionResult.success(selectedIds, storageMedium);
        }
        
        // Primary candidates insufficient
        if (strictStorageMedium || (isStorageMediumSpecified && !isOnlyForCheck)) {
            // Strict mode OR explicitly specified storage medium: fail immediately without fallback
            String mode = strictStorageMedium ? "Strict storage medium mode" : "Explicitly specified storage medium";
            String errorMsg = String.format(
                "%s: Failed to find enough backends with %s storage medium. " +
                "Required: %d, Found: %d. " +
                "Available backends with %s: %s. " +
                "Consider: 1) Add more %s storage backends%s",
                mode, storageMedium, requiredNum, primaryCandidates.size(), storageMedium,
                primaryCandidates.stream().map(be -> be.getHost() + ":" + be.getHeartbeatPort())
                    .collect(Collectors.joining(", ")),
                storageMedium,
                strictStorageMedium ? ", 2) Set " + PropertyAnalyzer.PROPERTIES_STRICT_STORAGE_MEDIUM + "=false to allow fallback" : "");
            return BackendSelectionResult.failure(errorMsg);
        }
        
        // Non-strict mode: try fallback candidates (already collected in single pass)
        if (!fallbackCandidates.isEmpty() && fallbackCandidates.size() >= requiredNum) {
            List<Long> selectedIds = fallbackCandidates.subList(0, requiredNum).stream()
                    .map(Backend::getId)
                    .collect(Collectors.toList());
            LOG.info("Backend selection fallback: requested {} but using {} (strict_storage_medium=false)", 
                    storageMedium, dualLists.getFallbackMedium());
            return BackendSelectionResult.success(selectedIds, dualLists.getFallbackMedium());
        }
        
        // Even fallback failed
        return BackendSelectionResult.failure(
            String.format("Insufficient backends even after fallback. " +
                        "Requested medium: %s (found: %d), Fallback medium: %s (found: %d), Required: %d",
                        storageMedium, primaryCandidates.size(), 
                        dualLists.getFallbackMedium(), fallbackCandidates.size(), requiredNum));
    }

    /**
     * Get fallback storage medium for the given medium.
     */
    private TStorageMedium getFallbackMedium(TStorageMedium medium) {
        if (medium == null) {
            return null;
        }
        return (medium == TStorageMedium.HDD) ? TStorageMedium.SSD : TStorageMedium.HDD;
    }

    /**
     * Inner class to hold dual candidate lists for optimized backend selection.
     * This allows us to collect both primary and fallback candidates in a single pass.
     */
    public static class DualCandidateLists {
        private final List<Backend> primaryCandidates;
        private final List<Backend> fallbackCandidates;
        private final TStorageMedium primaryMedium;
        private final TStorageMedium fallbackMedium;

        public DualCandidateLists(List<Backend> primaryCandidates, List<Backend> fallbackCandidates,
                                 TStorageMedium primaryMedium, TStorageMedium fallbackMedium) {
            this.primaryCandidates = primaryCandidates;
            this.fallbackCandidates = fallbackCandidates;
            this.primaryMedium = primaryMedium;
            this.fallbackMedium = fallbackMedium;
        }

        public List<Backend> getPrimaryCandidates() { return primaryCandidates; }
        public List<Backend> getFallbackCandidates() { return fallbackCandidates; }
        public TStorageMedium getPrimaryMedium() { return primaryMedium; }
        public TStorageMedium getFallbackMedium() { return fallbackMedium; }
    }

    /**
     * Optimized method to get dual candidate lists in a single pass.
     * This avoids double traversal when fallback is needed.
     *
     * @param backends the available backend pool
     * @return DualCandidateLists containing both primary and fallback candidates
     */
    public DualCandidateLists getDualCandidateBackends(Collection<Backend> backends) {
        List<Backend> primaryCandidates = new ArrayList<>();
        List<Backend> fallbackCandidates = new ArrayList<>();
        
        TStorageMedium fallbackMedium = getFallbackMedium(storageMedium);
        
        // Single pass through all backends
        for (Backend backend : backends) {
            // Check basic criteria (everything except storage medium)
            if (!isMatchExceptStorageMedium(backend)) {
                continue;
            }
            
            // Check primary storage medium
            if (storageMedium == null || backend.hasSpecifiedStorageMedium(storageMedium)) {
                if (!checkDiskUsage || !backend.diskExceedLimitByStorageMedium(storageMedium)) {
                    primaryCandidates.add(backend);
                }
            }
            
            // Check fallback storage medium (only if different from primary and not in strict mode)
            if (!strictStorageMedium && fallbackMedium != null && fallbackMedium != storageMedium 
                && backend.hasSpecifiedStorageMedium(fallbackMedium)) {
                if (!checkDiskUsage || !backend.diskExceedLimitByStorageMedium(fallbackMedium)) {
                    fallbackCandidates.add(backend);
                }
            }
        }
        
        // Apply preferred locations and shuffle
        primaryCandidates = applyPreferredLocationsAndShuffle(primaryCandidates);
        fallbackCandidates = applyPreferredLocationsAndShuffle(fallbackCandidates);
        
        // Apply compute node preferences
        primaryCandidates = applyComputeNodePreferences(primaryCandidates);
        fallbackCandidates = applyComputeNodePreferences(fallbackCandidates);
        
        return new DualCandidateLists(primaryCandidates, fallbackCandidates, storageMedium, fallbackMedium);
    }

    /**
     * Helper method to check if backend matches all criteria except storage medium.
     * This is extracted to avoid code duplication in getDualCandidateBackends.
     */
    private boolean isMatchExceptStorageMedium(Backend backend) {
        // Compute node is only used when preferComputeNode is set.
        if (!preferComputeNode && backend.isComputeNode()) {
            return false;
        }

        if (needScheduleAvailable && !backend.isScheduleAvailable()
                || needQueryAvailable && !backend.isQueryAvailable()
                || needLoadAvailable && !backend.isLoadAvailable()
                || (!resourceTags.isEmpty() && !resourceTags.contains(backend.getLocationTag()))
                || (requireAliveBe && !backend.isAlive())) {
            return false;
        }

        return true;
    }

    /**
     * Helper method to apply preferred locations and shuffle.
     */
    private List<Backend> applyPreferredLocationsAndShuffle(List<Backend> candidates) {
        List<Backend> preLocationFilterBackends = candidates.stream()
                .filter(item -> preferredLocations.contains(item.getHost()))
                .collect(Collectors.toList());
        
        // If preLocations were chosen, use the preLocation backends. Otherwise we just ignore this filter.
        if (!preLocationFilterBackends.isEmpty()) {
            candidates = preLocationFilterBackends;
        }
        
        Collections.shuffle(candidates);
        return candidates;
    }

    /**
     * Helper method to apply compute node preferences.
     */
    private List<Backend> applyComputeNodePreferences(List<Backend> filterBackends) {
        int numComputeNode = filterBackends.stream().filter(Backend::isComputeNode).collect(Collectors.toList()).size();
        List<Backend> candidates = new ArrayList<>();
        
        if (preferComputeNode && numComputeNode > 0) {
            int realExpectBeNum = expectBeNum == -1 ? numComputeNode : expectBeNum;
            int num = 0;
            // pick compute node first
            for (Backend backend : filterBackends) {
                if (backend.isComputeNode()) {
                    candidates.add(backend);
                    num++;
                }
            }
            // fill with some mix node.
            if (num < realExpectBeNum) {
                for (Backend backend : filterBackends) {
                    if (backend.isMixNode()) {
                        if (num >= realExpectBeNum) {
                            break;
                        }
                        candidates.add(backend);
                        num++;
                    }
                }
            }
        } else {
            candidates.addAll(filterBackends);
        }
        
        // filter out backends in black list
        if (!Config.disable_backend_black_list) {
            candidates = candidates.stream().filter(b -> SimpleScheduler.isAvailable(b)).collect(Collectors.toList());
        }
        
        Collections.shuffle(candidates);
        return candidates;
    }

    @Override
    public String toString() {
        return String.format("computeNode=%s | query=%s | load=%s | schedule=%s | tags=%s | medium=%s",
                preferComputeNode, needQueryAvailable, needLoadAvailable, needScheduleAvailable,
                resourceTags.stream().map(tag -> tag.toString()).collect(Collectors.joining(",")), storageMedium);
    }
}
