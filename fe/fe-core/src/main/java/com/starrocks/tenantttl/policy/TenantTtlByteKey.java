// Copyright 2021-present StarRocks, Inc. All rights reserved.
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

package com.starrocks.tenantttl.policy;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Immutable raw-byte key with unsigned lexicographic ordering. */
public final class TenantTtlByteKey implements Comparable<TenantTtlByteKey> {
    private final byte[] bytes;
    private final int hashCode;

    private TenantTtlByteKey(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes, "bytes is null").clone();
        this.hashCode = Arrays.hashCode(this.bytes);
    }

    public static TenantTtlByteKey copyOf(byte[] bytes) {
        return new TenantTtlByteKey(bytes);
    }

    public static TenantTtlByteKey utf8(String value) {
        return copyOf(Objects.requireNonNull(value, "value is null").getBytes(StandardCharsets.UTF_8));
    }

    public int size() {
        return bytes.length;
    }

    public byte[] copyBytes() {
        return bytes.clone();
    }

    void updateFingerprint(TenantTtlPolicyFingerprint fingerprint) {
        fingerprint.putBytes(bytes);
    }

    @Override
    public int compareTo(TenantTtlByteKey other) {
        return Arrays.compareUnsigned(bytes, other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TenantTtlByteKey)) {
            return false;
        }
        TenantTtlByteKey that = (TenantTtlByteKey) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
