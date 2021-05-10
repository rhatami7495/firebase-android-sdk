// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.firebase.firestore.local;

import com.google.protobuf.ByteString;

/** An index value encoder. */
public abstract class DirectionalIndexByteEncoder {

    private boolean encodesEntityRef = false;

    public final boolean encodesEntityRef() {
        return encodesEntityRef;
    }

    public final void recordEncodesEntityRef() {
        encodesEntityRef = true;
    }

    public abstract void writeBytes(byte[] val);

    public abstract void writeBytes(ByteString val);

    public abstract void writeString(String val);

    public abstract void writeLong(long val);

    public abstract void writeLongDecreasing(long val);

    public abstract void writeDouble(double val);

    /** Writes a double such that it is comparable with doubles and longs together. */
    public abstract void writeNumber(double comparableNumberAsDouble);

    /** Writes a long such that it is comparable with doubles and longs together. */
    public abstract void writeNumber(long comparableNumberAsLong);
}