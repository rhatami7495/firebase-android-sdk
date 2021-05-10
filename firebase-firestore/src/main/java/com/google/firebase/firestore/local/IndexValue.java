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
import com.google.protobuf.Timestamp;
import com.google.type.LatLng;

/**
 * An index value.
 *
 * <p>An index value may be truncated. Such an index value has at least one truncation marker.
 *
 * <p>A Datastore query semantics index value is never truncated and is of type NULL, BOOLEAN,
 * NUMBER, BYTES, ENTITY_REF, GEO_POINT, or LEGACY_USER.
 *
 * <p>A Firestore query semantics index value may be truncated, is of type NULL, BOOLEAN, NUMBER,
 * TIMESTAMP, STRING, BYTES, ENTITY_REF, GEO_POINT, ARRAY, or MAP, and may contain an index value of
 * type ABSENT.
 */
public class IndexValue {

    // LINT.IfChange(index_value_types)
    /** An index value type. */
    public enum Type {
        /** Used to represent a truncated-away value. */
        ABSENT_TYPE,
        NULL,
        BOOLEAN,
        NUMBER,
        TIMESTAMP,
        STRING,
        BYTES,
        ENTITY_REF,
        GEO_POINT,
        ARRAY,
        MAP,
        LEGACY_USER;
    }
    // LINT.ThenChange(../index/firestore/FirestoreIndexValueComparator.java)

    public static final int NULL_NATURAL_SIZE = 1;
    public static final int BOOLEAN_NATURAL_SIZE = 1;
    public static final int NUMBER_NATURAL_SIZE = 8;
    public static final int TIMESTAMP_NATURAL_SIZE = 8;
    public static final int GEO_POINT_NATURAL_SIZE = 16;
    public static final int STRING_BASE_NATURAL_SIZE = 1;
    public static final int ENTITY_REF_BASE_NATURAL_SIZE = 16;
    // The natural size overhead of a LegacyUser value (equivalent to the natural size of the strings
    // "email" and "auth_domain" summed).
    public static final int LEGACY_USER_BASE_NATURAL_SIZE = 18;
    // The natural size overhead for a LegacyUser value optional field, counted once for each of
    // federatedIdentity or federatedProvider (equivalent to the natural size of either strings
    // "federated_identity" or "federated_provider").
    public static final int LEGACY_USER_FEDERATED_FIELDS_BASE_NATURAL_SIZE = 19;

    /** An entity reference represented as index values. May contain truncation markers. */
    @AutoValue
    public abstract static class EntityRef {

        /** An empty index value entity reference. */
        public static final EntityRef EMPTY =
                create(DatabaseRef.EMPTY, IndexValue.EMPTY_STRING, IndexValue.EMPTY_ARRAY);

        public static EntityRef create(
                DatabaseRef databaseRef, IndexValue namespaceId, IndexValue segments) {
            if (!segments.isAbsent()) {
                checkArgument(!namespaceId.isShallowTruncated()); // Includes ABSENT.
                checkSegments(segments);
            }
            return new AutoValue_IndexValue_EntityRef(databaseRef, namespaceId, segments);
        }

        private static void checkSegments(IndexValue segments) {
            checkArgument(segments.type() == Type.ARRAY);
            ImmutableList<IndexValue> segmentsList = segments.asArray();
            int numSegments = segmentsList.size();
            for (int index = 0; index < numSegments; ++index) {
                IndexValue segment = segmentsList.get(index);
                switch (segment.type()) {
                    case NUMBER:
                        checkArgument((index % 2) == 1);
                        checkArgument(!segment.isNumberDouble());
                        break;
                    case STRING:
                        if (segment.isShallowTruncated()) {
                            checkArgument(index == (numSegments - 1));
                            checkArgument(!segments.isShallowTruncated());
                        }
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
            }
        }

        /** Empty only when the whole entity reference is empty. */
        public abstract DatabaseRef databaseRef();

        /**
         * Always type {@link Type#STRING} if not {@link IndexValue#ABSENT}. If empty and not truncated,
         * which represents the default namespace, its size does not count. If truncated (which includes
         * {@link IndexValue#ABSENT}), {@link #segments} must be {@link IndexValue#ABSENT}.
         */
        public abstract IndexValue namespaceId();

        /**
         * IndexValue mirrors {@link core.rep.EntityRef#pathElements}. Each {@link
         * core.rep.EntityRef.PathElement} is represented as a pair of collectionId and resourceId.
         *
         * <p>Always type {@link Type#ARRAY} if not {@link IndexValue#ABSENT}. The array may be empty or
         * truncated (but not both).
         *
         * <p>Each collectionId element (even index) is {@link Type#STRING}. Each resourceId element
         * (odd index) is {@link Type#STRING} or {@link Type#NUMBER}.
         */
        public abstract IndexValue segments();

        private int naturalSize() {
            int namespaceIdNaturalSize =
                    namespaceId().equals(EMPTY_STRING) // Note: NOT isShallowTruncated.
                            ? 0
                            : namespaceId().naturalSize();
            return ENTITY_REF_BASE_NATURAL_SIZE + namespaceIdNaturalSize + segments().naturalSize();
        }

        /** For Datastore, incomplete path segments are sized as longs. */
        private int datastoreNaturalSize() {
            checkArgument(!namespaceId().isDeepTruncated());
            checkArgument(!segments().isDeepTruncated());
            int namespaceIdNaturalSize =
                    namespaceId().equals(EMPTY_STRING) // Note: NOT isShallowTruncated.
                            ? 0
                            : namespaceId().naturalSize();
            boolean incompleteSegments = segments().asArray().size() % 2 != 0;
            return ENTITY_REF_BASE_NATURAL_SIZE
                    + namespaceIdNaturalSize
                    + segments().naturalSize()
                    + (incompleteSegments ? NUMBER_NATURAL_SIZE : 0);
        }
    }

    private static final Object NO_BOXED_OBJECT = new Object();
    /** The unique absent value. Truncated. */
    public static final IndexValue ABSENT = new IndexValue();
    /** The unique null value. */
    public static final IndexValue NULL =
            new IndexValue(Type.NULL, 0xDEADDEADBEEFBEEFL, /*isNumberDouble=*/ false, NULL_NATURAL_SIZE);

    public static final IndexValue NAN =
            new IndexValue(
                    Type.NUMBER,
                    Double.doubleToRawLongBits(Double.NaN),
                    /*isNumberDouble=*/ true,
                    NUMBER_NATURAL_SIZE);
    public static final IndexValue FALSE =
            new IndexValue(Type.BOOLEAN, 0, /*isNumberDouble=*/ false, BOOLEAN_NATURAL_SIZE);
    public static final IndexValue NEGATIVE_INFINITY =
            IndexValue.createNumberDouble(Double.NEGATIVE_INFINITY);
    public static final IndexValue MIN_VALID_TIMESTAMP =
            new IndexValue(
                    Type.TIMESTAMP,
                    Timestamps.fromSeconds(RepHelper.TIMESTAMP_MIN_SECONDS),
                    TIMESTAMP_NATURAL_SIZE);
    // TODO(b/147519645): Consider changing this to MIN_VALID_GEOPOINT and using the minimum allowable
    // geopoint values based on validation [-90, -180].
    public static final IndexValue MIN_GEOPOINT =
            IndexValue.createGeoPoint(
                    LatLng.newBuilder()
                            .setLatitude(Double.NEGATIVE_INFINITY)
                            .setLongitude(Double.NEGATIVE_INFINITY)
                            .build());
    public static final IndexValue TRUE =
            new IndexValue(Type.BOOLEAN, 1, /*isNumberDouble=*/ false, BOOLEAN_NATURAL_SIZE);
    public static final IndexValue EMPTY_STRING = createString(/*isTruncated=*/ false, "");
    public static final IndexValue EMPTY_BYTES =
            createBytes(/*isTruncated=*/ false, ByteString.EMPTY);
    public static final IndexValue EMPTY_ARRAY =
            createArray(/*isShallowTruncated=*/ false, ImmutableList.<IndexValue>of());
    public static final IndexValue EMPTY_MAP =
            createMap(/*isShallowTruncated=*/ false, ImmutableMap.<IndexValue, IndexValue>of());

    /** Create a boolean index value containing {@code x}. */
    public static IndexValue createBoolean(boolean x) {
        return (x ? TRUE : FALSE);
    }

    /** Create a number index value containing long {@code x}. */
    public static IndexValue createNumberLong(long x) {
        return new IndexValue(Type.NUMBER, x, /*isNumberDouble=*/ false, NUMBER_NATURAL_SIZE);
    }

    /** Create a number index value containing double {@code x}. */
    public static IndexValue createNumberDouble(double x) {
        return new IndexValue(
                Type.NUMBER, Double.doubleToRawLongBits(x), /*isNumberDouble=*/ true, NUMBER_NATURAL_SIZE);
    }

    /** Create a timestamp index value containing {@code x}. */
    public static IndexValue createTimestamp(Timestamp x) {
        // Make sure MIN_VALID_TIMESTAMP is actually the minimum value.
        checkArgument(Timestamps.compare(x, MIN_VALID_TIMESTAMP.asTimestamp()) >= 0);
        return new IndexValue(Type.TIMESTAMP, x, TIMESTAMP_NATURAL_SIZE);
    }

    /** Create a string index value containing {@code x}. */
    public static IndexValue createString(boolean isTruncated, String x) {
        return new IndexValue(isTruncated, Type.STRING, x, computeStringNaturalSize(x));
    }

    private static int computeStringNaturalSize(String string) {
        return STRING_BASE_NATURAL_SIZE + Utf8.encodedLength(string);
    }

    /**
     * Create a bytes index value containing {@code x}.
     *
     * <p>See method {@link #asBytes} for constraints on {@code x}.
     */
    public static IndexValue createBytes(boolean isTruncated, ByteString x) {
        checkArgument(!isTruncated || !x.isEmpty());
        return new IndexValue(isTruncated, Type.BYTES, x, x.size());
    }

    /**
     * Create a bytes index value containing {@code s}. Uses the string natural size calculation for
     * this value.
     */
    public static IndexValue createDatastoreUntruncatedBytesWithStringNaturalSize(ByteString x) {
        return new IndexValue(false, Type.BYTES, x, STRING_BASE_NATURAL_SIZE + x.size());
    }

    /** Create an entity reference index value containing {@code x}. */
    public static IndexValue createEntityRef(IndexValue.EntityRef x) {
        return new IndexValue(
                /*isDeepTruncated=*/ x.namespaceId().isDeepTruncated() || x.segments().isDeepTruncated(),
                /*isShallowTruncated=*/ false,
                Type.ENTITY_REF,
                x,
                x.naturalSize());
    }

    /**
     *  Create an entity reference index value containing {@code x}.
     *
     * @deprecated use {@link IndexValue#createEntityRef(IndexValue.EntityRef)), see b/144383048
     */
    @Deprecated
    public static IndexValue createDatastoreEntityRef(IndexValue.EntityRef x) {
        checkArgument(!(x.namespaceId().isDeepTruncated() || x.segments().isDeepTruncated()));
        return new IndexValue(
                /*isDeepTruncated=*/ false,
                /*isShallowTruncated=*/ false,
                Type.ENTITY_REF,
                x,
                x.datastoreNaturalSize());
    }

    /** Create an index value containing {@code x}. */
    public static IndexValue createGeoPoint(LatLng x) {
        return new IndexValue(Type.GEO_POINT, x, GEO_POINT_NATURAL_SIZE);
    }

    /**
     * Create an array index value containing {@code x}.
     *
     * <p>See method {@link #asArray} for constraints on {@code x}.
     */
    public static IndexValue createArray(boolean isShallowTruncated, ImmutableList<IndexValue> x) {
        ImmutableList<IndexValue> elements = x;
        int numElements = elements.size();
        int naturalSize = 0;
        checkArgument(!isShallowTruncated || (numElements != 0));
        boolean isDeepTruncated = isShallowTruncated;
        for (int index = 0; index < numElements; ++index) {
            IndexValue element = elements.get(index);
            if (element.isDeepTruncated()) {
                checkArgument(index == (numElements - 1));
                checkArgument(!isShallowTruncated);
                // ABSENT is deep truncated, so checking here is sufficient.
                checkArgument(!element.isAbsent());
                isDeepTruncated = true;
            }
            naturalSize += element.naturalSize();
        }
        return new IndexValue(isDeepTruncated, isShallowTruncated, Type.ARRAY, x, naturalSize);
    }

    /**
     * Create a new map index value containing {@code x}.
     *
     * <p>See method {@link #asMap} for constraints on {@code x}.
     */
    public static IndexValue createMap(
            boolean isShallowTruncated, ImmutableMap<IndexValue, IndexValue> x) {
        ImmutableMap<IndexValue, IndexValue> map = x;
        // Loop through the map entries:
        // - Sum the natural size of the map entries.
        // - Verify that the *previous* map entry is not deep truncated,
        //   thus verifying all map entries but the last.
        int naturalSize = 0;
        IndexValue prevKeyIndexValue = null;
        IndexValue prevValueIndexValue = null;
        for (ImmutableMap.Entry<IndexValue, IndexValue> entry : map.entrySet()) {
            IndexValue keyIndexValue = entry.getKey();
            IndexValue valueIndexValue = entry.getValue();
            naturalSize += keyIndexValue.naturalSize() + valueIndexValue.naturalSize();
            if (prevKeyIndexValue != null) {
                checkArgument(!prevKeyIndexValue.isDeepTruncated());
                // Also forbids ABSENT, which is truncated.
                checkArgument(!prevValueIndexValue.isDeepTruncated());
            }
            prevKeyIndexValue = keyIndexValue;
            prevValueIndexValue = valueIndexValue;
        }
        boolean isDeepTruncated = isShallowTruncated;
        if (prevKeyIndexValue == null) {
            checkArgument(!isShallowTruncated);
        } else {
            // The *previous* map entry is now the last map entry.
            if (isShallowTruncated) {
                // Verify that the last map entry is not deep truncated.
                checkArgument(!prevKeyIndexValue.isDeepTruncated());
                // Also forbids ABSENT, which is truncated.
                checkArgument(!prevValueIndexValue.isDeepTruncated());
            } else {
                // Determine whether the last map entry is deep truncated.
                if (prevKeyIndexValue.isDeepTruncated()) {
                    checkArgument(prevValueIndexValue.isAbsent());
                    isDeepTruncated = true;
                } else {
                    isDeepTruncated = prevValueIndexValue.isDeepTruncated();
                }
            }
        }
        return new IndexValue(isDeepTruncated, isShallowTruncated, Type.MAP, x, naturalSize);
    }

    /** Create a new legacy user index value containing {@code x}. */
    public static IndexValue createLegacyUser(Value.LegacyUser x) {
        int naturalSize = computeLegacyUserNaturalSize(x);
        return new IndexValue(Type.LEGACY_USER, x, naturalSize);
    }

    private static int computeLegacyUserNaturalSize(Value.LegacyUser user) {
        int naturalSize =
                LEGACY_USER_BASE_NATURAL_SIZE
                        + computeStringNaturalSize(user.email())
                        + computeStringNaturalSize(user.authDomain());
        if (user.federatedIdentity() != null) {
            naturalSize +=
                    LEGACY_USER_FEDERATED_FIELDS_BASE_NATURAL_SIZE
                            + computeStringNaturalSize(user.federatedIdentity());
        }
        if (user.federatedProvider() != null) {
            naturalSize +=
                    LEGACY_USER_FEDERATED_FIELDS_BASE_NATURAL_SIZE
                            + computeStringNaturalSize(user.federatedProvider());
        }
        return naturalSize;
    }

    private final boolean isDeepTruncated;
    private final boolean isShallowTruncated;
    /** True when this index value is a number represented as a double, and otherwise false. */
    private final boolean isNumberDouble;

    private final int naturalSize;
    private final Type type;
    /**
     * A subtle but vital feature of representing every unboxed value as a long is that double
     * equality is bitwise, which means (NaN == NaN) and (-0.0 != 0.0). If more than one NaN is
     * supported also mean s that (NaN[x] != NaN[y]) unless (x == y).
     */
    private final long unboxed;

    private final Object boxed;

    // Used for types absent.
    private IndexValue() {
        this.isDeepTruncated = true;
        this.isShallowTruncated = true;
        this.type = Type.ABSENT_TYPE;
        this.unboxed = 0xDEADBEEFDEADBEEFL;
        this.boxed = NO_BOXED_OBJECT;
        this.isNumberDouble = false;
        this.naturalSize = 0;
    }

    // Used for types null, boolean, and number.
    private IndexValue(Type type, long unboxed, boolean isNumberDouble, int naturalSize) {
        this.isDeepTruncated = false;
        this.isShallowTruncated = false;
        this.type = type;
        this.unboxed = unboxed;
        this.boxed = NO_BOXED_OBJECT;
        this.isNumberDouble = isNumberDouble;
        this.naturalSize = naturalSize;
    }

    // Used for types timestamp, geo point, and legacy user.
    private IndexValue(Type type, Object boxed, int naturalSize) {
        this.isDeepTruncated = false;
        this.isShallowTruncated = false;
        this.type = type;
        this.unboxed = 0xBEEFDEADBEEFDEADL;
        this.boxed = boxed;
        this.isNumberDouble = false;
        this.naturalSize = naturalSize;
    }

    // Used for types string and bytes.
    private IndexValue(boolean isTruncated, Type type, Object boxed, int naturalSize) {
        this.isDeepTruncated = isTruncated;
        this.isShallowTruncated = isTruncated;
        this.type = type;
        this.unboxed = 0xBEEFDEADBEEFDEADL;
        this.boxed = boxed;
        this.isNumberDouble = false;
        this.naturalSize = naturalSize;
    }

    // Used for types entity reference, array, and map.
    private IndexValue(
            boolean isDeepTruncated,
            boolean isShallowTruncated,
            Type type,
            Object boxed,
            int naturalSize) {
        this.isDeepTruncated = isDeepTruncated;
        this.isShallowTruncated = isShallowTruncated;
        this.type = type;
        this.unboxed = 0xBEEFDEADBEEFDEADL;
        this.boxed = boxed;
        this.isNumberDouble = false;
        this.naturalSize = naturalSize;
    }

    public Type type() {
        return type;
    }

    @SuppressWarnings("ReferenceEquality")
    public boolean isAbsent() {
        // We use reference equal because ABSENT is a singleton value.
        return this == ABSENT;
    }

    /**
     * Returns whether this index value is truncated at the outermost level.
     *
     * <p>Applies only to types {@link Type#ABSENT}, {@link Type#STRING}, {@link Type#BYTES}, {@link
     * Type#ARRAY}, and {@link Type#MAP}.
     *
     * <p>For types {@link Type#ARRAY} and {@link Type#MAP} {@link #isShallowTruncated} may be false
     * even though {@link #isDeepTruncated} is true.
     */
    public boolean isShallowTruncated() {
        return isShallowTruncated;
    }

    /**
     * Returns whether this index value is truncated at any level.
     *
     * <p>Applies only to types {@link Type#ABSENT}, {@link Type#STRING}, {@link Type#BYTES}, {@link
     * Type#ARRAY}, and {@link Type#MAP}.
     *
     * <p>For types {@link Type#ARRAY} and {@link Type#MAP} this method may return true even when
     * {@link #isShallowTruncated} returns false
     */
    public boolean isDeepTruncated() {
        return isDeepTruncated;
    }

    public int naturalSize() {
        return naturalSize;
    }

    public boolean asBoolean() {
        return (unboxed != 0);
    }

    /**
     * Returns true when this index value is a number represented as a double, and otherwise false.
     */
    public boolean isNumberDouble() {
        return isNumberDouble;
    }

    public long asNumberLong() {
        return unboxed;
    }

    public double asNumberDouble() {
        return Double.longBitsToDouble(unboxed);
    }

    public Timestamp asTimestamp() {
        return ((Timestamp) boxed);
    }

    public String asString() {
        return ((String) boxed);
    }

    /** A truncated bytes is not empty. */
    public ByteString asBytes() {
        return ((ByteString) boxed);
    }

    public IndexValue.EntityRef asEntityRef() {
        return ((IndexValue.EntityRef) boxed);
    }

    // TODO(b/181349055): replace with a POJO to make conversion to proto3 LatLng explicit
    //  (serialization will drop negative 0s).
    public LatLng asGeoPoint() {
        return ((LatLng) boxed);
    }

    /**
     * The last element and only the last element may be deep truncated. If it is, then the array
     * iself is not shallow truncated, and vice versa. Thus, a truncated array is not empty.
     */
    @SuppressWarnings("unchecked")
    public ImmutableList<IndexValue> asArray() {
        return (ImmutableList<IndexValue>) boxed;
    }

    /**
     * A map key index value is always type {@link Type#STRING}.
     *
     * <p>The last key/value pair and only the last pair may contain a deep truncated key or value. If
     * and only if the key is truncated the value may be {#link #ABSENT}. If the last key/value pair
     * does contain a deep truncated key or value, then the map iself is not shallow truncated, and
     * vice versa. Thus, a truncated map is not empty.
     */
    @SuppressWarnings("unchecked")
    public ImmutableMap<IndexValue, IndexValue> asMap() {
        return (ImmutableMap<IndexValue, IndexValue>) boxed;
    }

    public Value.LegacyUser asLegacyUser() {
        return ((Value.LegacyUser) boxed);
    }

    /**
     * Returns a shallow truncated index value that is otherwise equivalent to this index value, which
     * must not be deep truncated. Used only by tests.
     */
    @VisibleForTesting
    public IndexValue truncate() {
        checkState(!isDeepTruncated());
        switch (type) {
            case STRING:
                return IndexValue.createString(true, asString());
            case BYTES:
                return IndexValue.createBytes(true, asBytes());
            case ARRAY:
                ImmutableList<IndexValue> array = asArray();
                checkState(!array.isEmpty());
                return IndexValue.createArray(true, array);
            case MAP:
                ImmutableMap<IndexValue, IndexValue> map = asMap();
                checkState(!map.isEmpty());
                return IndexValue.createMap(true, map);
            case ENTITY_REF:
                // Truncate the segments array.
                IndexValue.EntityRef entityRef = asEntityRef();
                IndexValue truncatedSegments =
                        entityRef.segments().asArray().isEmpty() ? ABSENT : entityRef.segments().truncate();
                IndexValue.EntityRef truncatedEntityRef =
                        EntityRef.create(entityRef.databaseRef(), entityRef.namespaceId(), truncatedSegments);
                return IndexValue.createEntityRef(truncatedEntityRef);
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean equals(Object otherObject) {
        if (!(otherObject instanceof IndexValue)) {
            return false;
        }
        IndexValue other = (IndexValue) otherObject;
        return (type == other.type)
                && (isDeepTruncated == other.isDeepTruncated)
                && (isShallowTruncated == other.isShallowTruncated)
                && (isNumberDouble == other.isNumberDouble)
                && (unboxed == other.unboxed)
                && boxed.equals(other.boxed);
    }

    @Override
    public int hashCode() {
        int result = 31 + Boolean.hashCode(isDeepTruncated);
        result = 31 * result + Boolean.hashCode(isShallowTruncated);
        result = 31 * result + type.hashCode();
        result = 31 * result + Boolean.hashCode(isNumberDouble);
        result = 31 * result + Long.hashCode(unboxed);
        result = 31 * result + boxed.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (type != null) {
            switch (type) {
                case ABSENT_TYPE:
                    // Return rather than break to avoid explicitly noting that ABSENT is truncated.
                    return "<absent>";
                case NULL:
                    stringBuilder.append("null");
                    break;
                case BOOLEAN:
                    stringBuilder.append(asBoolean());
                    break;
                case NUMBER:
                    if (isNumberDouble) {
                        stringBuilder.append(asNumberDouble());
                    } else {
                        stringBuilder.append(asNumberLong());
                    }
                    break;
                case TIMESTAMP:
                    stringBuilder.append(TextFormat.shortDebugString(asTimestamp()));
                    break;
                case ENTITY_REF:
                    stringBuilder.append(asEntityRef());
                    break;
                case STRING:
                    stringBuilder.append("\"").append(asString()).append("\"");
                    break;
                case BYTES:
                    stringBuilder.append(asBytes());
                    break;
                case GEO_POINT:
                    stringBuilder.append(TextFormat.shortDebugString(asGeoPoint()));
                    break;
                case ARRAY:
                    stringBuilder.append(asArray());
                    break;
                case MAP:
                    stringBuilder.append(asMap());
                    break;
                case LEGACY_USER:
                    stringBuilder.append(asLegacyUser());
                    break;
            }
        }
        if (isShallowTruncated) {
            stringBuilder.append(" (truncated)");
        }
        return stringBuilder.toString();
    }
}