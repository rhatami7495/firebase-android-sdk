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

import com.google.common.collect.ImmutableList;
import com.google.firestore.v1.Value;
import com.google.protobuf.Timestamp;
import com.google.type.LatLng;

/**
     * Firestore index value writer.
     *
     * <p>See the <a
     * href="https://g3doc.corp.google.com/cloud/datastore/g3doc/architecture/spanner/storage_format.md">storage
     * format design</a> for details.
     */
    public class FirestoreIndexValueWriter {

        public static final FirestoreIndexValueWriter INSTANCE = new FirestoreIndexValueWriter();

        // Also used by class FirestoreIndexValueReader.
        static boolean isLastIndexValueEntityRefSegmentCollectionId(ImmutableList<IndexValue> segments) {
            return ((segments.size() % 2) == 1);
        }

        private FirestoreIndexValueWriter() {}

        // The write methods below short-circuit writing terminators for values containing a (terminating)
        // truncated value.
        //
        // As an example, consider the resulting encoding for:
        //
        // ["bar", [2, "foo"]] -> (STRING, "bar", TERM, ARRAY, NUMBER, 2, STRING, "foo", TERM, TERM, TERM)
        // ["bar", [2, truncated("foo")]] -> (STRING, "bar", TERM, ARRAY, NUMBER, 2, STRING, "foo", TRUNC)
        // ["bar", truncated(["foo"])] -> (STRING, "bar", TERM, ARRAY. STRING, "foo", TERM, TRUNC)

        /**
         * Writes an index value.
         *
         * @param contextPartialEntityRef the partition, parent path, and kind of the index entry for
         *     which entity we are encoding this value. Parent path and kind are only set if they have
         *     been encoded in the index value before this point (or index def's {@link IndexDef#kind()}
         *     is available even if kind is not encoded in index value).
         */
        public void writeIndexValue(
                IndexValue value,
                ContextPartialEntityRef contextPartialEntityRef,
                DirectionalIndexByteEncoder encoder) {
            writeIndexValueAux(value, contextPartialEntityRef, encoder);
        }

    // StorageFormat labels for value types in indexes.
    // This should match the ordering implied by IndexValue.value_type.
    enum ValueTypeLabel {  // Maybe use FieldORder enum
        NULL_TYPE ( 5),
        BOOLEAN (10),
        NAN_TYPE(13),
        NUMBER_TYPE(15),
        DATE(20),
        STRING(25),
        BYTES(30),

        // A resource name in a service with a name that is lexicographically
        // smaller than "firestore.googleapis.com".
        RESOURCE_NAME_LT_FIRESTORE(33),

        // A Firestore resource name.
        RESOURCE_NAME(37),

        // A resource name in a service with a name that is lexicographically
        // greater than "firestore.googleapis.com".
        RESOURCE_NAME_GT_FIRESTORE(41),

        GEO_POINT(45),
        ARRAY(50),
        MAP(55),

        // A numeric segment in a resource name path.
        PATH_SEGMENT_ID(58),

        // A string segment in a resource name path.
        PATH_SEGMENT_NAME(60);
        private int valueId;

        ValueTypeLabel(int valueId) {

            this.valueId = valueId;
        }

       
    }


    private boolean writeIndexValueAux(
                IndexValue indexValue,
                ContextPartialEntityRef contextPartialEntityRef,
                DirectionalIndexByteEncoder encoder) {
            switch (indexValue.type()) {
                case NULL:
                    writeValueTypeLabel(encoder, ValueTypeLabel.NULL_TYPE);
                    return false;
                case BOOLEAN:
                    writeValueTypeLabel(encoder, ValueTypeLabel.BOOLEAN);
                    encoder.writeLong(indexValue.asBoolean() ? 1 : 0);
                    return false;
                case NUMBER:
                    if (indexValue.isNumberDouble()) {
                        double number = indexValue.asNumberDouble();
                        // TODO: Make sure NaN works
                        if (Double.isNaN(number)) {
                            writeValueTypeLabel(encoder, ValueTypeLabel.NAN_TYPE);
                            return false;
                        }
                        writeValueTypeLabel(encoder, ValueTypeLabel.NUMBER_TYPE);
                        encoder.writeNumber(number);
                    } else {
                        writeValueTypeLabel(encoder, ValueTypeLabel.NUMBER_TYPE);
                        long number = indexValue.asNumberLong();
                        encoder.writeNumber(number);
                    }
                    return false;
                case TIMESTAMP:
                    Timestamp timestamp = indexValue.asTimestamp();
                    writeValueTypeLabel(encoder, ValueTypeLabel.DATE);
                    encoder.writeLong(timestamp.getSeconds());
                    encoder.writeLong(timestamp.getNanos());
                    return false;
                case STRING:
                    return writeIndexString(indexValue, encoder);
                case BYTES:
                    writeValueTypeLabel(encoder, ValueTypeLabel.BYTES);
                    encoder.writeBytes(indexValue.asBytes());
                    return writeTruncationMarker(encoder, indexValue.isShallowTruncated());
                case ENTITY_REF:
                    return writeIndexEntityRef(indexValue, contextPartialEntityRef, encoder);
                case GEO_POINT:
                    LatLng geoPoint = indexValue.asGeoPoint();
                    writeValueTypeLabel(encoder, ValueTypeLabel.GEO_POINT);
                    encoder.writeDouble(geoPoint.getLatitude());
                    encoder.writeDouble(geoPoint.getLongitude());
                    return false;
                case MAP:
                    return writeIndexMap(indexValue, contextPartialEntityRef, encoder);
                case ARRAY:
                    return writeIndexArray(indexValue, contextPartialEntityRef, encoder);
                default:
                    throw new IllegalArgumentException("unknown index value type " + indexValue.type());
            }
        }

        private void writeUnlabeledString(String string, DirectionalIndexByteEncoder encoder) {
            encoder.writeString(string);
            writeTruncationMarker(encoder, false);
        }

        private boolean writeIndexString(
                IndexValue stringIndexValue, DirectionalIndexByteEncoder encoder) {
            writeValueTypeLabel(encoder, ValueTypeLabel.STRING);
            return writeUnlabeledIndexString(stringIndexValue, encoder);
        }

        private boolean writeUnlabeledIndexString(
                IndexValue stringIndexValue, DirectionalIndexByteEncoder encoder) {
            encoder.writeString(stringIndexValue.asString());
            return writeTruncationMarker(encoder, stringIndexValue.isShallowTruncated());
        }

        private boolean writeIndexMap(
                IndexValue mapIndexValue,
                ContextPartialEntityRef contextPartialEntityRef,
                DirectionalIndexByteEncoder encoder) {
            writeValueTypeLabel(encoder, ValueTypeLabel.MAP);
            for (ImmutableMap.Entry<IndexValue, IndexValue> entry : mapIndexValue.asMap().entrySet()) {
                IndexValue key = entry.getKey();
                IndexValue value = entry.getValue();
                if (writeIndexString(key, encoder)) {
                    return true;
                }
                if (value.isAbsent()) {
                    return writeTruncationMarker(encoder, true);
                }
                if (writeIndexValueAux(value, contextPartialEntityRef, encoder)) {
                    return true;
                }
            }
            return writeTruncationMarker(encoder, mapIndexValue.isShallowTruncated());
        }

        private boolean writeIndexArray(
                IndexValue arrayIndexValue,
                ContextPartialEntityRef contextPartialEntityRef,
                DirectionalIndexByteEncoder encoder) {
            writeValueTypeLabel(encoder, ValueTypeLabel.ARRAY);
            for (IndexValue element : arrayIndexValue.asArray()) {
                if (writeIndexValueAux(element, contextPartialEntityRef, encoder)) {
                    return true;
                }
            }
            return writeTruncationMarker(encoder, arrayIndexValue.isShallowTruncated());
        }

        private boolean writeIndexEntityRef(
                IndexValue entityRefIndexValue,
                ContextPartialEntityRef contextPartialEntityRef,
                DirectionalIndexByteEncoder encoder) {
            encoder.recordEncodesEntityRef();
            IndexValue.EntityRef indexValueEntityRef = entityRefIndexValue.asEntityRef();
            writeValueTypeLabel(encoder, ValueTypeLabel.RESOURCE_NAME);
            // TODO(jbaum): This if statement ensures that the new byte encoding of an empty
            // entity reference matches the old encoding.  Remove as practical, if ever.
            if (indexValueEntityRef.equals(IndexValue.EntityRef.EMPTY)) {
                return writeTruncationMarker(encoder, true);
            }
            IndexValue.EntityRef contextEntityRef = contextPartialEntityRef.indexValueEntityRef();
            PartitionEncodingLabel label =
                    computePartitionLabelFromIndexValueAndContext(contextEntityRef, indexValueEntityRef);
            encoder.writeLong(label.getNumber());
            switch (label) {
                case PROJECTS_LT:
                case PROJECTS_GT:
                    return writeIndexEntityRefRelRoot(indexValueEntityRef, encoder);
                case LOCAL_DATABASE_LT:
                case LOCAL_DATABASE_GT:
                    return writeIndexEntityRefRelDatabase(indexValueEntityRef, encoder);
                case LOCAL_NAMESPACE:
                    return writeIndexEntityRefRelPartition(contextEntityRef, indexValueEntityRef, encoder);
                default:
                    throw new IllegalArgumentException("unexpected resource name label " + label);
            }
        }

        /**
         * Compares the partition of an entity reference index value to the partition of a context partial
         * entity reference and returns the appropriate PartitionEncodingLabel tag depending on how much
         * they have in common.
         */
        private PartitionEncodingLabel computePartitionLabelFromIndexValueAndContext(
                IndexValue.EntityRef contextEntityRef, IndexValue.EntityRef indexValueEntityRef) {
            DatabaseRef contextDatabaseRef = contextEntityRef.databaseRef();
            DatabaseRef indexValueDatabaseRef = indexValueEntityRef.databaseRef();
            int projectIdComparison =
                    indexValueDatabaseRef.projectId().compareTo(contextDatabaseRef.projectId());
            int databaseIdComparison =
                    indexValueDatabaseRef.databaseId().compareTo(contextDatabaseRef.databaseId());
            if ((projectIdComparison < 0) || ((projectIdComparison == 0) && (databaseIdComparison < 0))) {
                return PartitionEncodingLabel.PROJECTS_LT;
            }
            if ((projectIdComparison > 0) || ((projectIdComparison == 0) && (databaseIdComparison > 0))) {
                return PartitionEncodingLabel.PROJECTS_GT;
            }
            IndexValue namespaceId = indexValueEntityRef.namespaceId();
            if (indexValueEntityRef.namespaceId().isAbsent()) {
                // This behavior is bonkers - but matches the proto version for rollout reasons. This should
                // really be truncation aware.
                // TODO(b/122264442): Change this to correct/simplier behavior by instead returning
                // PartitionEncodingLabel.LOCAL_DATABASE_LT;
                namespaceId = IndexValue.EMPTY_STRING;
            }
            int namespaceIdComparison =
                    compareIndexValueToContext(namespaceId, contextEntityRef.namespaceId());
            if (namespaceIdComparison < 0) {
                return PartitionEncodingLabel.LOCAL_DATABASE_LT;
            }
            if (namespaceIdComparison > 0) {
                return PartitionEncodingLabel.LOCAL_DATABASE_GT;
            }
            return PartitionEncodingLabel.LOCAL_NAMESPACE;
        }

        private boolean writeIndexEntityRefRelRoot(
                IndexValue.EntityRef indexValueEntityRef, DirectionalIndexByteEncoder encoder) {
            DatabaseRef indexValueDatabaseRef = indexValueEntityRef.databaseRef();
            writeUnlabeledString(indexValueDatabaseRef.projectId(), encoder);
            encoder.writeLong(DatabaseEncodingLabel.DATABASES.getNumber());
            writeUnlabeledString(indexValueDatabaseRef.databaseId(), encoder);
            return writeIndexEntityRefRelDatabase(indexValueEntityRef, encoder);
        }

        private boolean writeIndexEntityRefRelDatabase(
                IndexValue.EntityRef indexValueEntityRef, DirectionalIndexByteEncoder encoder) {
            if (indexValueEntityRef.namespaceId().isAbsent()) {
                return writeTruncationMarker(encoder, true);
            }
            if (indexValueEntityRef.namespaceId().asString().isEmpty()
                    && !indexValueEntityRef.namespaceId().isShallowTruncated()) {
                encoder.writeLong(NamespaceEncodingLabel.DEFAULT_NAMESPACE.getNumber());
            } else {
                encoder.writeLong(NamespaceEncodingLabel.NAMESPACE.getNumber());
                if (writeUnlabeledIndexString(indexValueEntityRef.namespaceId(), encoder)) {
                    return true;
                }
            }
            return writeIndexEntityRefRelSegments(indexValueEntityRef.segments(), 0, encoder);
        }

        /**
         * Encodes an entity reference path. If the path's leading elements match the context entity's
         * parent path, these segments are skipped. If the path's next element's resource id matches the
         * context entitys leaf collection id, it is also skipped.
         */
        private boolean writeIndexEntityRefRelPartition(
                IndexValue.EntityRef contextEntityRef,
                IndexValue.EntityRef indexValueEntityRef,
                DirectionalIndexByteEncoder encoder) {
            ImmutableList<IndexValue> contextSegments = contextEntityRef.segments().asArray();
            int relNumContextSegments;
            if (contextSegments.isEmpty()
                    || indexValueEntityRef.segments().isAbsent()
                    || indexValueEntityRef.segments().asArray().isEmpty()) {
                relNumContextSegments = 0;
            } else {
                // Skip the entity reference path segments that match the context.
                PathEncodingLabel label =
                        computeSegmentsLabelFromIndexValueAndContext(contextEntityRef, indexValueEntityRef);
                if (label != PathEncodingLabel.PATH_GT) {
                    encoder.writeLong(label.getNumber());
                }
                switch (label) {
                    case PATH_LT:
                    case PATH_GT:
                        relNumContextSegments = 0;
                        break;
                    case KIND_GT:
                    case KIND_LT:
                        checkArgument(isLastIndexValueEntityRefSegmentCollectionId(contextSegments));
                        relNumContextSegments = contextSegments.size() - 1;
                        break;
                    case PATH_EQ:
                        checkArgument(!isLastIndexValueEntityRefSegmentCollectionId(contextSegments));
                        relNumContextSegments = contextSegments.size();
                        break;
                    case KIND_EQ:
                        checkArgument(isLastIndexValueEntityRefSegmentCollectionId(contextSegments));
                        relNumContextSegments = contextSegments.size();
                        break;
                    default:
                        throw new IllegalArgumentException("unexpected resource name label " + label);
                }
            }
            // TODO(b/122264442): Change this to correct/simplier behavior by deleting this if statement.
            // The proto version of IndexValue did "implicit" truncation if the entity ref was truncated
            // in the middle of the base size - so the truncation marker was not present.
            if (indexValueEntityRef.segments().isAbsent() && indexValueEntityRef.namespaceId().isAbsent()) {
                return writeTruncationMarker(encoder, false);
            }
            return writeIndexEntityRefRelSegments(
                    indexValueEntityRef.segments(), relNumContextSegments, encoder);
        }

        // contextEntityRef.segments and indexValueEntityRef must not be empty or ABSENT.
        private PathEncodingLabel computeSegmentsLabelFromIndexValueAndContext(
                IndexValue.EntityRef contextEntityRef, IndexValue.EntityRef indexValueEntityRef) {
            ImmutableList<IndexValue> contextSegments = contextEntityRef.segments().asArray();
            ImmutableList<IndexValue> indexValueSegments = indexValueEntityRef.segments().asArray();
            int numContextSegments = contextSegments.size();
            int numIndexValueSegments = indexValueSegments.size();
            int numContextParentSegments;
            if (isLastIndexValueEntityRefSegmentCollectionId(contextSegments)) {
                // The context entity reference segments specify a full entity reference ending with the
                // collection id, which means the parent segment list omits that last segment.
                numContextParentSegments = numContextSegments - 1;
            } else {
                // The context entity reference path specifies a full entity reference, which means it is the
                // parent segment list.
                numContextParentSegments = numContextSegments;
            }
            for (int segmentIndex = 0; segmentIndex < numContextParentSegments; ++segmentIndex) {
                if (segmentIndex >= numIndexValueSegments) {
                    return PathEncodingLabel.PATH_LT;
                }
                IndexValue indexValueSegment = indexValueSegments.get(segmentIndex);
                IndexValue contextSegment = contextSegments.get(segmentIndex);
                int segmentComparison = compareIndexValueToContext(indexValueSegment, contextSegment);
                if (segmentComparison < 0) {
                    return PathEncodingLabel.PATH_LT;
                }
                if (segmentComparison > 0) {
                    return PathEncodingLabel.PATH_GT;
                }
            }
            // The context parent entity reference is a prefix of the index value entity reference.
            if (numContextParentSegments == numContextSegments) {
                // The context entity references does not specify the collection id.
                // The context entity reference is a prefix of the index value entity reference.
                return PathEncodingLabel.PATH_EQ;
            }
            // There is a context collection id.
            if (numContextParentSegments == numIndexValueSegments) {
                // There's no more to the index value, and thus the index value entity reference is a prefix
                // of the context entity.
                return PathEncodingLabel.KIND_LT;
            }
            // Both the context entity reference and the index value entity reference contain more than the
            // common parent entity reference.  (In the context case the additional segments are at most
            // a single collection id.)
            IndexValue indexValueCollectionIdSegment = indexValueSegments.get(numContextParentSegments);
            IndexValue contextCollectionIdSegment = contextSegments.get(numContextParentSegments);
            int collectionIdComparison =
                    compareIndexValueToContext(indexValueCollectionIdSegment, contextCollectionIdSegment);
            if (collectionIdComparison < 0) {
                // The next collection id in the index value is before the next collection id in the context.
                return PathEncodingLabel.KIND_LT;
            }
            if (collectionIdComparison > 0) {
                // The next collection id in the index value is after the next collection id in the context.
                return PathEncodingLabel.KIND_GT;
            }
            // The next collection id in the index value IS the next collection id in the context.
            return PathEncodingLabel.KIND_EQ;
        }

        private boolean writeIndexEntityRefRelSegments(
                IndexValue indexValueSegmentsIndexValue,
                int relNumSegments,
                DirectionalIndexByteEncoder encoder) {
            if (indexValueSegmentsIndexValue.isAbsent()) {
                return writeTruncationMarker(encoder, true);
            }
            ImmutableList<IndexValue> indexValueSegments = indexValueSegmentsIndexValue.asArray();
            int numSegments = indexValueSegments.size();
            for (int index = relNumSegments; index < numSegments; ++index) {
                IndexValue segment = indexValueSegments.get(index);
                if (writeIndexEntityRefSegment(segment, encoder)) {
                    return true;
                }
            }
            return writeTruncationMarker(encoder, indexValueSegmentsIndexValue.isShallowTruncated());
        }

        private boolean writeIndexEntityRefSegment(
                IndexValue segmentIndexValue, DirectionalIndexByteEncoder encoder) {
            switch (segmentIndexValue.type()) {
                case NUMBER:
                    writeValueTypeLabel(encoder, ValueTypeLabel.PATH_SEGMENT_ID);
                    encoder.writeLong(segmentIndexValue.asNumberLong());
                    return false;
                case STRING:
                    writeValueTypeLabel(encoder, ValueTypeLabel.PATH_SEGMENT_NAME);
                    return writeUnlabeledIndexString(segmentIndexValue, encoder);
                default:
                    throw new IllegalArgumentException("invalid segment type " + segmentIndexValue.type());
            }
        }

        private void writeValueTypeLabel(
                DirectionalIndexByteEncoder encoder, ValueTypeLabel valueId) {
            encoder.writeLong(valueId.valueId);
        }

        private boolean writeTruncationMarker(
                DirectionalIndexByteEncoder encoder, boolean isShallowTruncated) {
            encoder.writeLong(
                    (isShallowTruncated
                            ? TruncationLabel.TRUNCATED
                            : TruncationLabel.NOT_TRUNCATED)
                            .getNumber());
            return isShallowTruncated;
        }

        private int compareIndexValueToContext(IndexValue indexValue, IndexValue context) {
            // TODO(jbaum): Refactor to avoid depending directly on class FirestoreIndexValueComparator.
            return FirestoreIndexValueComparator.INSTANCE.compare(indexValue, context);
        }
    }
