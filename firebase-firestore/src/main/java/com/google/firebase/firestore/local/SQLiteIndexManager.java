// Copyright 2019 Google LLC
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

import static com.google.firebase.firestore.util.Assert.hardAssert;

import androidx.annotation.Nullable;
import com.google.firebase.firestore.auth.User;
import com.google.firebase.firestore.core.OrderBy;
import com.google.firebase.firestore.model.Document;
import com.google.firebase.firestore.model.DocumentKey;
import com.google.firebase.firestore.model.ResourcePath;
import com.google.firestore.v1.Value;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/** A persisted implementation of IndexManager. */
final class SQLiteIndexManager implements IndexManager {
  public static final Charset UTF_8 = Charset.forName("UTF-8");
  /**
   * An in-memory copy of the index entries we've already written since the SDK launched. Used to
   * avoid re-writing the same entry repeatedly.
   *
   * <p>This is *NOT* a complete cache of what's in persistence and so can never be used to satisfy
   * reads.
   */
  private final MemoryIndexManager.MemoryCollectionParentIndex collectionParentsCache =
      new MemoryIndexManager.MemoryCollectionParentIndex();

  private final SQLitePersistence db;
  private User user;

  SQLiteIndexManager(SQLitePersistence persistence, User user) {
    db = persistence;
    this.user = user;
  }

  @Override
  public void addToCollectionParentIndex(ResourcePath collectionPath) {
    hardAssert(collectionPath.length() % 2 == 1, "Expected a collection path.");

    if (collectionParentsCache.add(collectionPath)) {
      String collectionId = collectionPath.getLastSegment();
      ResourcePath parentPath = collectionPath.popLast();
      db.execute(
          "INSERT OR REPLACE INTO collection_parents "
              + "(collection_id, parent) "
              + "VALUES (?, ?)",
          collectionId,
          EncodedPath.encode(parentPath));
    }
  }

  @Override
  public List<ResourcePath> getCollectionParents(String collectionId) {
    ArrayList<ResourcePath> parentPaths = new ArrayList<>();
    db.query("SELECT parent FROM collection_parents WHERE collection_id = ?")
        .binding(collectionId)
        .forEach(
            row -> {
              parentPaths.add(EncodedPath.decodeResourcePath(row.getString(0)));
            });
    return parentPaths;
  }

  @Override
  public void addDocument(Document document) {}

  @Override
  public void enableIndex(ResourcePath collectionPath, List<IndexComponent> filters) {
    db.execute(
        "INSERT OR IGNORE index_configuration ("
            + "uid TEXT, "
            + "parent_path TEXT, "
            + "field_paths BLOB, " // field path, direction pairs
            + "index_id INTEGER) VALUES(?, ?, ?, (SELECT MAX(index_id) + 1 FROM index_configuration)",
        user.getUid(),
        collectionPath.canonicalString(),
        encodeFilterPath(filters));
  }

  @Override
  @Nullable
  public Integer getIndexId(ResourcePath collectionPath, List<IndexComponent> filters) {
    return db.query(
            "SELECT index_id FROM index_configuration WHERE parent_path = ? AND field_paths = ?")
        .binding(collectionPath.canonicalString(), encodeFilterPath(filters))
        .firstValue(row -> row.getInt(0));
  }

  private byte[] encodeFilterPath(List<IndexComponent> path) {
    OrderedCodeWriter orderedCode = new OrderedCodeWriter();
    for (IndexComponent component : path) {
      orderedCode.writeUtf8Ascending(component.fieldPath.canonicalString());
      orderedCode.writeNumberAscending(
          component.direction.equals(OrderBy.Direction.ASCENDING) ? 0 : 1);
    }
    return orderedCode.encodedBytes();
  }

  @Override
  public Iterable<DocumentKey> getDocumentsMatchingConstraints(
      ResourcePath parentPath, List<IndexComponent> filters, int indexId, List<Value> values) {

    ArrayList<DocumentKey> documents = new ArrayList<>();
    db.query("SELECT document_id from field_index WHERE index_id = ? AND index_value = ?")
        .binding(indexId, encodeValues(filters, values))
        .forEach(row -> documents.add(DocumentKey.fromPath(parentPath.append(row.getString(0)))));
    return documents;
  }

  private byte[] encodeValues(List<IndexComponent> filters, List<Value> values) {
    IndexByteEncoder indexByteEncoder = new IndexByteEncoder();
    for (int i = 0; i < filters.size(); ++i) {
      FirestoreIndexValueWriter.INSTANCE.writeIndexValue(
          values.get(i), indexByteEncoder.forDirection(filters.get(i).direction));
    }
    return indexByteEncoder.getEncodedBytes();
  }
}
