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

import com.google.firebase.firestore.Query;
import com.google.firebase.firestore.auth.User;
import com.google.firebase.firestore.model.Document;
import com.google.firebase.firestore.model.DocumentKey;
import com.google.firebase.firestore.model.ResourcePath;
import com.google.firebase.firestore.util.OrderedCode;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/** A persisted implementation of IndexManager. */
final class SQLiteIndexManager implements IndexManager {
  public static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final byte[] SEPARATOR = new byte[] {0};
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

  private byte[] encodeFilterPath(List<IndexComponent> path) {
    OrderedCode orderedCode = new OrderedCode();
    for (IndexComponent component : path) {
      orderedCode.writeBytes(component.fieldPath.canonicalString().getBytes(UTF_8));
      orderedCode.writeBytes(SEPARATOR);
      orderedCode.writeBytes(
          new byte[] {component.direction.equals(Query.Direction.ASCENDING) ? (byte) 0 : (byte) 1});
      orderedCode.writeBytes(SEPARATOR);
    }
    return orderedCode.getEncodedBytes();
  }

  @Override
  public Iterable<DocumentKey> getDocumentsMatchingConstraints(
      ResourcePath collectionPath, List<IndexComponent> constrains) {
    return null;
  }
}
