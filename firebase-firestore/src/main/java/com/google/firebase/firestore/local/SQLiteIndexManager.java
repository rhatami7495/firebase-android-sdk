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

import android.database.Cursor;
import androidx.annotation.Nullable;
import com.google.firebase.firestore.auth.User;
import com.google.firebase.firestore.core.Query;
import com.google.firebase.firestore.index.OrderedCodeReader;
import com.google.firebase.firestore.index.OrderedCodeWriter;
import com.google.firebase.firestore.model.Document;
import com.google.firebase.firestore.model.DocumentKey;
import com.google.firebase.firestore.model.FieldPath;
import com.google.firebase.firestore.model.ResourcePath;
import com.google.firebase.firestore.util.Function;
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
  private final User user;

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
  public void addDocument(Document document) {
    db.query("SELECT index_id, field_paths FROM index_configuration WHERE parent_path = ?")
        .binding(document.getKey().getPath().popLast().canonicalString())
        .forEach(
            row -> {
              int indexId = row.getInt(0);
              IndexDefinition components = decodeFilterPath(row.getBlob(1));

              List<Value> values = new ArrayList<>();
              for (IndexComponent component : components) {
                Value field = document.getField(component.fieldPath);
                if (field == null) return;
                values.add(field);
              }

              db.execute(
                  "INSERT OR IGNORE INTO field_index ("
                      + "index_id, "
                      + "index_value, "
                      + "document_id ) VALUES(?, ?, ?)",
                  indexId,
                  encodeValues(components, values),
                  document.getKey().getPath().getLastSegment());
            });
  }

  @Override
  public void enableIndex(ResourcePath collectionPath, IndexDefinition index) {
    int currentMax =
        db.query("SELECT MAX(index_id) FROM index_configuration")
            .firstValue(
                new Function<Cursor, Integer>() {
                  @javax.annotation.Nullable
                  @Override
                  public Integer apply(@javax.annotation.Nullable Cursor input) {
                    return input.isNull(0) ? 0 : input.getInt(0);
                  }
                });
    db.execute(
        "INSERT OR IGNORE INTO index_configuration ("
            + "uid, "
            + "parent_path, "
            + "field_paths, " // field path, direction pairs
            + "index_id) VALUES(?, ?, ?, ?)",
        user.getUid(),
        collectionPath.canonicalString(),
        encodeFilterPath(index),
        currentMax);
  }

  private byte[] encodeFilterPath(IndexDefinition index) {
    OrderedCodeWriter orderedCode = new OrderedCodeWriter();
    for (IndexComponent component : index) {
      orderedCode.writeUtf8Ascending(component.fieldPath.canonicalString());
      orderedCode.writeUnsignedLongAscending(
          component.getType().equals(IndexComponent.IndexType.ASC) ? 0 : 1);
    }
    return orderedCode.encodedBytes();
  }

  private IndexDefinition decodeFilterPath(byte[] bytes) {
    IndexDefinition components = new IndexDefinition();
    OrderedCodeReader orderedCodeReader = new OrderedCodeReader(bytes);
    while (orderedCodeReader.hasRemainingBytes()) {
      String fieldPath = orderedCodeReader.readUtf8Ascending();
      long direction = orderedCodeReader.readUnsignedLongAscending();
      components.add(
          new IndexComponent(
              FieldPath.fromServerFormat(fieldPath),
              direction == 0 ? IndexComponent.IndexType.ASC : IndexComponent.IndexType.DESC));
    }
    return components;
  }

  @Override
  @Nullable
  public Iterable<DocumentKey> getDocumentsMatchingQuery(Query query) {
    ResourcePath parentPath = query.getPath();
    List<IndexManager.IndexDefinition> indexComponents = query.getIndexComponents();
    List<Value> lowerBound = query.getLowerBound();
    boolean lowerInclusive = query.isLowerInclusive();
    List<Value> upperBound = query.getUpperBound();
    boolean upperInclusive = query.isUpperInclusive();
    for (IndexManager.IndexDefinition index : indexComponents) {
      Integer indexId =
          db.query(
                  "SELECT index_id FROM index_configuration WHERE parent_path = ? AND field_paths = ?")
              .binding(parentPath.canonicalString(), encodeFilterPath(index))
              .firstValue(row -> row.getInt(0));

      if (indexId == null) continue;

      // Could we do a join here and return the documents?
      ArrayList<DocumentKey> documents = new ArrayList<>();

      if (lowerBound != null && upperBound != null) {
        db.query(
                "SELECT document_id from field_index WHERE index_id = ? AND index_value "
                    + (lowerInclusive ? ">=" : ">")
                    + " ? && index_value "
                    + (upperInclusive ? ">=" : "=")
                    + " ?")
            .binding(indexId, encodeValues(index, lowerBound), encodeValues(index, upperBound))
            .forEach(
                row -> documents.add(DocumentKey.fromPath(parentPath.append(row.getString(0)))));
      } else if (lowerBound != null) {
        db.query(
                "SELECT document_id from field_index WHERE index_id = ? AND index_value "
                    + (lowerInclusive ? ">=" : ">")
                    + "  ?")
            .binding(indexId, encodeValues(index, lowerBound))
            .forEach(
                row -> documents.add(DocumentKey.fromPath(parentPath.append(row.getString(0)))));
      } else {
        db.query(
                "SELECT document_id from field_index WHERE index_id = ? AND index_value "
                    + (upperInclusive ? ">=" : ">")
                    + "  ?")
            .binding(indexId, encodeValues(index, upperBound))
            .forEach(
                row -> documents.add(DocumentKey.fromPath(parentPath.append(row.getString(0)))));
      }
      return documents;
    }
    return null;
  }

  private byte[] encodeValues(IndexDefinition index, List<Value> values) {
    IndexByteEncoder indexByteEncoder = new IndexByteEncoder();
    for (int i = 0; i < index.size(); ++i) {
      FirestoreIndexValueWriter.INSTANCE.writeIndexValue(
          values.get(i), indexByteEncoder.forDirection(index.get(i).type));
    }
    return indexByteEncoder.getEncodedBytes();
  }
}
