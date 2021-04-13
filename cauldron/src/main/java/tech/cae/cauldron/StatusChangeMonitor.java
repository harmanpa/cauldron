/*
 * The MIT License
 *
 * Copyright 2021 CAE Tech Limited.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package tech.cae.cauldron;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import tech.cae.cauldron.api.CauldronStatus;
import tech.cae.cauldron.api.CauldronStatusChangeListener;
import tech.cae.cauldron.api.CauldronTask;
import tech.cae.cauldron.api.exceptions.CauldronException;

/**
 *
 * @author peter
 */
class StatusChangeMonitor {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final MongoCollection<Document> collection;
    private final List<CauldronStatusChangeListener> listeners;
    private final ConcurrentMap<String, CompletableFuture<CauldronTask>> futures;
    private final Cauldron cauldron;

    StatusChangeMonitor(final MongoCollection<Document> collection, Cauldron cauldron) {
        Objects.requireNonNull(collection);
        this.collection = collection;
        this.listeners = new ArrayList<>();
        this.futures = new ConcurrentHashMap<>();
        this.cauldron = cauldron;
    }

    public void start() {
        executor.submit(() -> this.collection.watch().forEach(change -> onChange(change)));
    }

    public void addListener(CauldronStatusChangeListener listener) {
        this.listeners.add(listener);
    }

    public void removeListener(CauldronStatusChangeListener listener) {
        this.listeners.remove(listener);
    }

    public CompletableFuture<CauldronTask> getCompletion(String id) {
        if (futures.containsKey(id)) {
            return futures.get(id);
        }
        CompletableFuture<CauldronTask> future = new CompletableFuture<>();
        // Make a single attempt to fetch the futures, else just wait
        switch (cauldron.getTaskMeta(id).getStatus()) {
            case Completed:
            case Failed:
            case Cancelled:
                // Notify any completed or failed tasks and remove
                future.complete(cauldron.getTask(id, CauldronTask.class));
                return future;
            default:
        }
        CompletableFuture<CauldronTask> otherFuture = futures.putIfAbsent(id, future);
        future = otherFuture == null ? future : otherFuture;
        return future;
    }

    @SuppressWarnings("null")
    private void onChange(ChangeStreamDocument<Document> change) {
        if (change.getDocumentKey() != null && change.getDocumentKey().containsKey("_id")) {
            String id = change.getDocumentKey().getObjectId("_id").getValue().toHexString();
            switch (change.getOperationType()) {
                case INSERT:
                    if (change.getFullDocument() != null && change.getFullDocument().containsKey("status")) {
                        Document payload = change.getFullDocument().containsKey("payload")
                                ? bsonToDocument(change.getFullDocument().toBsonDocument().getDocument("payload"))
                                : null;
                        onChange(id, change.getFullDocument().getString("status"), payload);
                    }
                    break;
                case UPDATE:
                    if (change.getUpdateDescription().getUpdatedFields() != null && change.getUpdateDescription().getUpdatedFields().containsKey("status")) {
                        Document payload = change.getUpdateDescription().getUpdatedFields().containsKey("payload")
                                ? bsonToDocument(change.getUpdateDescription().getUpdatedFields().getDocument("payload"))
                                : null;
                        onChange(id, change.getUpdateDescription().getUpdatedFields().getString("status").getValue(), payload);
                    }
                    break;
                default:
                // do nothing
            }
        }
    }

    private void onChange(String id, String status, Document payload) {
        onChange(id, CauldronStatus.fromString(status), payload == null ? null : cauldron.deserialize(payload, CauldronTask.class));
    }

    private void onChange(String id, CauldronStatus status, CauldronTask payload) {
        this.listeners.forEach(listener -> listener.taskStatusChanged(id, status));
        if (this.futures.containsKey(id)) {
            switch (status) {
                case Completed:
                case Cancelled:
                case Failed:
                    this.futures.remove(id).complete(payload);
                    break;
            }
        }
    }

    public void stop() {
        this.executor.shutdownNow();
    }

    static Document bsonToDocument(BsonDocument bsonDocument) {
        DocumentCodec codec = new DocumentCodec();
        DecoderContext decoderContext = DecoderContext.builder().build();
        return codec.decode(new BsonDocumentReader(bsonDocument), decoderContext);
    }
}
