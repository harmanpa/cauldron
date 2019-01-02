/*
 * The MIT License
 *
 * Copyright 2019 CAE Tech Limited.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.util.Iterator;
import org.bson.Document;
import org.bson.types.ObjectId;
import tech.cae.cauldron.db.FileDeserializer;
import tech.cae.cauldron.db.FileSerializer;
import tech.cae.cauldron.db.MongoArtifactStore;
import tech.cae.cauldron.db.MongoQueueCore;
import tech.cae.cauldron.tasks.CauldronStatus;
import tech.cae.cauldron.tasks.CauldronTask;

/**
 *
 * @author Pete
 */
public class Cauldron {

    private final ObjectMapper mapper;
    private final MongoQueueCore queue;
    private final MongoArtifactStore store;
    private final MongoCollection<Document> collection;
    private final ThreadLocal<String> serializationId;
    private final ThreadLocal<String> deserializationId;

    /**
     *
     * @param workingDir
     * @param database
     * @param queueCollection
     * @param fileBucket
     */
    public Cauldron(File workingDir, MongoDatabase database, String queueCollection, String fileBucket) {
        this.mapper = new ObjectMapper();
        this.serializationId = new ThreadLocal<>();
        this.deserializationId = new ThreadLocal<>();
        this.store = new MongoArtifactStore(workingDir,
                database, fileBucket, serializationId, deserializationId);
        SimpleModule module = new SimpleModule();
        module.addSerializer(File.class, new FileSerializer(store));
        module.addDeserializer(File.class, new FileDeserializer(store));
        this.mapper.registerModule(module);
        this.collection = database.getCollection(queueCollection);
        this.queue = new MongoQueueCore(collection);
        this.queue.ensureGetIndex();
    }

    public static <T extends CauldronTask> void startWorker(String[] args, Class<T> taskType) {

    }

    <T extends CauldronTask> Document serialize(T object) {
        serializationId.set(object.getId());
        try {
            JsonNode node = mapper.valueToTree(object);
            return mapper.convertValue(node, Document.class);
        } finally {
            serializationId.remove();
        }
    }

    <T extends CauldronTask> T deserialize(Document document, Class<T> objectClass) {
        deserializationId.set(document.getString("id"));
        try {
            return mapper.convertValue(document, objectClass);
        } finally {
            deserializationId.remove();
        }
    }

    /**
     * Blocking call to get task of given type
     *
     * @param <T>
     * @param taskType
     * @return
     */
    public <T extends CauldronTask> T poll(Class<T> taskType) {
        Document doc = null;
        while (doc == null) {
            doc = queue.get(new Document("type", taskType.getName()), 1000);
        }
        return deserialize(doc, taskType);
    }

    /**
     *
     * @param <T>
     * @param task
     * @return
     */
    public <T extends CauldronTask> SubmitResponse submit(T task) {
        return new SubmitResponse(queue.send(serialize(task)));
    }

    /**
     *
     * @param <T>
     * @param task
     * @param status
     */
    public <T extends CauldronTask> void completed(T task, CauldronStatus status) {
        queue.ack(serialize(task), status.toString());
    }

    public class SubmitResponse {

        private final String id;

        SubmitResponse(String id) {
            this.id = id;
        }

        public <T extends CauldronTask> void then(T task) {
// TODO: Add a task blocked by the previous task
        }

    }

    /**
     *
     * @param <T>
     * @param id
     * @param taskType
     * @return
     */
    public <T extends CauldronTask> T getTask(String id, Class<T> taskType) {
        Iterator<Document> it = collection.find(new Document("_id", new ObjectId(id))).iterator();
        if (it.hasNext()) {
            Document message = it.next();
            Document payload = message.get("payload", Document.class);
            payload.put("id", message.getObjectId("_id").toHexString());
            return deserialize(payload, taskType);
        }
        return null;
    }

    /**
     *
     * @param <T>
     * @param taskType
     * @return
     */
    public <T extends CauldronTask> Iterable<T> getTasks(Class<T> taskType) {
        return () -> {
            Iterator<Document> it = collection.find(
                    new Document("payload.type", taskType.getName())).iterator();
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public T next() {
                    Document message = it.next();
                    Document payload = message.get("payload", Document.class);
                    payload.put("id", message.getObjectId("_id").toHexString());
                    return deserialize(payload, taskType);
                }
            };
        };
    }

    public static class TaskMeta {

    }
}
