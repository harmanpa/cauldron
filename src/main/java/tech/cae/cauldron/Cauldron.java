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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.Document;
import org.bson.types.ObjectId;
import tech.cae.cauldron.db.FileDeserializer;
import tech.cae.cauldron.db.FileSerializer;
import tech.cae.cauldron.db.MongoArtifactStore;
import tech.cae.cauldron.db.MongoQueueCore;
import tech.cae.cauldron.tasks.CauldronStatus;
import tech.cae.cauldron.tasks.CauldronTask;
import tech.cae.cauldron.worker.CauldronWorker;

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

    public CauldronWorker startWorker(Class<? extends CauldronTask>... taskTypes) {
        return new CauldronWorker(this, 4, taskTypes);
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
     * Blocking call to get task of one of an array of given types
     *
     * @param taskTypes
     * @return
     */
    public MultiTaskResponse pollMulti(Class<? extends CauldronTask>... taskTypes) {
        Map<String, Class<? extends CauldronTask>> typeMap = new HashMap<>(taskTypes.length);
        for (Class<? extends CauldronTask> taskType : taskTypes) {
            typeMap.put(taskType.getName(), taskType);
        }
        Document doc = null;
        while (doc == null) {
            doc = queue.get(new Document("type", new Document("$in", typeMap.keySet())), 1000);
        }
        Class<? extends CauldronTask> taskType = typeMap.get(doc.getString("type"));
        return new MultiTaskResponse(taskType, deserialize(doc, taskType));
    }

    public static class MultiTaskResponse {

        private final Class<? extends CauldronTask> taskType;
        private final CauldronTask task;

        MultiTaskResponse(Class<? extends CauldronTask> taskType, CauldronTask task) {
            this.taskType = taskType;
            this.task = task;
        }

        public Class<? extends CauldronTask> getTaskType() {
            return taskType;
        }

        public CauldronTask getTask() {
            return task;
        }

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

        public String getId() {
            return id;
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

    public TaskMeta getTaskMeta(String id) {
        Document message = collection.find(new Document("_id", new ObjectId(id))).first();
        if (message == null) {
            return null;
        }
        TaskMeta meta = new TaskMeta();
        meta.setId(message.getObjectId("_id").toHexString());
        meta.setType(message.get("payload", Document.class).getString("type"));
        meta.setPriority(message.getDouble("priority"));
        meta.setProgress(message.getDouble("progress"));
        meta.setCreated(message.getDate("created"));
        meta.setEarliestGet(message.getDate("earliestGet"));
        meta.setResetTimestamp(message.getDate("resetTimestamp"));
        meta.setStatus(message.getString("status"));
        meta.setLog(message.get("log", BsonArray.class).stream().map((bv) -> bv.asString().getValue()).collect(Collectors.toList()));
        return meta;
    }

    public Iterable<TaskMeta> getTasksMetaData() {
        return getTasksMetaData(null, new HashMap<>());
    }

    public Iterable<TaskMeta> getTasksMetaData(String status) {
        return getTasksMetaData(status, new HashMap<>());
    }

    public Iterable<TaskMeta> getTasksMetaData(Map<String, String> payloadQuery) {
        return getTasksMetaData(null, payloadQuery);
    }

    public Iterable<TaskMeta> getTasksMetaData(String status, Map<String, String> payloadQuery) {
        return () -> {
            Document query = status == null ? new Document() : new Document("status", status);
            payloadQuery.forEach((key, value) -> query.append("payload." + key, value));
            Iterator<Document> it = collection.find(query).iterator();
            return new Iterator<TaskMeta>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public TaskMeta next() {
                    Document message = it.next();
                    TaskMeta meta = new TaskMeta();
                    meta.setId(message.getObjectId("_id").toHexString());
                    meta.setType(message.get("payload", Document.class).getString("type"));
                    meta.setPriority(message.getDouble("priority"));
                    meta.setProgress(message.getDouble("progress"));
                    meta.setCreated(message.getDate("created"));
                    meta.setEarliestGet(message.getDate("earliestGet"));
                    meta.setResetTimestamp(message.getDate("resetTimestamp"));
                    meta.setStatus(message.getString("status"));
                    meta.setLog(message.get("log", BsonArray.class).stream().map((bv) -> bv.asString().getValue()).collect(Collectors.toList()));
                    return meta;
                }
            };
        };
    }

    public static class TaskMeta {

        @JsonProperty
        private String id;
        @JsonProperty
        private String type;
        @JsonProperty
        private String status;
        @JsonProperty
        private Date created;
        @JsonProperty
        private Date earliestGet;
        @JsonProperty
        private Date resetTimestamp;
        @JsonProperty
        private double priority;
        @JsonProperty
        private double progress;
        @JsonProperty
        private List<String> log;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Date getCreated() {
            return created;
        }

        public void setCreated(Date created) {
            this.created = created;
        }

        public Date getEarliestGet() {
            return earliestGet;
        }

        public void setEarliestGet(Date earliestGet) {
            this.earliestGet = earliestGet;
        }

        public Date getResetTimestamp() {
            return resetTimestamp;
        }

        public void setResetTimestamp(Date resetTimestamp) {
            this.resetTimestamp = resetTimestamp;
        }

        public double getPriority() {
            return priority;
        }

        public void setPriority(double priority) {
            this.priority = priority;
        }

        public double getProgress() {
            return progress;
        }

        public void setProgress(double progress) {
            this.progress = progress;
        }

        public List<String> getLog() {
            return log;
        }

        public void setLog(List<String> log) {
            this.log = log;
        }

    }

    public MongoQueueCore getQueue() {
        return queue;
    }
}
