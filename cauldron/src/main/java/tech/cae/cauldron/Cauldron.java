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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import tech.cae.cauldron.api.CauldronConfiguration;
import tech.cae.cauldron.api.CauldronConfigurationProvider;
import tech.cae.cauldron.api.CauldronStatus;
import tech.cae.cauldron.api.CauldronTask;
import tech.cae.cauldron.api.exceptions.CauldronException;

/**
 *
 * @author Pete
 */
public class Cauldron {

    private static final Logger LOG = Logger.getLogger(Cauldron.class.getName());
    private static Cauldron INSTANCE;

    private final ObjectMapper mapper;
    private final MongoQueueCore queue;
    private final MongoCollection<Document> collection;
    private final ScheduledExecutorService completionMonitorService;
    private final ConcurrentMap<String, CompletableFuture<CauldronTask>> futures;
    private boolean completionCheckScheduled;

    public static Cauldron get() throws CauldronException {
        if (INSTANCE == null) {
            INSTANCE = new Cauldron();
        }
        return INSTANCE;
    }

    Cauldron() throws CauldronException {
        this(CauldronConfigurationProvider.get());
    }

    Cauldron(CauldronConfiguration configuration) {
        this((configuration.getDbUri() != null
                ? new MongoClient(new MongoClientURI(configuration.getDbUri()))
                : new MongoClient(configuration.getDbHost(), configuration.getDbPort()))
                .getDatabase(configuration.getDbName()),
                configuration.getDbCollection());
    }

    Cauldron(MongoDatabase database, String queueCollection) {
        this.mapper = new ObjectMapper();
        this.collection = database.getCollection(queueCollection);
        this.queue = new MongoQueueCore(collection);
        this.queue.ensureGetIndex();
        this.completionMonitorService = Executors.newSingleThreadScheduledExecutor();
        this.futures = new ConcurrentHashMap<>();
        this.completionCheckScheduled = false;
    }

    <T extends CauldronTask> Document serialize(T object) {
        JsonNode node = mapper.valueToTree(object);
        return mapper.convertValue(node, Document.class);
    }

    <T extends CauldronTask> T deserialize(Document document, Class<T> objectClass) {
        return mapper.convertValue(document, objectClass);
    }

    /**
     * Blocking call to get task of given type
     *
     * @param <T>
     * @param taskType
     * @return
     */
    public <T extends CauldronTask> T pollScheduler(Class<T> taskType) {
        LOG.info("Requesting a task");
        Document doc = null;
        while (doc == null) {
            doc = queue.get(new Document("type", taskType.getName()), 1000, 1000, 100, true, "");
        }
        LOG.info("Returning a task");
        return deserialize(doc, taskType);
    }

    /**
     * Blocking call to get task of given type
     *
     * @param <T>
     * @param taskType
     * @param worker
     * @return
     */
    public <T extends CauldronTask> T pollWorker(Class<T> taskType, String worker) {
        LOG.info("Requesting a task");
        Document doc = null;
        while (doc == null) {
            doc = queue.get(new Document("type", taskType.getName()), 1000, 1000, 100, false, worker);
        }
        LOG.info("Returning a task");
        return deserialize(doc, taskType);
    }

    /**
     * Blocking call to get task of one of an array of given types
     *
     * @param taskTypes
     * @return
     */
    public MultiTaskResponse pollMultiScheduler(Collection<Class<? extends CauldronTask>> taskTypes) {
        Map<String, Class<? extends CauldronTask>> typeMap = new HashMap<>(taskTypes.size());
        taskTypes.forEach((taskType) -> {
            typeMap.put(taskType.getName(), taskType);
        });
        Document doc = null;
        while (doc == null) {
            doc = queue.get(new Document("type", new Document("$in", typeMap.keySet())), 1000, 1000, 100, true, "");
        }
        Class<? extends CauldronTask> taskType = typeMap.get(doc.getString("type"));
        return new MultiTaskResponse(taskType, deserialize(doc, taskType));
    }

    /**
     * Blocking call to get task of one of an array of given types
     *
     * @param taskTypes
     * @param worker
     * @return
     */
    public MultiTaskResponse pollMultiWorker(Collection<Class<? extends CauldronTask>> taskTypes, String worker) {
        Map<String, Class<? extends CauldronTask>> typeMap = new HashMap<>(taskTypes.size());
        taskTypes.forEach((taskType) -> {
            typeMap.put(taskType.getName(), taskType);
        });
        Document doc = null;
        while (doc == null) {
            doc = queue.get(new Document("type", new Document("$in", typeMap.keySet())), 1000, 1000, 100, false, worker);
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
        return new SubmitResponse(queue.send(serialize(task), new Date(), 0.0));
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

    public void progress(String id, Collection<String> log, double progress, int resetDuration, String worker) {
        queue.progress(id, log, progress, resetDuration, worker);
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

    /**
     * Get progress information for specific task
     *
     * @param id Task id
     * @return
     */
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
        meta.setAttempt(message.getInteger("attempt"));
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
                    meta.setAttempt(message.getInteger("attempt"));
                    return meta;
                }
            };
        };
    }

    private Iterable<TaskMeta> getTasksMetaData(Collection<String> ids) {
        return () -> {
            BsonArray idArray = new BsonArray(ids.stream().map((id) -> new BsonObjectId(new ObjectId(id))).collect(Collectors.toList()));
            Iterator<Document> it = collection.find(new Document("_id", new Document("$in", idArray))).iterator();
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
                    return meta;
                }
            };
        };
    }

    /**
     * Returns a CompletableFuture that completes on task completion or failure,
     * and returns the task as it's payload.
     *
     * @param id Task id
     * @return
     */
    public CompletableFuture<CauldronTask> getCompletion(String id) {
        if (futures.containsKey(id)) {
            return futures.get(id);
        }
        CompletableFuture<CauldronTask> future = new CompletableFuture<>();
        CompletableFuture<CauldronTask> otherFuture = futures.putIfAbsent(id, future);
        if (otherFuture == null) {
            if (!completionCheckScheduled) {
                scheduleCompletionCheck();
            }
            return future.thenApply(t -> {
                futures.remove(id);
                return t;
            });
        }
        return otherFuture;
    }

    private void scheduleCompletionCheck() {
        LOG.info("Scheduling completion check");
        completionCheckScheduled = true;
        completionMonitorService.schedule(() -> {
            LOG.info("Running completion check");
            // Fetch the data for the futures
            getTasksMetaData(new ArrayList<>(futures.keySet())).forEach((task) -> {
                switch (task.getStatus()) {
                    case Completed:
                    case Failed:
                    case Cancelled:
                        // Notify any completed or failed tasks and remove
                        futures.remove(task.getId()).complete(getTask(task.getId(), CauldronTask.class));
                        break;
                    default:
                }
            });
            // If futures is not empty schedule again
            if (!futures.isEmpty()) {
                scheduleCompletionCheck();
            } else {
                completionCheckScheduled = false;
            }
            LOG.info("Finished completion check");
        }, 2, TimeUnit.SECONDS);
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
        private int attempt;

        TaskMeta() {
        }

        public String getId() {
            return id;
        }

        void setId(String id) {
            this.id = id;
        }

        public String getType() {
            return type;
        }

        void setType(String type) {
            this.type = type;
        }

        public CauldronStatus getStatus() {
            return CauldronStatus.fromString(status);
        }

        void setStatus(String status) {
            this.status = status;
        }

        public Date getCreated() {
            return created;
        }

        void setCreated(Date created) {
            this.created = created;
        }

        public Date getEarliestGet() {
            return earliestGet;
        }

        void setEarliestGet(Date earliestGet) {
            this.earliestGet = earliestGet;
        }

        public Date getResetTimestamp() {
            return resetTimestamp;
        }

        void setResetTimestamp(Date resetTimestamp) {
            this.resetTimestamp = resetTimestamp;
        }

        public double getPriority() {
            return priority;
        }

        void setPriority(double priority) {
            this.priority = priority;
        }

        public double getProgress() {
            return progress;
        }

        void setProgress(double progress) {
            this.progress = progress;
        }

        public int getAttempt() {
            return attempt;
        }

        void setAttempt(int attempt) {
            this.attempt = attempt;
        }

    }

}
