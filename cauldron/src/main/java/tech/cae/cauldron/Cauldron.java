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
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import tech.cae.cauldron.api.CauldronConfiguration;
import tech.cae.cauldron.api.CauldronConfigurationProvider;
import tech.cae.cauldron.api.CauldronStatus;
import tech.cae.cauldron.api.CauldronTask;
import tech.cae.cauldron.api.CauldronTaskTypeProvider;
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
    private Distributor distributor;
    private StatusChangeMonitor changeMonitor;

    public static Cauldron get() {
        if (INSTANCE == null) {
            INSTANCE = new Cauldron();
        }
        return INSTANCE;
    }

    Cauldron() {
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
    }

    <T extends CauldronTask> Document serialize(T object) {
        JsonNode node = mapper.valueToTree(object);
        return mapper.convertValue(node, Document.class);
    }

    <T extends CauldronTask> T deserialize(Document document, Class<T> objectClass) {
        return mapper.convertValue(document, objectClass);
    }

    public Distributor getDistributor() throws CauldronException {
        if (distributor == null) {
            distributor = new Distributor(this, CauldronTaskTypeProvider.getAllTaskTypes());
        }
        return distributor;
    }

    StatusChangeMonitor getChangeMonitor() {
        if (changeMonitor == null) {
            changeMonitor = new StatusChangeMonitor(collection, this);
            changeMonitor.start();
        }
        return changeMonitor;
    }

    MongoQueueCore getMongoQueue() {
        return queue;
    }

    public <T extends CauldronTask> SubmitResponse submit(T task) {
        return submit(task, 0, Arrays.asList());
    }

    <T extends CauldronTask> SubmitResponse submit(T task, long delay, List<String> parents) {
        return new SubmitResponse(queue.send(serialize(task), Date.from(Instant.now().plusMillis(delay)), 0.0, parents));
    }

    public <T extends CauldronTask> SubmitResponse resubmit(String id) throws CauldronException {
        Iterator<Document> it = collection.find(new Document("_id", new ObjectId(id))).iterator();
        if (it.hasNext()) {
            Document message = it.next();
            return new SubmitResponse(queue.requeue(message, new Date(), 0.0));
        }
        throw new CauldronException("No such task");
    }

    public <T extends CauldronTask> List<SubmitResponse> submitMulti(List<T> tasks) {
        return queue.sendMulti(
                tasks.stream().map(task -> serialize(task))
                        .collect(Collectors.toList()), new Date(), 0.0).stream()
                .map(id -> new SubmitResponse(id)).collect(Collectors.toList());
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
        return deserializeMeta(collection.find(new Document("_id", new ObjectId(id))).first());
    }

    private TaskMeta deserializeMeta(Document message) {
        if (message == null) {
            return null;
        }
        TaskMeta meta = new TaskMeta();
        meta.setId(message.getObjectId("_id").toHexString());
        meta.setType(message.get("payload", Document.class).getString("type"));
        meta.setPriority(message.getDouble("priority"));
        meta.setProgress(message.getDouble("progress"));
        meta.setCreated(message.getDate("created"));
        meta.setResetTimestamp(message.getDate("resetTimestamp"));
        meta.setStatus(message.getString("status"));
        meta.setAttempt(message.getInteger("attempt"));
        return meta;
    }

    private Iterable<TaskMeta> deserializeMeta(Iterable<Document> messages) {
        return () -> deserializeMeta(messages.iterator());
    }

    private Iterator<TaskMeta> deserializeMeta(Iterator<Document> messages) {
        return new Iterator<TaskMeta>() {
            @Override
            public boolean hasNext() {
                return messages.hasNext();
            }

            @Override
            public TaskMeta next() {
                return deserializeMeta(messages.next());
            }
        };
    }

    public List<String> getTaskLogs(String id) {
        Document message = collection.find(new Document("_id", new ObjectId(id))).first();
        if (message == null) {
            return Arrays.asList();
        }
        return message.getList("log", String.class, Arrays.asList());
    }

    public Iterable<TaskMeta> getTasksMetaData() {
        return getTasksMetaData(Arrays.asList(), new HashMap<>());
    }

    public Iterable<TaskMeta> getTasksMetaData(String status) {
        return getTasksMetaData(status, new HashMap<>());
    }

    public Iterable<TaskMeta> getTasksMetaData(Map<String, String> payloadQuery) {
        return getTasksMetaData(Arrays.asList(), payloadQuery);
    }

    public Iterable<TaskMeta> getTasksMetaData(String status, Map<String, String> payloadQuery) {
        return getTasksMetaData(Arrays.asList(status), payloadQuery);
    }

    public Iterable<TaskMeta> getTasksMetaData(List<String> statuses, Map<String, String> payloadQuery) {
        Document query = statuses == null || statuses.isEmpty()
                ? new Document()
                : (statuses.size() == 1
                ? new Document("status", statuses.get(0))
                : new Document("status", new Document("$in", new BsonArray(statuses.stream().map(s -> new BsonString(s)).collect(Collectors.toList())))));
        payloadQuery.forEach((key, value) -> query.append("payload." + key, value));
        return deserializeMeta(collection.find(query));
    }

    Iterable<TaskMeta> getTasksMetaData(Collection<String> ids) {
        BsonArray idArray = new BsonArray(ids.stream().map((id) -> new BsonObjectId(new ObjectId(id))).collect(Collectors.toList()));
        return deserializeMeta(collection.find(new Document("_id", new Document("$in", idArray))));
    }

    /**
     * Returns a CompletableFuture that completes on task completion or failure,
     * and returns the task as it's payload.
     *
     * @param id Task id
     * @return
     */
    public CompletableFuture<CauldronTask> getCompletion(String id) {
        return getChangeMonitor().getCompletion(id);
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
