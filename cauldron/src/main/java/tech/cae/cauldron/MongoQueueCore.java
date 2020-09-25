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

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 *
 * @author https://github.com/infon-zed/mongo-queue-java
 * @author https://github.com/gaillard/mongo-queue-java
 *
 */
final class MongoQueueCore {

    private static final Logger LOG = Logger.getLogger(MongoQueueCore.class.getName());

    private final MongoCollection<Document> collection;

    MongoQueueCore(final MongoCollection<Document> collection) {
        Objects.requireNonNull(collection);

        this.collection = collection;
    }

    /**
     * Ensure index for get() method with no fields before or after sort fields
     */
    public void ensureGetIndex() {
        ensureGetIndex(new Document());
    }

    /**
     * Ensure index for get() method with no fields after sort fields
     *
     * @param beforeSort fields in get() call that should be before the sort
     * fields in the index. Should not be null
     */
    public void ensureGetIndex(final Document beforeSort) {
        ensureGetIndex(beforeSort, new Document());
    }

    /**
     * Ensure index for get() method
     *
     * @param beforeSort fields in get() call that should be before the sort
     * fields in the index. Should not be null
     * @param afterSort fields in get() call that should be after the sort
     * fields in the index. Should not be null
     */
    public void ensureGetIndex(final Document beforeSort, final Document afterSort) {
        Objects.requireNonNull(beforeSort);
        Objects.requireNonNull(afterSort);

        //using general rule: equality, sort, range or more equality tests in that order for index
        final Document completeIndex = new Document("status", 1);

        beforeSort.entrySet().stream().map((field) -> {
            if (!Objects.equals(field.getValue(), 1) && !Objects.equals(field.getValue(), -1)) {
                throw new IllegalArgumentException("field values must be either 1 or -1");
            }
            return field;
        }).forEachOrdered((field) -> {
            completeIndex.append("payload." + field.getKey(), field.getValue());
        });

        completeIndex.append("priority", 1).append("created", 1);

        afterSort.entrySet().stream().map((field) -> {
            if (!Objects.equals(field.getValue(), 1) && !Objects.equals(field.getValue(), -1)) {
                throw new IllegalArgumentException("field values must be either 1 or -1");
            }
            return field;
        }).forEachOrdered((field) -> {
            completeIndex.append("payload." + field.getKey(), field.getValue());
        });

        completeIndex.append("earliestGet", 1);

        ensureIndex(completeIndex);//main query in Get()
        ensureIndex(new Document("status", 1).append("resetTimestamp", 1));//for the stuck messages query in Get()
    }

    /**
     * Ensure index for count() method
     *
     * @param index fields in count() call. Should not be null
     * @param includeRunning whether running was given to count() or not
     */
    public void ensureCountIndex(final Document index, final boolean includeRunning) {
        Objects.requireNonNull(index);

        final Document completeIndex = new Document();

        if (includeRunning) {
            completeIndex.append("status", 1);
        }

        index.entrySet().stream().map((field) -> {
            if (!Objects.equals(field.getValue(), 1) && !Objects.equals(field.getValue(), -1)) {
                throw new IllegalArgumentException("field values must be either 1 or -1");
            }
            return field;
        }).forEachOrdered((field) -> {
            completeIndex.append("payload." + field.getKey(), field.getValue());
        });

        ensureIndex(completeIndex);
    }

    /**
     * Get a non running message from queue
     *
     * @param query query where top level fields do not contain operators. Lower
     * level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid
     * {$and: [{...}, {...}]}. Should not be null.
     * @param resetDuration duration in seconds before this message is
     * considered abandoned and will be given with another call to get()
     * @param waitDuration duration in milliseconds to poll
     * @param pollAttempts number of attempts to poll between resetting
     * @return message or null
     */
    @SuppressWarnings("SleepWhileInLoop")
    public Document get(final Document query, final int resetDuration, final int waitDuration, int pollAttempts, boolean scheduler, String worker) {
        Objects.requireNonNull(query);

        //reset stuck messages
        collection.updateMany(new Document("status", "running").append("resetTimestamp", new Document("$lte", new Date())),
                new Document("$set", new Document("status", "queued")).append("$inc", new Document("attempt", 1)),
                new UpdateOptions().upsert(false));

        final Calendar calendar = Calendar.getInstance();

        calendar.add(Calendar.SECOND, resetDuration);
        final Date resetTimestamp = calendar.getTime();

        final Document sort = new Document("priority", 1).append("created", 1);
        final Document update = new Document("$set", new Document("status", "running").append("resetTimestamp", resetTimestamp).append("progress", 0.0));
        final Document fields = new Document("payload", 1);

        for (int pollAttempt = 0; pollAttempt < pollAttempts; pollAttempt++) {
            final Document builtQuery = new Document("status", "queued");
            query.entrySet().forEach((field) -> {
                builtQuery.append("payload." + field.getKey(), field.getValue());
            });
            builtQuery.append("earliestGet", new Document("$lte", new Date()));
            FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions().sort(sort).upsert(false).returnDocument(ReturnDocument.AFTER).projection(fields);
            LOG.log(Level.INFO, "Querying: {0}", builtQuery.toJson());
            final Document message = collection.findOneAndUpdate(builtQuery, update, opts);
            if (message != null) {
                final ObjectId id = message.getObjectId("_id");
                return ((Document) message.get("payload")).append("id", id.toHexString());
            }
            try {
                Thread.sleep(waitDuration);
            } catch (InterruptedException | IllegalArgumentException ex) {
                return null;
            }
        }
        return null;
    }

    /**
     * Count in queue, running true or false
     *
     * @param query query where top level fields do not contain operators. Lower
     * level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid
     * {$and: [{...}, {...}]}. Should not be null
     * @return count
     */
    public long count(final Document query) {
        Objects.requireNonNull(query);

        final Document completeQuery = new Document();

        query.entrySet().forEach((field) -> {
            completeQuery.append("payload." + field.getKey(), field.getValue());
        });

        return collection.count(completeQuery);
    }

    /**
     * Count in queue
     *
     * @param query query where top level fields do not contain operators. Lower
     * level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid
     * {$and: [{...}, {...}]}. Should not be null
     * @param status count messages of status
     * @return count
     */
    public long count(final Document query, final String status) {
        Objects.requireNonNull(query);

        final Document completeQuery = new Document("status", status);

        query.entrySet().forEach((field) -> {
            completeQuery.append("payload." + field.getKey(), field.getValue());
        });

        return collection.count(completeQuery);
    }

    /**
     * Acknowledge a message was processed and remove from queue
     *
     * @param message message received from get(). Should not be null.
     * @param status "completed" or "failed"
     */
    public void ack(final Document message, final String status) {
        Objects.requireNonNull(message);
        final String id = message.getString("id");

        collection.findOneAndUpdate(new Document("_id", new ObjectId(id)),
                new Document("$set", new Document("status", status).append("payload", message)));

        //bump any blocked messages onto queue or mark as failed
        collection.updateMany(new Document("status", "blocked").append("parent", id),
                new Document("$set", new Document("status", "completed".equals(status) ? "queued" : "failed")),
                new UpdateOptions().upsert(false));
    }

    /**
     * Ack message and send payload to queue, atomically
     *
     * @param message message to ack received from get(). Should not be null
     * @param payload payload to send. Should not be null
     * @param earliestGet earliest instant that a call to get() can return
     * message. Should not be null
     * @param priority priority for order out of get(). 0 is higher priority
     * than 1. Should not be NaN
     */
    public void ackSend(final Document message, final Document payload, final Date earliestGet, final double priority) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(earliestGet);
        if (Double.isNaN(priority)) {
            throw new IllegalArgumentException("priority was NaN");
        }

        final String id = message.getString("id");

        final Document newMessage = new Document("$set", new Document("payload", payload)
                .append("status", "queued")
                .append("resetTimestamp", new Date(Long.MAX_VALUE))
                .append("earliestGet", earliestGet)
                .append("priority", priority)
                .append("created", new Date()))
                .append("log", new BsonArray())
                .append("progress", 0.0)
                .append("attempt", 0);

        //using upsert because if no documents found then the doc was removed (SHOULD ONLY HAPPEN BY SOMEONE MANUALLY) so we can just send
        //collection.update(new Document("_id", id), newMessage, true, false);
        collection.updateOne(Filters.eq("_id", new ObjectId(id)), newMessage, new UpdateOptions().upsert(true));
    }

    /**
     * Requeue message. Same as ackSend() with the same message.
     *
     * @param message message to requeue received from get(). Should not be null
     * @param earliestGet earliest instant that a call to get() can return
     * message. Should not be null
     * @param priority priority for order out of get(). 0 is higher priority
     * than 1. Should not be NaN
     */
    public void requeue(final Document message, final Date earliestGet, final double priority) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(earliestGet);
        if (Double.isNaN(priority)) {
            throw new IllegalArgumentException("priority was NaN");
        }

        final String id = message.getString("id");

        final Document forRequeue = new Document(message);
        forRequeue.remove("id");
        ackSend(message, forRequeue, earliestGet, priority);
    }

    /**
     * Send message to queue
     *
     * @param payload payload. Should not be null
     * @param earliestGet earliest instant that a call to Get() can return
     * message. Should not be null
     * @param priority priority for order out of Get(). 0 is higher priority
     * than 1. Should not be NaN
     * @return hex string of the message id
     */
    public String send(final Document payload, final Date earliestGet, final double priority) {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(earliestGet);
        if (Double.isNaN(priority)) {
            throw new IllegalArgumentException("priority was NaN");
        }
        final Document message = new Document("payload", payload)
                .append("status", "queued")
                .append("resetTimestamp", new Date(Long.MAX_VALUE))
                .append("earliestGet", earliestGet)
                .append("priority", priority)
                .append("created", new Date())
                .append("log", new BsonArray())
                .append("progress", 0.0)
                .append("attempt", 0);
        LOG.log(Level.INFO, "Inserting: {0}", message.toJson());
        collection.insertOne(message);
        return message.getObjectId("_id").toHexString();
    }

    public void progress(String id, Collection<String> log, double progress, int resetDuration, String worker) {
        final Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, resetDuration);
        final Date resetTimestamp = calendar.getTime();
        Document setters = new Document("status", "running").append("resetTimestamp", resetTimestamp).append("worker", worker);
        if (progress >= 0.0) {
            setters.append("progress", progress);
        }
        Document update = new Document("$set", setters);
        if (!log.isEmpty()) {
            update.append("$push", new Document("log", new Document("$each", new BsonArray(log.stream().map((s) -> new BsonString(s)).collect(Collectors.toList())))));
        }
        collection.findOneAndUpdate(Filters.eq("_id", new ObjectId(id)), update);
    }

    private void ensureIndex(final Document index) {
        for (int i = 0; i < 5; ++i) {
            for (String name = UUID.randomUUID().toString(); name.length() > 0; name = name.substring(0, name.length() - 1)) {
                //creating an index with the same name and different spec does nothing.
                //creating an index with different name and same spec does nothing.
                //so we use any generated name, and then find the right spec after we have called, and just go with that name.

                IndexOptions iOpts = new IndexOptions().background(true).name(name);
                for (final Document existingIndex : collection.listIndexes()) {

                    if (existingIndex.get("key").equals(index)) {
                        return;
                    }
                }
                collection.createIndex(index, iOpts);
            }
        }
        // Just warn, it will still work
        LOG.warning("couldnt create index after 5 attempts");
    }
}
