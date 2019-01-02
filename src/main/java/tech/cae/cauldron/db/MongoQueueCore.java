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
package tech.cae.cauldron.db;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 *
 * @author https://github.com/infon-zed/mongo-queue-java
 * @author https://github.com/gaillard/mongo-queue-java
 *
 */
public final class MongoQueueCore {

    private final MongoCollection<Document> collection;

    public MongoQueueCore(final MongoCollection<Document> collection) {
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
     * Get a non running message from queue with a wait of 3 seconds and poll of
     * 200 milliseconds
     *
     * @param query query where top level fields do not contain operators. Lower
     * level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid
     * {$and: [{...}, {...}]}. Should not be null.
     * @param resetDuration duration in seconds before this message is
     * considered abandoned and will be given with another call to get()
     * @return message or null
     */
    public Document get(final Document query, final int resetDuration) {
        return get(query, resetDuration, 3000, 200);
    }

    /**
     * Get a non running message from queue with a poll of 200 milliseconds
     *
     * @param query query where top level fields do not contain operators. Lower
     * level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid
     * {$and: [{...}, {...}]}. Should not be null.
     * @param resetDuration duration in seconds before this message is
     * considered abandoned and will be given with another call to get()
     * @param waitDuration duration in milliseconds to keep polling before
     * returning null
     * @return message or null
     */
    public Document get(final Document query, final int resetDuration, final int waitDuration) {
        return get(query, resetDuration, waitDuration, 200);
    }

    /**
     * Get a non running message from queue
     *
     * @param query query where top level fields do not contain operators. Lower
     * level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3}, invalid
     * {$and: [{...}, {...}]}. Should not be null.
     * @param resetDuration duration in seconds before this message is
     * considered abandoned and will be given with another call to get()
     * @param waitDuration duration in milliseconds to keep polling before
     * returning null
     * @param pollDuration duration in milliseconds between poll attempts
     * @return message or null
     */
    @SuppressWarnings("SleepWhileInLoop")
    public Document get(final Document query, final int resetDuration, final int waitDuration, long pollDuration) {
        Objects.requireNonNull(query);

        //reset stuck messages
        collection.updateMany(new Document("status", "running").append("resetTimestamp", new Document("$lte", new Date())),
                new Document("$set", new Document("status", "queued")),
                new UpdateOptions().upsert(false));

        final Document builtQuery = new Document("status", "queued");
        query.entrySet().forEach((field) -> {
            builtQuery.append("payload." + field.getKey(), field.getValue());
        });

        builtQuery.append("earliestGet", new Document("$lte", new Date()));

        final Calendar calendar = Calendar.getInstance();

        calendar.add(Calendar.SECOND, resetDuration);
        final Date resetTimestamp = calendar.getTime();

        final Document sort = new Document("priority", 1).append("created", 1);
        final Document update = new Document("$set", new Document("status", "running").append("resetTimestamp", resetTimestamp));
        final Document fields = new Document("payload", 1);

        calendar.setTimeInMillis(System.currentTimeMillis());
        calendar.add(Calendar.MILLISECOND, waitDuration);
        final Date end = calendar.getTime();

        while (true) {
            // final Document message = (Document) collection.findAndModify(builtQuery, fields, sort, false, update, true, false);
            FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions().sort(sort).upsert(false).returnDocument(ReturnDocument.AFTER).projection(fields);
            final Document message = collection.findOneAndUpdate(builtQuery, update, opts);
            if (message != null) {
                final ObjectId id = message.getObjectId("_id");
                return ((Document) message.get("payload")).append("id", id.toHexString());
            }

            if (new Date().compareTo(end) >= 0) {
                return null;
            }

            try {
                if (pollDuration == 0) {
                    continue;
                }
                Thread.sleep(pollDuration);
            } catch (final InterruptedException ex) {
                throw new RuntimeException(ex);
            } catch (final IllegalArgumentException ex) {
                pollDuration = 0;
            }
        }
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
                new Document("$set", new Document("status", status)));

        //bump any blocked messages onto queue or mark as failed
        collection.updateMany(new Document("status", "blocked").append("parent", id),
                new Document("$set", new Document("status", "completed".equals(status) ? "queued" : "failed")),
                new UpdateOptions().upsert(false));
    }

    /**
     * Ack message and send payload to queue, atomically, with earliestGet as
     * Now and 0.0 priority
     *
     * @param message message to ack received from get(). Should not be null
     * @param payload payload to send. Should not be null
     */
    public void ackSend(final Document message, final Document payload) {
        ackSend(message, payload, new Date());
    }

    /**
     * Ack message and send payload to queue, atomically, with 0.0 priority
     *
     * @param message message to ack received from get(). Should not be null
     * @param payload payload to send. Should not be null
     * @param earliestGet earliest instant that a call to get() can return
     * message. Should not be null
     */
    public void ackSend(final Document message, final Document payload, final Date earliestGet) {
        ackSend(message, payload, earliestGet, 0.0);
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
                .append("created", new Date()));

        //using upsert because if no documents found then the doc was removed (SHOULD ONLY HAPPEN BY SOMEONE MANUALLY) so we can just send
        //collection.update(new Document("_id", id), newMessage, true, false);
        collection.updateOne(Filters.eq("_id", new ObjectId(id)), newMessage, new UpdateOptions().upsert(true));
    }

    /**
     * Requeue message with earliestGet as Now and 0.0 priority. Same as
     * ackSend() with the same message.
     *
     * @param message message to requeue received from get(). Should not be null
     */
    public void requeue(final Document message) {
        requeue(message, new Date());
    }

    /**
     * Requeue message with 0.0 priority. Same as ackSend() with the same
     * message.
     *
     * @param message message to requeue received from get(). Should not be null
     * @param earliestGet earliest instant that a call to get() can return
     * message. Should not be null
     */
    public void requeue(final Document message, final Date earliestGet) {
        requeue(message, earliestGet, 0.0);
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
     * Send message to queue with earliestGet as Now and 0.0 priority
     *
     * @param payload payload. Should not be null
     * @return hex string of the message id
     */
    public String send(final Document payload) {
        return send(payload, new Date());
    }

    /**
     * Send message to queue with 0.0 priority
     *
     * @param payload payload. Should not be null
     * @param earliestGet earliest instant that a call to Get() can return
     * message. Should not be null
     * @return hex string of the message id
     */
    public String send(final Document payload, final Date earliestGet) {
        return send(payload, earliestGet, 0.0);
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
                .append("created", new Date());

        collection.insertOne(message);
        return message.getObjectId("_id").toHexString();
    }

    public void progress(String id, String log, double progress) {
//        final Calendar calendar = Calendar.getInstance();
//
//        calendar.add(Calendar.SECOND, resetDuration);
//        final Date resetTimestamp = calendar.getTime();
//        final Document update = new Document("$set", new Document("status", "running").append("resetTimestamp", resetTimestamp));
        //new Document("$push", new Document("log", new Document("$each", new String[]{log})))
    }

    private void ensureIndex(final Document index) {
        for (int i = 0; i < 5; ++i) {
            for (String name = UUID.randomUUID().toString(); name.length() > 0; name = name.substring(0, name.length() - 1)) {
                //creating an index with the same name and different spec does nothing.
                //creating an index with different name and same spec does nothing.
                //so we use any generated name, and then find the right spec after we have called, and just go with that name.

                IndexOptions iOpts = new IndexOptions().background(true).name(name);
                collection.createIndex(index, iOpts);

                for (final Document existingIndex : collection.listIndexes()) {

                    if (existingIndex.get("key").equals(index)) {
                        return;
                    }
                }
            }
        }

        throw new RuntimeException("couldnt create index after 5 attempts");
    }
}
