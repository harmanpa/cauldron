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
package tech.cae.cauldron.cloudrun.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import tech.cae.cauldron.api.CauldronCallback;
import tech.cae.cauldron.api.CauldronTask;
import tech.cae.cauldron.api.exceptions.CauldronException;
import tech.cae.cauldron.cloudrun.base.CloudRunConfiguration;
import tech.cae.cauldron.cloudrun.base.CloudRunConfigurationProvider;
import tech.cae.cauldron.cloudrun.base.CloudRunResponse;
import tech.cae.cauldron.cloudrun.base.CloudRunUtilities;

/**
 *
 * @author peter
 */
public class WorkerResource {

    private final ThreadLocal<WorkerCallback> callback;
    private final Publisher publisher;
    private final String name;

    public WorkerResource() throws IOException, CauldronException {
        CloudRunConfiguration configuration = CloudRunConfigurationProvider.get();
        this.callback = new ThreadLocal<>();
        this.publisher = Publisher.newBuilder(
                ProjectTopicName.of(configuration.getProjectId(),
                        configuration.getResponseTopic())).build();
        this.name = UUID.randomUUID().toString();
    }

    @POST
    @Path("/")
    public Response performTask(PubsubMessage message) {
        try {
            CauldronTask task = CloudRunUtilities.deserializeTask(message);
            try {
                callback.set(new WorkerCallback(task.getId(), this.publisher, this.name));
                task.run(callback.get());
                callback.get().complete(task);
            } catch (CauldronException ex) {
                callback.get().fail(task, ex);
            } finally {
                callback.remove();
            }
            return Response.status(200).build();
        } catch (IOException ex) {
            return Response.status(500).build();
        }
    }

    static class WorkerCallback implements CauldronCallback {

        private final String taskId;
        private final Publisher publisher;
        private long lastLog;
        private double progress;
        private final List<String> logs;
        private CauldronTask task;
        private boolean success;
        private final String name;

        WorkerCallback(String taskId, Publisher publisher, String name) {
            this.taskId = taskId;
            this.publisher = publisher;
            this.lastLog = 0L;
            this.progress = -1.0;
            this.logs = new ArrayList<>();
            this.name = name;
        }

        @Override
        public void log(String message) {
            logs.add(message);
            log(false);
        }

        @Override
        public void progress(String message, double progress) {
            logs.add(message);
            this.progress = progress;
            log(progress >= 1.0);
        }

        @Override
        public void progress(double progress) {
            this.progress = progress;
            log(progress >= 1.0);
        }

        public void complete(CauldronTask task) {
            this.task = task;
            this.success = true;
            log(true);
        }

        public void fail(CauldronTask task, CauldronException ex) {
            this.task = task;
            this.success = false;
            log(ex.getMessage());
            log(true);
        }

        private void log(boolean force) {
            if (force || System.currentTimeMillis() - lastLog > 1000) {
                CloudRunResponse response = new CloudRunResponse(taskId, task, progress, new ArrayList<>(logs), success, name);
                send(response);
                logs.clear();
                lastLog = System.currentTimeMillis();
            }
        }

        private void send(CloudRunResponse response) {
            try {
                ApiFuture<String> future = this.publisher.publish(CloudRunUtilities.serializeResponse(response));
            } catch (JsonProcessingException ex) {
                Logger.getLogger(WorkerResource.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }
}
