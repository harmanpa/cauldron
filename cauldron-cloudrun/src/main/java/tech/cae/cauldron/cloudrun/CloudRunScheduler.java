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
package tech.cae.cauldron.cloudrun;

import tech.cae.cauldron.cloudrun.base.CloudRunResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import tech.cae.cauldron.Cauldron;
import tech.cae.cauldron.api.CauldronStatus;
import tech.cae.cauldron.api.CauldronTask;
import tech.cae.cauldron.api.CauldronTaskTypeProvider;
import tech.cae.cauldron.api.exceptions.CauldronException;
import tech.cae.cauldron.cloudrun.base.CloudRunConfiguration;
import tech.cae.cauldron.cloudrun.base.CloudRunConfigurationProvider;
import tech.cae.cauldron.cloudrun.base.CloudRunUtilities;

/**
 *
 * @author peter
 */
public class CloudRunScheduler {

    private static CloudRunScheduler INSTANCE;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Cauldron cauldron;
    private final Publisher publisher;
    private final Subscriber subscriber;
    private final Collection<Class<? extends CauldronTask>> types;

    public static void start() throws CauldronException {
        if (INSTANCE == null) {
            INSTANCE = new CloudRunScheduler();
        }
    }

    public static void stop() {
        if (INSTANCE != null) {
            INSTANCE.shutdown();
        }
    }

    CloudRunScheduler() throws CauldronException {
        try {
            CloudRunConfiguration configuration = CloudRunConfigurationProvider.get();
            this.publisher = Publisher.newBuilder(
                    ProjectTopicName.of(configuration.getProjectId(),
                            configuration.getScheduleTopic())).build();
            this.cauldron = Cauldron.get();
            this.types = CauldronTaskTypeProvider.getAllTaskTypes();
            this.subscriber = Subscriber.newBuilder(ProjectSubscriptionName.of(
                    configuration.getProjectId(),
                    configuration.getResponseTopic()), new CloudRunSchedulerMessageReceiver()).build();
            this.subscriber.startAsync();
            this.executor.submit(new CloudRunSchedulerRunnable());
        } catch (IOException ex) {
            throw new CauldronException("Failure setting up cloud run scheduler", ex);
        }
    }

    class CloudRunSchedulerRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    CauldronTask task = cauldron.pollMultiScheduler(types).getTask();
                    publisher.publish(CloudRunUtilities.serializeTask(task)).get();
                } catch (JsonProcessingException | InterruptedException | ExecutionException ex) {
                    Logger.getLogger(CloudRunScheduler.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    class CloudRunSchedulerMessageReceiver implements MessageReceiver {

        @Override
        public void receiveMessage(PubsubMessage pm, AckReplyConsumer arc) {
            try {
                CloudRunResponse response = CloudRunUtilities.deserializeResponse(pm);
                // Response could be progress/logging, completion, or failure
                if (response.getTask() != null) {
                    if (response.isSuccess()) {
                        cauldron.completed(response.getTask(), CauldronStatus.Completed);
                    } else {
                        cauldron.completed(response.getTask(), CauldronStatus.Failed);
                    }
                } else {
                    cauldron.progress(response.getTaskId(), response.getLog(), response.getProgress(), 1000, response.getWorker());
                }
                arc.ack();
            } catch (IOException ex) {
                Logger.getLogger(CloudRunScheduler.class.getName()).log(Level.SEVERE, null, ex);
                arc.nack();
            }
        }
    }

    void shutdown() {
        executor.shutdownNow();
        subscriber.stopAsync();
        publisher.shutdown();
    }

}
