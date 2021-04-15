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

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.bson.Document;
import tech.cae.cauldron.api.CauldronStatus;
import tech.cae.cauldron.api.CauldronStatusChangeListener;
import tech.cae.cauldron.api.CauldronTask;
import tech.cae.cauldron.api.exceptions.CauldronException;

/**
 *
 * @author peter
 */
public class Distributor implements CauldronStatusChangeListener {

    private final BlockingQueue<String> workerQueue = new LinkedBlockingDeque<>();
    private final BlockingQueue<CauldronTask> queue = new LinkedBlockingDeque<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Cauldron cauldron;
    private final Set<Class<? extends CauldronTask>> types;
    private final Document query;
    private boolean paused = false;
    private Future<?> task = null;
    private static final Logger LOG = Logger.getLogger(Distributor.class.getName());

    Distributor(Cauldron cauldron, Set<Class<? extends CauldronTask>> types) {
        this.cauldron = cauldron;
        this.types = types;
        this.query = new Document("type", new Document("$in", types.stream().map(type -> type.getName()).collect(Collectors.toList())));
        this.cauldron.getChangeMonitor().addListener(this);
        start();
    }

    public CauldronTask get(String worker) throws InterruptedException {
        LOG.info("Fetching for worker " + worker);
        this.workerQueue.add(worker);
        try {
            this.start();
            return this.queue.take();
        } finally {
            LOG.info("Returning to worker " + worker);
        }
    }

    public final synchronized void start() {
        this.paused = false;
        if (this.task == null || this.task.isDone()) {
            LOG.info("Starting distributor");
            this.task = this.executor.submit(() -> {
                while (!paused) {
                    try {
                        LOG.info(workerQueue.size() + " workers waiting");
                        String worker = this.workerQueue.take();
                        // Try n times
                        Document doc = this.cauldron.getMongoQueue().get(query, 30 * 60, 1000, 4, worker);
                        if (doc == null) {
                            // if you don't, put the worker back, and go to sleep until woken
                            this.workerQueue.add(worker);
                            this.paused = true;
                            LOG.info("Pausing distributor");
                        } else {
                            try {
                                // if you get one, put it on the queue and continue
                                this.queue.add(this.cauldron.deserialize(doc));
                            } catch (CauldronException ex) {
                                // If it fails, log an error and find another task
                                Logger.getLogger(Distributor.class.getName()).log(Level.SEVERE, null, ex);
                                this.workerQueue.add(worker);
                            }
                        }
                    } catch (InterruptedException ex) {
                    }
                }
            });
        }
    }

    @Override
    public void taskStatusChanged(String task, CauldronStatus status) {
        LOG.info("Status change " + task + " " + status);
        if (status == CauldronStatus.Queued) {
            // Wake up!
            start();
        }
    }

    public void pause() {
        this.paused = true;
    }
}
