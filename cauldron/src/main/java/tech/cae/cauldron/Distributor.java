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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import tech.cae.cauldron.api.CauldronTask;

/**
 *
 * @author peter
 */
public class Distributor {

    private final BlockingQueue<String> workerQueue = new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors());
    private final BlockingQueue<Cauldron.MultiTaskResponse> queue = new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors());
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Cauldron cauldron;
    private final List<Class<? extends CauldronTask>> types;
    private boolean cancelled = false;

    Distributor(Cauldron cauldron, List<Class<? extends CauldronTask>> types) {
        this.cauldron = cauldron;
        this.types = types;
        start();
    }

    public Cauldron.MultiTaskResponse get(String worker) throws InterruptedException {
        this.workerQueue.add(worker);
        return this.queue.take();
    }

    final void start() {
        this.executor.submit(() -> {
            while (!cancelled) {
                try {
                    String worker = this.workerQueue.take();
                    this.queue.add(this.cauldron.pollMultiWorker(types, worker));
                } catch (InterruptedException ex) {
                }
            }
        });
    }

    public void shutdown() {
        this.cancelled = true;
        this.executor.shutdownNow();
    }
}
