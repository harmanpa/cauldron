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
package tech.cae.cauldron.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import tech.cae.cauldron.Cauldron;
import tech.cae.cauldron.api.exceptions.CauldronException;

/**
 *
 * @author peter
 */
public class CauldronWorker {

    private final ExecutorService service;
    private final String name;
    private final List<CauldronWorkerRunnable> running;

    public CauldronWorker() throws CauldronException {
        this(Cauldron.get(), Runtime.getRuntime().availableProcessors());
    }

    public CauldronWorker(Cauldron cauldron, int parallelism) throws CauldronException {
        this.service = Executors.newFixedThreadPool(parallelism);
        this.name = UUID.randomUUID().toString();
        this.running = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            CauldronWorkerRunnable runner = new CauldronWorkerRunnable(cauldron, name + ":" + Integer.toString(i + 1));
            this.service.submit(runner);
            this.running.add(runner);
        }
    }

    public static void main(String[] args) {
        try {
            final CauldronWorker worker = new CauldronWorker();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                worker.shutdown(false);
            }));
        } catch (CauldronException ex) {
            Logger.getLogger(CauldronWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void shutdown(boolean force) {
        this.running.forEach(runner -> runner.shutdown());
        if (force) {
            this.service.shutdownNow();
        } else {
            this.service.shutdown();
        }
    }
}
