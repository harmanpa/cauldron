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
import java.util.Collection;
import java.util.List;
import tech.cae.cauldron.Cauldron;
import tech.cae.cauldron.api.CauldronCallback;
import tech.cae.cauldron.api.CauldronStatus;
import tech.cae.cauldron.api.CauldronTask;

/**
 *
 * @author peter
 */
public class CauldronWorkerRunnable implements Runnable {

    private final Cauldron cauldron;
    private final Collection<Class<? extends CauldronTask>> taskTypes;
    private final String name;

    public CauldronWorkerRunnable(Cauldron cauldron, Collection<Class<? extends CauldronTask>> taskTypes, String name) {
        this.cauldron = cauldron;
        this.taskTypes = taskTypes;
        this.name = name;
    }

    @Override
    @SuppressWarnings({"UseSpecificCatch", "CallToPrintStackTrace"})
    public void run() {
        while (true) {
            try {
                CauldronTask task = cauldron.pollMultiWorker(taskTypes, name).getTask();
                CauldronCallback callback = new WorkerCallback(cauldron, task.getId(), name);
                try {
                    task.run(callback);
                    callback.progress(1.0);
                    cauldron.completed(task, CauldronStatus.Completed);
                } catch (Throwable ex) {
                    callback.progress(ex.getMessage(), 1.0);
                    cauldron.completed(task, CauldronStatus.Failed);
                }
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
        }
    }

    class WorkerCallback implements CauldronCallback {

        private final Cauldron cauldron;
        private final String id;
        private long lastLog;
        private double progress;
        private final List<String> logs;
        private final String name;

        public WorkerCallback(Cauldron cauldron, String id, String name) {
            this.cauldron = cauldron;
            this.id = id;
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

        private void log(boolean force) {
            if (force || System.currentTimeMillis() - lastLog > 1000) {
                cauldron.progress(id, logs, progress, 1000, name);
                logs.clear();
                lastLog = System.currentTimeMillis();
            }
        }

    }
}
