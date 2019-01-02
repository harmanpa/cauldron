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

import tech.cae.cauldron.Cauldron;
import tech.cae.cauldron.tasks.CauldronCallback;
import tech.cae.cauldron.tasks.CauldronStatus;
import tech.cae.cauldron.tasks.CauldronTask;

/**
 *
 * @author peter
 * @param <T>
 */
public class CauldronWorker<T extends CauldronTask> implements Runnable {

    private final Cauldron cauldron;
    private final Class<T> taskType;

    public CauldronWorker(Cauldron cauldron, Class<T> taskType) {
        this.cauldron = cauldron;
        this.taskType = taskType;
    }

    @Override
    @SuppressWarnings("UseSpecificCatch")
    public void run() {
        while (true) {
            T task = cauldron.poll(taskType);
            try {
                task.run(new WorkerCallback(task.getId()));
                cauldron.completed(task, CauldronStatus.Completed);
            } catch (Throwable ex) {
                cauldron.completed(task, CauldronStatus.Failed);
            }
        }
    }

    class WorkerCallback implements CauldronCallback {

        private final String id;

        public WorkerCallback(String id) {
            this.id = id;
        }

        @Override
        public void log(String message) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void warning(String message) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void progress(String message, double progress) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void progress(double progress) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }
}
