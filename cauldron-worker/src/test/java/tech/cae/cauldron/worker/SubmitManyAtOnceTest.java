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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import tech.cae.cauldron.Cauldron.SubmitResponse;
import tech.cae.cauldron.api.CauldronTask;
import tech.cae.cauldron.api.exceptions.CauldronException;

/**
 *
 * @author peter
 */
public class SubmitManyAtOnceTest extends AbstractCauldronTest {

    @Test
    public void test() throws CauldronException, InterruptedException, ExecutionException {
        CauldronWorker worker = new CauldronWorker(getCauldron(), Runtime.getRuntime().availableProcessors());
        List<CauldronTask> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                tasks.add(create(i * 1.0, j * 10.0));
            }
        }
        List<CompletableFuture<CauldronTask>> futures = submit(tasks);
        for (CompletableFuture<CauldronTask> future : futures) {
            System.out.println(((AddingTask) future.get()).getC());
        }
        worker.shutdown(false);
    }

    private CauldronTask create(double a, double b) throws CauldronException {
        AddingTask task = new AddingTask();
        task.setA(a);
        task.setB(b);
        return task;
    }

    private List<CompletableFuture<CauldronTask>> submit(List<CauldronTask> tasks) throws CauldronException {
        List<CompletableFuture<CauldronTask>> futures = new ArrayList<>();
        for (SubmitResponse sr : getCauldron().submitMulti(tasks)) {
            futures.add(getCauldron().getCompletion(sr.getId()));
        }
        return futures;
    }
}
