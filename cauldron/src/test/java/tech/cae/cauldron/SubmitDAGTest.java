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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.junit.Test;
import tech.cae.cauldron.api.CauldronStatus;
import tech.cae.cauldron.api.CauldronTask;
import tech.cae.cauldron.api.exceptions.CauldronException;

/**
 *
 * @author peter
 */
public class SubmitDAGTest extends AbstractCauldronTest {

    @Test
    public void test() throws CauldronException, InterruptedException, ExecutionException {
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    CauldronTask polledTask = null;
                    try {
                        polledTask = Cauldron.get().getDistributor().get("executor");
                        polledTask.run(null);
                        Cauldron.get().completed(polledTask, CauldronStatus.Completed);
                    } catch (Throwable ex) {
                        Cauldron.get().completed(polledTask, CauldronStatus.Failed);
                    }
                }
            }
        });
        SillyTask first = new SillyTask("FIRST");
        CauldronDAG dag = CauldronDAG.create(new SillyTask("LAST")).after(CauldronDAG.create(new SillyTask("SECOND_ONE")).after(first));
        Cauldron.get().getCompletion(dag.submit().getId()).get();

    }
}
