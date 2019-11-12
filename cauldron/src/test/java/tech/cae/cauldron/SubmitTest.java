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
package tech.cae.cauldron;

import java.util.concurrent.ExecutionException;
import org.junit.Test;
import tech.cae.cauldron.api.CauldronStatus;

/**
 *
 * @author peter
 */
public class SubmitTest extends AbstractCauldronTest {

    @Test
    public void test() throws InterruptedException, ExecutionException {
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                MyTask polledTask = getCauldron().pollWorker(MyTask.class, "thread2");
                try {
                    polledTask.run(null);
                    getCauldron().completed(polledTask, CauldronStatus.Completed);
                } catch (Throwable ex) {
                    getCauldron().completed(polledTask, CauldronStatus.Failed);
                }
            }
        });
        thread2.start();
        MyTask task = new MyTask();
        task.setInput("bananas");
        //getCauldron().submit(task);
        MyTask task2 = (MyTask)getCauldron().getCompletion(getCauldron().submit(task).getId()).get();
        System.out.println(task2.getOutput());
        thread2.join();
//        for (MyTask task2 : getCauldron().getTasks(MyTask.class)) {
//            System.out.println(task2.getOutput());
//        }

    }
}
