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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.IOException;
import org.bson.Document;
import org.junit.Test;
import tech.cae.cauldron.api.exceptions.CauldronException;
import tech.cae.cauldron.api.CauldronCallback;

/**
 *
 * @author peter
 */
public class SerializationTest extends AbstractCauldronTest {

    @Test
    public void test() throws JsonProcessingException, CauldronException, IOException {
        MyTask task = new MyTask();
        task.setInput("Hello");
        Document doc = Cauldron.get().serialize(task);
        System.out.println(doc.toJson());
        MyTask task2 = (MyTask)Cauldron.get().deserialize(doc);
        task2.run(new CauldronCallback() {
            @Override
            public void log(String message) {
            }

            @Override
            public void progress(String message, double progress) {
            }

            @Override
            public void progress(double progress) {
            }

        });
    }
}
