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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import tech.cae.cauldron.api.CauldronConfiguration;
import tech.cae.cauldron.api.CauldronConfigurationProvider;
import tech.cae.cauldron.api.exceptions.CauldronException;

/**
 *
 * @author peter
 */
@AutoService(CauldronConfigurationProvider.class)
public class AbstractCauldronTest extends CauldronConfigurationProvider {

    private static GenericContainer mongo;

    @BeforeClass
    public static void startMongo() {
        mongo = new GenericContainer("mongo:4.0").withExposedPorts(27017);
        mongo.setCommand("--replSet", "rs0");
        mongo.start();
        try {
            mongo.execInContainer("mongo", "--eval", "rs.initiate()");
        } catch (UnsupportedOperationException ex) {
            Logger.getLogger(AbstractCauldronTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AbstractCauldronTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(AbstractCauldronTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

//    @AfterClass
//    public static void stopMongo() {
//        mongo.stop();
//    }
    @Override
    public CauldronConfiguration getConfiguration() {
        return new CauldronConfiguration(mongo.getContainerIpAddress(), mongo.getMappedPort(27017), "cauldron", "tasks");
    }
}
