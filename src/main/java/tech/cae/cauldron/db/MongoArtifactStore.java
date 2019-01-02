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
package tech.cae.cauldron.db;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.bson.types.ObjectId;

/**
 *
 * @author peter
 */
public class MongoArtifactStore {

    private final File workingDir;
    private final GridFSBucket bucket;
    private final ThreadLocal<String> putId;
    private final ThreadLocal<String> getId;

    public MongoArtifactStore(File workingDir,
            MongoDatabase database,
            String bucketName,
            ThreadLocal<String> putId,
            ThreadLocal<String> getId) {
        this.workingDir = workingDir;
        this.bucket = GridFSBuckets.create(database, bucketName);
        this.putId = putId;
        this.getId = getId;
    }

    public void delete(Artifact artifact) {
        String taskId = getId.get();
        if (artifact.getId() == null) {
            return;
        }
        bucket.delete(new ObjectId(artifact.getId()));
    }

    public File get(Artifact artifact) throws IOException {
        String taskId = getId.get();
        if (artifact.getId() == null) {
            return new File(artifact.getName());
        }
        File out = new File(workingDir, artifact.getName());
        try (FileOutputStream os = new FileOutputStream(out)) {
            bucket.downloadToStream(new ObjectId(artifact.getId()), os);
            os.flush();
        }
        return out;
    }

    public Artifact put(File file) throws IOException {
        String taskId = putId.get();
        if (!file.exists()) {
            return new Artifact(file.getAbsolutePath(), null);
        }
        if (file.isDirectory()) {
            // Zip it
        }
        ObjectId id = bucket.uploadFromStream(file.getName(), new FileInputStream(file));
        return new Artifact(file.getName(), id.toHexString());
    }
}
