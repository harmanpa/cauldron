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
package tech.cae.cauldron.cloudrun.base;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import tech.cae.cauldron.api.CauldronTask;

/**
 * Combines the task and any payload with logs and status
 *
 * @author peter
 */
public class CloudRunResponse {

    @JsonProperty
    private String taskId;
    @JsonProperty
    private CauldronTask task;
    @JsonProperty
    private double progress;
    @JsonProperty
    private List<String> log;
    @JsonProperty
    private boolean success;

    public CloudRunResponse() {
    }

    public CloudRunResponse(String taskId, CauldronTask task, double progress, List<String> log, boolean success) {
        this.taskId = taskId;
        this.task = task;
        this.progress = progress;
        this.log = log;
        this.success = success;
    }

    public CauldronTask getTask() {
        return task;
    }

    public double getProgress() {
        return progress;
    }

    public List<String> getLog() {
        return log;
    }

    public String getTaskId() {
        return taskId;
    }

    public boolean isSuccess() {
        return success;
    }

}
