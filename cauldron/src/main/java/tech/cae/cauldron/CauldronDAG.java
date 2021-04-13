/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tech.cae.cauldron;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import tech.cae.cauldron.api.CauldronStatus;
import tech.cae.cauldron.api.CauldronTask;

/**
 *
 * @author peter
 */
public class CauldronDAG {

    private final Function<List<String>, IdAndStatus> taskSupplier;
    private final List<CauldronDAG> parents;

    CauldronDAG(Function<List<String>, IdAndStatus> taskSupplier) {
        this.taskSupplier = taskSupplier;
        this.parents = new ArrayList<>();
    }

    public static CauldronDAG create(CauldronTask task) {
        return new CauldronDAG(new OnSubmissionSupplier(task));
    }

    public static CauldronDAG create(Cauldron.TaskMeta task) {
        return new CauldronDAG(new ExistingSupplier(task.getId()));
    }

    public static CauldronDAG create(String task) {
        return new CauldronDAG(new ExistingSupplier(task));
    }

    public CauldronDAG after(CauldronTask... tasks) {
        return after(Arrays.asList(tasks).stream().map(t -> new OnSubmissionSupplier(t)));
    }

    public CauldronDAG after(Cauldron.TaskMeta... tasks) {
        return after(Arrays.asList(tasks).stream().map(t -> new ExistingSupplier(t.getId())));
    }

    public CauldronDAG after(String... tasks) {
        return after(Arrays.asList(tasks).stream().map(t -> new ExistingSupplier(t)));
    }

    private CauldronDAG after(Stream<Function<List<String>, IdAndStatus>> tasks) {
        tasks.map(task -> new CauldronDAG(task)).forEach(dag -> parents.add(dag));
        return this;
    }

    public CauldronDAG after(CauldronDAG... dags) {
        this.parents.addAll(Arrays.asList(dags));
        return this;
    }

    private List<String> submitParents() {
        return parents.stream().map(parent -> parent.submit())
                .filter(idStat -> !idStat.getStatus().isFinished())
                .map(idStat -> idStat.getId())
                .collect(Collectors.toList());
    }

    public IdAndStatus submit() {
        return taskSupplier.apply(submitParents());
    }

    static class ExistingSupplier implements Function<List<String>, IdAndStatus> {

        private final String id;

        public ExistingSupplier(String id) {
            this.id = id;
        }

        @Override
        public IdAndStatus apply(List<String> parents) {
            return new IdAndStatus(id, Cauldron.get().getTaskMeta(id).getStatus());
        }

    }

    static class OnSubmissionSupplier implements Function<List<String>, IdAndStatus> {

        private final CauldronTask task;

        public OnSubmissionSupplier(CauldronTask task) {
            this.task = task;
        }

        @Override
        public IdAndStatus apply(List<String> parents) {
            return new IdAndStatus(Cauldron.get().submit(task, 100, parents).getId(), CauldronStatus.Queued);
        }

    }

    public static class IdAndStatus {

        private final String id;
        private final CauldronStatus status;

        IdAndStatus(String id, CauldronStatus status) {
            this.id = id;
            this.status = status;
        }

        public String getId() {
            return id;
        }

        public CauldronStatus getStatus() {
            return status;
        }

    }
}
