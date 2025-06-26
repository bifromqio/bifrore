package bifrore.processor.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TaskTracker {
    private Map<ReorderBufferNode, List<CompletableFuture<Void>>> futureMap = new HashMap<>();

    public void track(ReorderBufferNode node) {
        List<CompletableFuture<Void>> futures = List.of(new CompletableFuture<>(), new CompletableFuture<>());
        futureMap.put(node, futures);
    }

    public List<CompletableFuture<Void>> getFutures(ReorderBufferNode node) {
        return futureMap.get(node);
    }
}
