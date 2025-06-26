package bifrore.processor.worker;

import bifrore.router.client.Matched;

import java.util.List;
import java.util.Optional;

public class ReorderBufferNode {
    private Optional<List<Matched>> matchedList;
    private long tag;

    ReorderBufferNode(long tag) {
        this.matchedList = Optional.empty();
        this.tag = tag;
    }

    public Optional<List<Matched>> getMatchedList() {
        return matchedList;
    }

    public void setMatchedList(Optional<List<Matched>> matchedList) {
        this.matchedList = matchedList;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(tag);
    }
}
