package bifrore.processor.worker;

import io.reactivex.Scheduler;

import java.util.concurrent.Executor;

public class ConsumerExecutor implements Executor {
    private final Scheduler.Worker worker;

    public ConsumerExecutor(Scheduler scheduler) {
        this.worker = scheduler.createWorker();
    }
    @Override
    public void execute(Runnable command) {
        worker.schedule(command);
    }
}
