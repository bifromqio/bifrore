package bifrore.processor.worker;

import bifrore.router.client.IRouterClient;
import com.hazelcast.map.IMap;
import org.pf4j.PluginManager;

public class ProcessorWorkerBuilder {
    String nodeId;
    int clientNum;
    String groupName;
    String userName;
    String password;
    boolean cleanStart = true;
    boolean ordered;
    long sessionExpiryInterval = 0;
    String host;
    int port;
    String clientPrefix;
    String orderedTopicFilterPrefix = "$oshare";
    PluginManager pluginManager;
    IRouterClient routerClient;
    IMap<String, byte[]> callerCfgs;

    public ProcessorWorkerBuilder nodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public ProcessorWorkerBuilder clientNum(int clientNum) {
        this.clientNum = clientNum;
        return this;
    }

    public ProcessorWorkerBuilder groupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public ProcessorWorkerBuilder userName(String userName) {
        this.userName = userName;
        return this;
    }

    public ProcessorWorkerBuilder password(String password) {
        this.password = password;
        return this;
    }

    public ProcessorWorkerBuilder cleanStart(boolean cleanStart) {
        this.cleanStart = cleanStart;
        return this;
    }

    public ProcessorWorkerBuilder ordered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public ProcessorWorkerBuilder orderedTopicFilterPrefix(String orderedTopicFilterPrefix) {
        this.orderedTopicFilterPrefix = orderedTopicFilterPrefix;
        return this;
    }

    public ProcessorWorkerBuilder sessionExpiryInterval(long sessionExpiryInterval) {
        this.sessionExpiryInterval = sessionExpiryInterval;
        return this;
    }

    public ProcessorWorkerBuilder host(String host) {
        this.host = host;
        return this;
    }

    public ProcessorWorkerBuilder port(int port) {
        this.port = port;
        return this;
    }

    public ProcessorWorkerBuilder clientPrefix(String clientPrefix) {
        this.clientPrefix = clientPrefix;
        return this;
    }

    public ProcessorWorkerBuilder pluginManager(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
        return this;
    }

    public ProcessorWorkerBuilder routerClient(IRouterClient routerClient) {
        this.routerClient = routerClient;
        return this;
    }

    public ProcessorWorkerBuilder callerCfgs(IMap<String, byte[]> callerCfgs) {
        this.callerCfgs = callerCfgs;
        return this;
    }

    public IProcessorWorker build() {
        return new ProcessorWorker(this);
    }
}
