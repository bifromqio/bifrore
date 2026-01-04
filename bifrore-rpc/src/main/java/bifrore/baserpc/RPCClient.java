package bifrore.baserpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static bifrore.baserpc.RPCContext.CUSTOM_METADATA_CTX_KEY;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;

@Slf4j
final class RPCClient implements IRPCClient {
    private Channel channel;
    private CallOptions callOptions;

    RPCClient(Channel channel) {
        this.channel = channel;
        this.callOptions = CallOptions.DEFAULT;
    }

    @Override
    public <ReqT, RespT> CompletableFuture<RespT> invoke(ReqT req,
                                                         Map<String, String> metadata,
                                                         MethodDescriptor<ReqT, RespT> methodDesc) {
        Context ctx = prepareContext(metadata);
        Context prev = ctx.attach();
        try {
            CompletableFuture<RespT> future = new CompletableFuture<>();
            asyncUnaryCall(channel.newCall(methodDesc, callOptions), req, new StreamObserver<RespT>() {
                @Override
                public void onNext(RespT value) {
                    future.complete(value);
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Unary call of method {} failure:", methodDesc.getFullMethodName(), t);
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {

                }
            });
            return future;
        }finally {
            ctx.detach(prev);
        }
    }

    private Context prepareContext(Map<String, String> metadata) {
        return Context.ROOT.fork()
                .withValue(CUSTOM_METADATA_CTX_KEY, metadata);
    }
}
