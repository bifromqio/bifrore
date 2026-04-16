package com.bifrore;

import java.util.function.Consumer;

public final class BifroREOptions {
    final MqttOptions mqtt = new MqttOptions();
    final FfiOptions ffi = new FfiOptions();
    final JvmOptions jvm = new JvmOptions();

    public BifroREOptions mqtt(Consumer<MqttOptions> configurer) {
        configurer.accept(mqtt);
        return this;
    }

    public BifroREOptions ffi(Consumer<FfiOptions> configurer) {
        configurer.accept(ffi);
        return this;
    }

    public BifroREOptions jvm(Consumer<JvmOptions> configurer) {
        configurer.accept(jvm);
        return this;
    }

    public static final class MqttOptions {
        String host = "127.0.0.1";
        int port = 1883;
        String nodeId;
        int clientCount = 1;
        boolean multiNci = false;
        String username;
        String password;
        boolean cleanStart = true;
        int sessionExpiryInterval = 3600;
        String groupName = "bifrore-group";

        private MqttOptions() {}

        public MqttOptions host(String host) {
            this.host = host;
            return this;
        }

        public MqttOptions port(int port) {
            this.port = port;
            return this;
        }

        public MqttOptions nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public MqttOptions clientCount(int clientCount) {
            this.clientCount = clientCount;
            return this;
        }

        public MqttOptions multiNci(boolean multiNci) {
            this.multiNci = multiNci;
            return this;
        }

        public MqttOptions username(String username) {
            this.username = username;
            return this;
        }

        public MqttOptions password(String password) {
            this.password = password;
            return this;
        }

        public MqttOptions cleanStart(boolean cleanStart) {
            this.cleanStart = cleanStart;
            return this;
        }

        public MqttOptions sessionExpiryInterval(int sessionExpiryInterval) {
            this.sessionExpiryInterval = sessionExpiryInterval;
            return this;
        }

        public MqttOptions groupName(String groupName) {
            this.groupName = groupName;
            return this;
        }

    }

    public static final class FfiOptions {
        String ruleJsonPath;
        int payloadFormat = BifroRE.PAYLOAD_JSON;
        String clientIdsPath = "./client_ids";
        String protobufDescriptorSetPath;
        String protobufMessageName;
        boolean detailedLatencyMetrics = false;

        private FfiOptions() {}

        public FfiOptions ruleJsonPath(String ruleJsonPath) {
            this.ruleJsonPath = ruleJsonPath;
            return this;
        }

        public FfiOptions payloadFormat(int payloadFormat) {
            this.payloadFormat = payloadFormat;
            return this;
        }

        public FfiOptions clientIdsPath(String clientIdsPath) {
            this.clientIdsPath = clientIdsPath;
            return this;
        }

        public FfiOptions protobufDescriptorSetPath(String protobufDescriptorSetPath) {
            this.protobufDescriptorSetPath = protobufDescriptorSetPath;
            return this;
        }

        public FfiOptions protobufMessageName(String protobufMessageName) {
            this.protobufMessageName = protobufMessageName;
            return this;
        }

        public FfiOptions detailedLatencyMetrics(boolean detailedLatencyMetrics) {
            this.detailedLatencyMetrics = detailedLatencyMetrics;
            return this;
        }

    }

    public static final class JvmOptions {
        int callbackQueueCapacity = 1024;
        int pollBatchLimit = 64;
        int directPollSlotCount = 4;
        int directPayloadBufferBytes = 1024 * 1024;

        private JvmOptions() {}

        public JvmOptions callbackQueueCapacity(int callbackQueueCapacity) {
            this.callbackQueueCapacity = callbackQueueCapacity;
            return this;
        }

        public JvmOptions pollBatchLimit(int pollBatchLimit) {
            this.pollBatchLimit = pollBatchLimit;
            return this;
        }

        public JvmOptions directPollSlotCount(int directPollSlotCount) {
            this.directPollSlotCount = directPollSlotCount;
            return this;
        }

        public JvmOptions directPayloadBufferBytes(int directPayloadBufferBytes) {
            this.directPayloadBufferBytes = directPayloadBufferBytes;
            return this;
        }

    }
}
