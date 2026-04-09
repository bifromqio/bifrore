package com.bifrore;

public final class BifroREOptions {
    public String host = "127.0.0.1";
    public int port = 1883;
    public String ruleJsonPath;
    public String nodeId;
    public int clientCount = 1;
    public boolean multiNci = false;
    public int payloadFormat = BifroRE.PAYLOAD_JSON;
    public String clientIdsPath = "./client_ids";
    public String protobufDescriptorSetPath;
    public String protobufMessageName;
    public int callbackQueueCapacity = 1024;
    public int pollBatchLimit = 64;
    public int directPollSlotCount = 4;
    public int directPayloadBufferBytes = 1024 * 1024;
    public boolean cleanStart = true;
    public int sessionExpiryInterval = 3600;
    public String groupName = "bifrore-group";

    public BifroREOptions ruleJsonPath(String ruleJsonPath) {
        this.ruleJsonPath = ruleJsonPath;
        return this;
    }

    public BifroREOptions host(String host) {
        this.host = host;
        return this;
    }

    public BifroREOptions port(int port) {
        this.port = port;
        return this;
    }

    public BifroREOptions nodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public BifroREOptions clientCount(int clientCount) {
        this.clientCount = clientCount;
        return this;
    }

    public BifroREOptions multiNci(boolean multiNci) {
        this.multiNci = multiNci;
        return this;
    }

    public BifroREOptions payloadFormat(int payloadFormat) {
        this.payloadFormat = payloadFormat;
        return this;
    }

    public BifroREOptions clientIdsPath(String clientIdsPath) {
        this.clientIdsPath = clientIdsPath;
        return this;
    }

    public BifroREOptions protobufDescriptorSetPath(String protobufDescriptorSetPath) {
        this.protobufDescriptorSetPath = protobufDescriptorSetPath;
        return this;
    }

    public BifroREOptions protobufMessageName(String protobufMessageName) {
        this.protobufMessageName = protobufMessageName;
        return this;
    }

    public BifroREOptions callbackQueueCapacity(int callbackQueueCapacity) {
        this.callbackQueueCapacity = callbackQueueCapacity;
        return this;
    }

    public BifroREOptions pollBatchLimit(int pollBatchLimit) {
        this.pollBatchLimit = pollBatchLimit;
        return this;
    }

    public BifroREOptions directPollSlotCount(int directPollSlotCount) {
        this.directPollSlotCount = directPollSlotCount;
        return this;
    }

    public BifroREOptions directPayloadBufferBytes(int directPayloadBufferBytes) {
        this.directPayloadBufferBytes = directPayloadBufferBytes;
        return this;
    }

    public BifroREOptions cleanStart(boolean cleanStart) {
        this.cleanStart = cleanStart;
        return this;
    }

    public BifroREOptions sessionExpiryInterval(int sessionExpiryInterval) {
        this.sessionExpiryInterval = sessionExpiryInterval;
        return this;
    }

    public BifroREOptions groupName(String groupName) {
        this.groupName = groupName;
        return this;
    }
}
