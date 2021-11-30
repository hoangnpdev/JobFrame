package com.jobframe.core;

import lombok.Data;

import java.util.List;

@Data
public class ComputingNode {
    private NodeType type;
    private Object data;
    private List<ComputingNode> children;

    public ComputingNode(Object data, NodeType type) {
        this.type = type;
        this.data = data;
    }
}
