package com.jobframe.core;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ComputingNode {
    private NodeType type;
    private Object data;
    private List<ComputingNode> children;

    public ComputingNode(Object data, NodeType type) {
        this.type = type;
        this.data = data;
        children = new ArrayList<>();
    }

    public ComputingNode left() {
        return children.get(0);
    }

    public ComputingNode right() {
        return children.get(1);
    }

    public void setLeft(ComputingNode node) {
        children.add(0, node);
    }

    public void setRight(ComputingNode node) {
        children.add(1, node);
    }
}
