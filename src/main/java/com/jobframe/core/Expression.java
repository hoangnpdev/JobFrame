package com.jobframe.core;

public class Expression {

    private ComputingNode root;

    public Expression(Object value) {
        root = new ComputingNode(value, NodeType.CONSTANT);
    }

    public Expression(String columnName) {
        root = new ComputingNode(columnName, NodeType.COLUMN);
    }

    public Object calculate(Row row) {
        return computeNode(root, row);
    }

    private Object computeNode(ComputingNode node, Row row) {
        NodeType type = node.getType();
        if (type.equals(NodeType.CONSTANT)) {
            return node.getData();
        }
        if (type.equals(NodeType.ADD)) {
            Object left = computeNode(node.getChildren().get(0), row);
            Object right = computeNode(node.getChildren().get(1), row);
            if (left instanceof Integer && right instanceof Integer) {
                return (int) left + (int) right;
            }
            return (double) left + (double) right;
        }
        throw new RuntimeException("NodeType not found Exception: " + node.getType());
    }
}
