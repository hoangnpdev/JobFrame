package com.jobframe.core;

import com.jobframe.util.CalculatorUtils;

public class Expression {

    private ComputingNode root;

    public Expression(Object value) {
        root = new ComputingNode(value, NodeType.CONSTANT);
    }

    public Expression(String columnName) {
        root = new ComputingNode(columnName, NodeType.COLUMN);
    }

    public Expression(ComputingNode node) {
        root = node;
    }

    public Object calculate(Row row) {
        return computeNode(root, row);
    }

    private Object computeNode(ComputingNode node, Row row) {
        NodeType type = node.getType();
        if (type.equals(NodeType.CONSTANT)) {
            return node.getData();
        }
        if (type.equals(NodeType.COLUMN)) {
            return row.getField(node.getData().toString());
        }
        if (type.equals(NodeType.ADD)) {
            return CalculatorUtils.add(computeNode(node.left(), row), computeNode(node.right(), row));
        }
        if (type.equals(NodeType.SUB)) {
            return CalculatorUtils.subtract(computeNode(node.left(), row), computeNode(node.right(), row));
        }
        if (type.equals(NodeType.MUL)) {
            return CalculatorUtils.multiply(computeNode(node.left(), row), computeNode(node.right(), row));
        }
        if (type.equals(NodeType.DIV)) {
            return CalculatorUtils.divide(computeNode(node.left(), row), computeNode(node.right(), row));
        }
        if (type.equals(NodeType.AND)) {
            return CalculatorUtils.and(computeNode(node.left(), row), computeNode(node.right(), row));
        }
        if (type.equals(NodeType.OR)) {
            return CalculatorUtils.or(computeNode(node.left(), row), computeNode(node.right(), row));
        }
        if (type.equals(NodeType.NOT)) {
            return CalculatorUtils.not(computeNode(node.left(), row));
        }
        if (type.equals(NodeType.EQUAL)) {
            Object leftValue = computeNode(node.left(), row);
            Object rightValue = computeNode(node.right(), row);
            if (leftValue == null || rightValue == null) {
                return false;
            }
            return computeNode(node.left(), row).equals(computeNode(node.right(), row));
        }
        throw new RuntimeException("NodeType not found Exception: " + node.getType());
    }

    public ComputingNode getRoot() {
        return root;
    }

    public Expression add(Expression expression) {
        ComputingNode newRoot = new ComputingNode(null, NodeType.ADD);
        newRoot.setLeft(root);
        newRoot.setRight(expression.getRoot());
        root = newRoot;
        return this;
    }

    public Expression subtract(Expression expression) {
        ComputingNode newRoot = new ComputingNode(null, NodeType.SUB);
        newRoot.setLeft(root);
        newRoot.setRight(expression.getRoot());
        root = newRoot;
        return this;
    }

    public Expression multiply(Expression expression) {
        ComputingNode newRoot = new ComputingNode(null, NodeType.MUL);
        newRoot.setLeft(root);
        newRoot.setRight(expression.getRoot());
        root = newRoot;
        return this;

    }

    public Expression divide(Expression expression) {
        ComputingNode newRoot = new ComputingNode(null, NodeType.DIV);
        newRoot.setLeft(root);
        newRoot.setRight(expression.getRoot());
        root = newRoot;
        return this;
    }

    public Expression and(Expression expression) {
        ComputingNode newRoot = new ComputingNode(null, NodeType.AND);
        newRoot.setLeft(root);
        newRoot.setRight(expression.getRoot());
        root = newRoot;
        return this;
    }

    public Expression or(Expression expression) {
        ComputingNode newRoot = new ComputingNode(null, NodeType.OR);
        newRoot.setLeft(root);
        newRoot.setRight(expression.getRoot());
        root = newRoot;
        return this;
    }

    public Expression equalTo(Expression expression) {
        ComputingNode newRoot = new ComputingNode(null, NodeType.EQUAL);
        newRoot.setLeft(root);
        newRoot.setRight(expression.getRoot());
        root = newRoot;
        return this;
    }
}
