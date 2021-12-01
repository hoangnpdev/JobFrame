package com.jobframe.core;

public class ExpressionBuilder {

    public static Expression lit(Object value) {
        return new Expression(value);
    }

    public static Expression col(String columnName) {
        return new Expression(columnName);
    }

    public static Expression not(Expression expression) {
        ComputingNode newRoot = new ComputingNode(null, NodeType.NOT);
        newRoot.setLeft(expression.getRoot());
        return new Expression(newRoot);
    }
}
