package com.jobframe.core;

import lombok.Data;

@Data
public class ComputingNode {
    private String type;
    private Object data;
    private ComputingNode left;
    private ComputingNode right;
}
