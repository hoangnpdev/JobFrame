package com.jobframe.core;

import com.jobframe.utils.CalculatorUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class JobFrameGroup {

    private String groupedColumn;

    private Map<Object, List<Integer>> groupedInfo;

    private final JobFrame originalFrame;

    public JobFrameGroup(String groupedColumn, Map<Object, List<Integer>> groupedInfo, JobFrame frame) {
        this.groupedColumn = groupedColumn;
        this.groupedInfo = groupedInfo;
        originalFrame = frame;
    }

    public JobFrame sum(String columnName) {
        Map<String, Column> newData = new HashMap<>();
        newData.put(groupedColumn, new Column());
        newData.put(columnName, new Column());

        List<Entry<Object, List<Integer>>> entrySet = new ArrayList<>(groupedInfo.entrySet());

        for (int i = 0; i < entrySet.size(); i ++ ) {
            Entry<Object, List<Integer>> entry = entrySet.get(i);
            newData.get(groupedColumn).append(entry.getKey());

            newData.get(columnName).append(
                CalculatorUtils.sum(
                        originalFrame.getColumn(columnName)
                                .generateColumnFromKeys(entry.getValue()).values().toArray()
                )
            );

        }
        return new JobFrame(newData);
    }
}
