package com.jobframe.core;

import com.jobframe.utils.CalculatorUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class JobFrameGroup {

    private String[] groupedColumns;

    private Map<List<Object>, List<Integer>> groupedInfo;

    private final JobFrame originalFrame;

    public JobFrameGroup(String[] groupedColumns, Map<List<Object>, List<Integer>> groupedInfo, JobFrame frame) {
        this.groupedColumns = groupedColumns;
        this.groupedInfo = groupedInfo;
        originalFrame = frame;
    }

    /**
     * lazy
     * @param columnName
     * @return
     */
    public JobFrame sum(String columnName) {
        Map<String, Column> newData = new HashMap<>();
        for (String gc: groupedColumns) {
            newData.put(gc, new Column());
        }
        newData.put(columnName, new Column());

        List<Entry<List<Object>, List<Integer>>> entrySet = new ArrayList<>(groupedInfo.entrySet());

        for (int i = 0; i < entrySet.size(); i ++ ) {
            Entry<List<Object>, List<Integer>> entry = entrySet.get(i);
            for (int kid = 0; kid < entry.getKey().size(); kid ++) {
                Object v = entry.getKey().get(kid);
                newData.get(groupedColumns[kid]).append(v);
            }

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
