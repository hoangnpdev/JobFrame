package com.jobframe.core;

import com.jobframe.utils.CalculatorUtils;
import lombok.Setter;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class JobGroup {

    @Setter
    private JobFrameData jobFrameData;

    @Setter
    private BiFunction<JobFrameData, JobFrameData, JobGroupData> grouping;

    /**
     * lazy
     * @param columnName
     * @return
     */
    public JobFrame sum(String columnName) {
        Map<String, Column> newData = new HashMap<>();
        for (String gc: jobGroupData.getGroupedColumns()) {
            newData.put(gc, new Column());
        }
        newData.put(columnName, new Column());

        List<Entry<List<Object>, List<Integer>>> entrySet = new ArrayList<>(jobGroupData.getGroupedInfo().entrySet());

        for (int i = 0; i < entrySet.size(); i ++ ) {
            Entry<List<Object>, List<Integer>> entry = entrySet.get(i);
            for (int kid = 0; kid < entry.getKey().size(); kid ++) {
                Object v = entry.getKey().get(kid);
                newData.get(jobGroupData.getGroupedColumns()[kid]).append(v);
            }

            newData.get(columnName).append(
                CalculatorUtils.sum(
                        jobGroupData.getOriginalFrame().getColumn(columnName)
                                .generateColumnFromKeys(entry.getValue()).values().toArray()
                )
            );

        }
        return new JobFrame(newData);
    }
}
