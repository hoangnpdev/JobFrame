package com.jobframe.core;

import com.jobframe.util.CalculatorUtils;
import lombok.Setter;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiFunction;

public class JobGroup {

    @Setter
    private JobFrame jobFrame;

    @Setter
    private BiFunction<JobFrameData, JobFrameData, JobGroupData> grouping;

    /**
     * lazy
     * @param columnName
     * @return
     */
    public JobFrame sum(String columnName) {
        JobFrame newJobFrame = new JobFrame();
        BiFunction<JobFrameData, JobFrameData, JobFrameData> transforming = (JobFrameData d1, JobFrameData d2) -> {
            JobGroupData jobGroupData = grouping.apply(d1, null);
            Map<String, Object> newData = new HashMap<>();
            for (String gc: jobGroupData.getGroupedColumns()) {
                newData.put(gc, new Column());
            }
            newData.put(columnName, new Column());

            List<Entry<List<Object>, List<Integer>>> entrySet = new ArrayList<>(jobGroupData.getGroupedInfo().entrySet());

            for (int i = 0; i < entrySet.size(); i ++ ) {
                Entry<List<Object>, List<Integer>> entry = entrySet.get(i);
                for (int kid = 0; kid < entry.getKey().size(); kid ++) {
                    Object v = entry.getKey().get(kid);
                    ((Column) newData.get(jobGroupData.getGroupedColumns()[kid])).append(v);
                }

                ((Column) newData.get(columnName)).append(
                    CalculatorUtils.sum(
                            jobGroupData.getOriginalFrame().getColumn(columnName)
                                    .generateColumnFromKeys(entry.getValue()).values().toArray()
                    )
                );

            }
            return new JobFrameData(newData);
        };
        newJobFrame.setParent(jobFrame);
        newJobFrame.setTransform(transforming);
        return newJobFrame;
    }
}
