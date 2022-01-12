package com.jobframe.core;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

public class JobGroupData {

	@Setter
	@Getter
	private String[] groupedColumns;

	@Setter
	@Getter
	private Map<List<Object>, List<Integer>> groupedInfo;

	@Setter
	@Getter
	private final JobFrameData originalFrame;

	public JobGroupData(String[] groupedColumns, Map<List<Object>, List<Integer>> groupedInfo, JobFrameData frame) {
		this.groupedColumns = groupedColumns;
		this.groupedInfo = groupedInfo;
		originalFrame = frame;
	}
}
