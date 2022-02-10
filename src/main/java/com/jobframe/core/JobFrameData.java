package com.jobframe.core;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JobFrameData {

	// data

	private Map<String, Object> columnMapper;

	private Set<String> leftColumnSet;

	private Set<String> rightColumnSet;

	private Map<Integer, Integer> rowLeftIndex; // todo: caching this

	private Map<Integer, Integer> rowRightIndex; // todo: caching this

	// logic

	public JobFrameData() {
		columnMapper = new HashMap<>();
	}

	public JobFrameData(Map<String, Object> columnMapper) {
		this.columnMapper = columnMapper;
	}

	public void addColumn(String columnName, RandomAccessFile randomAccessFile, Class type) throws FileNotFoundException {
		columnMapper.put(columnName, new Column(randomAccessFile, type));
	}

	public Column getColumn(String columnName) {
		return (Column) columnMapper.get(columnName);
	}

	public Map<String, Object> getColumnMapper() {
		return columnMapper;
	}

	public void setColumnMapper(Map<String, Object> columnMapper) {
		this.columnMapper = columnMapper;
	}

	public void resetIndex() {
		columnMapper.values()
				.forEach(
						(col) -> {
							((Column) col).resetIndex();
						}
				);
	}

	public Row getRow(int index) {
		Map<String, Object> rData = new HashMap<>();
		columnMapper.forEach((key, value) -> rData.put(key, ((Column) value).get(index)));
		return new Row(rData);
	}

	public Object at(int rowId, String columnName) {
		Object objectRef = columnMapper.get(columnName);
		if (objectRef instanceof Column) {
			return ((Column) objectRef).get(rowId);
		}
		return ((JobFrameData) objectRef).at(indexRef(rowId, columnName), columnName);
	}

	public int indexRef(int rowId, String columnName) {
		if (leftColumnSet.contains(columnName)) {
			return rowLeftIndex.get(rowId);
		}
		return rowRightIndex.get(rowId);
	}

	public int size() {
		return ((Column) columnMapper.entrySet().stream().findFirst().get().getValue()).size();
	}
}
