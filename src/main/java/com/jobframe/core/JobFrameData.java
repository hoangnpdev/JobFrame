package com.jobframe.core;

import java.util.HashMap;
import java.util.Map;

public class JobFrameData {

	private Map<String, Column> columnMapper;

	public JobFrameData() {
		columnMapper = new HashMap<>();
	}

	public JobFrameData(Map<String, Column> columnMapper) {
		this.columnMapper = columnMapper;
	}

	public void addColumn(String columnName) {
		columnMapper.put(columnName, new Column());
	}

	public Column getColumn(String columnName) {
		return columnMapper.get(columnName);
	}

	public Map<String, Column> getColumnMapper() {
		return columnMapper;
	}

	public void setColumnMapper(Map<String, Column> columnMapper) {
		this.columnMapper = columnMapper;
	}

	public void resetIndex() {
		columnMapper.values()
				.forEach(Column::resetIndex);
	}

	public Row getRow(int index) {
		Map<String, Object> rData = new HashMap<>();
		columnMapper.forEach((key, value) -> rData.put(key, value.get(index)));
		return new Row(rData);
	}

	public int size() {
		return columnMapper.entrySet().stream().findFirst().get().getValue().size();
	}
}
