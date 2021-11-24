package com.jobframe.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JobFrame {

	private Map<String, Column> columnMapper;

	public JobFrame(List<List<Object>> datas, List<String> columnNames) {
		columnMapper = new HashMap<>();
		for (String columnName: columnNames) {
			columnMapper.put(columnName, new Column());
		}
		for (List<Object> data: datas) {
			for (int i = 0; i < columnNames.size(); i++) {
				columnMapper.get(columnNames.get(i)).append(data.get(i));
			}
		}
	}

	public JobFrame(Map<String, Column> data) {
		columnMapper = data;
	}

	public Column getColumn(String name) {
		return columnMapper.get(name);
	}

	public Object at(int rowIndex, String columnName) {
		return columnMapper.get(columnName).get(rowIndex);
	}

	public JobFrame eqAndGet(String columnName, Object value) {
		Column column = getColumn(columnName);
		Set<Integer> indexes = column.getIndexes(value);
		Map<String, Column> data = columnMapper.entrySet()
				.stream()
				.collect(
						Collectors.toMap(
							entry -> entry.getKey(),
							entry -> entry.getValue().filterByIndexes(indexes)
						)
				);
		return new JobFrame(data);
	}

	public void resetIndex() {
		columnMapper.values()
				.forEach(column -> {
			column.resetIndex();
		});
	}
}
