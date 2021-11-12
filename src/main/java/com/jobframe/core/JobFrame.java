package com.jobframe.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

	public Column getColumn(String name) {
		return columnMapper.get(name);
	}
}
