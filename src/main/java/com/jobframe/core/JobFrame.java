package com.jobframe.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Map.Entry;

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
		JobFrame result = new JobFrame(data);
		result.resetIndex();
		return result;
	}

	public void resetIndex() {
		columnMapper.values()
				.forEach(column -> {
			column.resetIndex();
		});
	}

	public Map<String, Column> getData() {
		return columnMapper;
	}

	public JobFrame join(JobFrame otherFrame, String how, String joinType) {
		Map<String, Column> joinData = new HashMap<>();
		if (joinType.equals("inner")) {
			String[] keyKey = how.split("=");
			String leftKey = keyKey[0];
			String rightKey = keyKey[1];
			Column leftKeyColumn = getColumn(leftKey);
			Column rightKeyColumn = otherFrame.getColumn(rightKey);
			List<Entry<Integer, Integer>> innerKeys = leftKeyColumn.getInnerKeyWith(rightKeyColumn);

			List<Integer> rightKeyList = innerKeys.stream().map(Entry::getValue).collect(Collectors.toList());
			otherFrame.getData().entrySet()
					.forEach( entry -> {
						Column column = entry.getValue().generateColumnFromKeys(rightKeyList);
						joinData.put(entry.getKey(), column);
					});

			List<Integer> leftKeyList = innerKeys.stream().map(Entry::getKey).collect(Collectors.toList());
			columnMapper.entrySet()
					.forEach( entry -> {
						Column column = entry.getValue().generateColumnFromKeys(leftKeyList);
						joinData.put(entry.getKey(), column);
					});
			return new JobFrame(joinData);
		}
		throw new RuntimeException("JoinType invalid Exception");
	}

	public Row getRow(int index) {
		Map<String, Object> rData = new HashMap<>();
		columnMapper.entrySet().forEach(entry -> {
			rData.put(entry.getKey(), entry.getValue().get(index));
		});
		return new Row(rData);
	}
}
