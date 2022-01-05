package com.jobframe.core;

import com.jobframe.udf.define.UDF1;
import com.jobframe.udf.define.UDF2;
import com.jobframe.udf.define.UDF3;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.Map.Entry;

public class JobFrame {

	private Map<String, Column> columnMapper;

	private Function<JobFrame, JobFrame> transform;

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
								Entry::getKey,
							entry -> entry.getValue().filterByIndexes(indexes)
						)
				);
		JobFrame result = new JobFrame(data);
		result.resetIndex();
		return result;
	}

	public void resetIndex() {
		columnMapper.values()
				.forEach(Column::resetIndex);
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
			otherFrame.getData().forEach((key, value) -> {
				Column column = value.generateColumnFromKeys(rightKeyList);
				joinData.put(key, column);
			});

			List<Integer> leftKeyList = innerKeys.stream().map(Entry::getKey).collect(Collectors.toList());
			columnMapper.forEach((key, value) -> {
				Column column = value.generateColumnFromKeys(leftKeyList);
				joinData.put(key, column);
			});

			return new JobFrame(joinData);
		}
		throw new RuntimeException("JoinType invalid Exception");
	}

	public Row getRow(int index) {
		Map<String, Object> rData = new HashMap<>();
		columnMapper.forEach((key, value) -> rData.put(key, value.get(index)));
		return new Row(rData);
	}


	public JobFrame where(Expression expression) {
		List<Integer> newIndexList = new LinkedList<>();
		for (int i = 0; i < size(); i ++) {
			if(expression.calculate(getRow(i)).equals(true)) {
				newIndexList.add(i);
			}
		}
		Map<String, Column> newData = new HashMap<>();
		columnMapper.forEach((key, value) -> {
			newData.put(key, value.generateColumnFromKeys(newIndexList));
		});
		return new JobFrame(newData);
	}

	public JobFrame select(String... columns) {
		Map<String, Column> newData = new HashMap<>();
		for (String column: columns) {
			newData.put(column, columnMapper.get(column));
		}
		return new JobFrame(newData);
	}

	public int size() {
		Optional<Entry<String, Column>> op = columnMapper.entrySet().stream().findFirst();
		return op.map(stringColumnEntry -> stringColumnEntry.getValue().size()).orElse(0);
	}

	public List<String> columns() {
		return new ArrayList<>(columnMapper.keySet());
	}

	public JobFrame withColumn(String columnName, Expression expression) {
		Map<String, Column> newData = new HashMap<>(columnMapper);
		Column newColumn = new Column();
		for (int i = 0; i < size(); i ++) {
			Object value = expression.calculate(getRow(i));
			newColumn.append(value);
		}
		newData.put(columnName, newColumn);
		return new JobFrame(newData);
	}

	public <T1, R> JobFrame withColumn(String columnName, UDF1<T1, R> udf, String inputColumn) {
		Map<String, Column> newData = new HashMap<>(columnMapper);
		Column newColumn = new Column();
		for (int i = 0; i < size(); i ++) {
			Row row = getRow(i);
			Object value = udf.apply((T1) row.getField(inputColumn));
			newColumn.append(value);
		}
		newData.put(columnName, newColumn);
		return new JobFrame(newData);

	}

	public <T1, T2, R> JobFrame withColumn(String columnName, UDF2<T1, T2, R> udf, String... inputColumns) {
		Map<String, Column> newData = new HashMap<>(columnMapper);
		Column newColumn = new Column();
		for (int i = 0; i < size(); i ++) {
			Row row = getRow(i);
			Object value = udf.apply(
					(T1) row.getField(inputColumns[0]),
					(T2) row.getField(inputColumns[1])
			);
			newColumn.append(value);
		}
		newData.put(columnName, newColumn);
		return new JobFrame(newData);

	}

	public <T1, T2, T3, R> JobFrame withColumn(String columnName, UDF3<T1, T2, T3, R> udf, String... inputColumns) {
		Map<String, Column> newData = new HashMap<>(columnMapper);
		Column newColumn = new Column();
		for (int i = 0; i < size(); i ++) {
			Row row = getRow(i);
			Object value = udf.apply(
					(T1) row.getField(inputColumns[0]),
					(T2) row.getField(inputColumns[1]),
					(T3) row.getField(inputColumns[2])
			);
			newColumn.append(value);
		}
		newData.put(columnName, newColumn);
		return new JobFrame(newData);

	}

	public JobFrameGroup groupBy(String... columnName) {
		Map<List<Object>, List<Integer>> groupedInfo = new HashMap<>();
		for (int i = 0; i < size(); i ++) {

			// generate key
			Row row = getRow(i);
			List<Object> key = new ArrayList<>();
			for (String c: columnName) {
				key.add(row.getField(c));
			}

			// group index
			List<Integer> grList = groupedInfo.getOrDefault(key, new LinkedList<>());
			grList.add(i);
			groupedInfo.put(key, grList);
		}

		return new JobFrameGroup(columnName, groupedInfo, this);
	}

//	public JobFrameGroup groupBy(String... columnNames) {
//
//	}


}
