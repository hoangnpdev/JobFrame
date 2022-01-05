package com.jobframe.core;

import com.jobframe.udf.define.UDF1;
import com.jobframe.udf.define.UDF2;
import com.jobframe.udf.define.UDF3;
import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.Map.Entry;

public class JobFrame {

	private JobFrameData jobFrameData;

	private BiFunction<JobFrameData, JobFrameData, JobFrameData> transform = (JobFrameData self, JobFrameData other) -> self;

	@Setter
	@Getter
	private JobFrame parent;

	@Getter
	@Setter
	private JobFrame other;


	/**
	 * temp eager
	 * @param datas
	 * @param columnNames
	 */
	public JobFrame(List<List<Object>> datas, List<String> columnNames) {
		jobFrameData = new JobFrameData();
		for (String columnName: columnNames) {
			jobFrameData.addColumn(columnName);
		}
		for (List<Object> data: datas) {
			for (int i = 0; i < columnNames.size(); i++) {
				jobFrameData.getColumn(columnNames.get(i)).append(data.get(i));
			}
		}
	}

	/**
	 * temp eager
	 * @param data
	 */
	public JobFrame(Map<String, Column> data) {
		this.jobFrameData = new JobFrameData(data);
	}

	private JobFrame() {
	}


	private JobFrameData execute() {
		JobFrameData result;
		if (parent == null) {
			result = transform.apply(this.jobFrameData, null);
		} else {
			if (other == null) {
				result = transform.apply(this.parent.execute(), null);
			} else {
				result = transform.apply(this.parent.execute(), this.other.execute());
			}
		}
		return result;
	}

	/**
	 * eager
	 * @param name
	 * @return
	 */
	public Column getColumn(String name) {
		return jobFrameData.getColumn(name);
	}

	/**
	 * eager
	 * @param rowIndex
	 * @param columnName
	 * @return
	 */
	public Object at(int rowIndex, String columnName) {
		JobFrameData result = execute();
		return result.getColumn(columnName).get(rowIndex);
	}

	/**
	 * lazy
	 * @param columnName
	 * @param value
	 * @return
	 */
	public JobFrame eqAndGet(String columnName, Object value) {
		JobFrame newJobFrame = new JobFrame();
		BiFunction<JobFrameData, JobFrameData, JobFrameData> transform = (JobFrameData d1, JobFrameData d2) -> {
			Column column = jobFrameData.getColumn(columnName);
			Set<Integer> indexes = column.getIndexes(value);
			Map<String, Column> data = jobFrameData.getColumnMapper().entrySet()
					.stream()
					.collect(
							Collectors.toMap(
									Entry::getKey,
								entry -> entry.getValue().filterByIndexes(indexes)
							)
					);
			JobFrameData result = new JobFrameData(data);
			result.resetIndex();
			return result;
		};
		newJobFrame.setParent(this);
		newJobFrame.transform = transform;

		return newJobFrame;
	}

	/**
	 * eager
	 * @return
	 */
	private Map<String, Column> getData() {
		return jobFrameData.getColumnMapper();
	}

	/**
	 * lazy
	 * @param otherFrame
	 * @param how
	 * @param joinType
	 * @return
	 */
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
			jobFrameData.getColumnMapper().forEach((key, value) -> {
				Column column = value.generateColumnFromKeys(leftKeyList);
				joinData.put(key, column);
			});

			return new JobFrame(joinData);
		}
		throw new RuntimeException("JoinType invalid Exception");
	}

	/**
	 * eager
	 * @param index
	 * @return
	 */
	public Row getRow(int index) {
		Map<String, Object> rData = new HashMap<>();
		jobFrameData.getColumnMapper().forEach((key, value) -> rData.put(key, value.get(index)));
		return new Row(rData);
	}


	/**
	 * lazy
	 * @param expression
	 * @return
	 */
	public JobFrame where(Expression expression) {
		List<Integer> newIndexList = new LinkedList<>();
		for (int i = 0; i < size(); i ++) {
			if(expression.calculate(getRow(i)).equals(true)) {
				newIndexList.add(i);
			}
		}
		Map<String, Column> newData = new HashMap<>();
		jobFrameData.getColumnMapper().forEach((key, value) -> {
			newData.put(key, value.generateColumnFromKeys(newIndexList));
		});
		return new JobFrame(newData);
	}

	/**
	 * lazy
	 * @param columns
	 * @return
	 */
	public JobFrame select(String... columns) {
		Map<String, Column> newData = new HashMap<>();
		for (String column: columns) {
			newData.put(column, jobFrameData.getColumnMapper().get(column));
		}
		return new JobFrame(newData);
	}

	/**
	 * eager
	 * @return
	 */
	public int size() {
		Optional<Entry<String, Column>> op = jobFrameData.getColumnMapper().entrySet().stream().findFirst();
		return op.map(stringColumnEntry -> stringColumnEntry.getValue().size()).orElse(0);
	}

	/**
	 * eager
	 * @return
	 */
	public List<String> columns() {
		return new ArrayList<>(jobFrameData.getColumnMapper().keySet());
	}

	/**
	 * lazy
	 * @param columnName
	 * @param expression
	 * @return
	 */
	public JobFrame withColumn(String columnName, Expression expression) {
		Map<String, Column> newData = new HashMap<>(jobFrameData.getColumnMapper());
		Column newColumn = new Column();
		for (int i = 0; i < size(); i ++) {
			Object value = expression.calculate(getRow(i));
			newColumn.append(value);
		}
		newData.put(columnName, newColumn);
		return new JobFrame(newData);
	}

	/**
	 * lazy
	 * @param columnName
	 * @param udf
	 * @param inputColumn
	 * @param <T1>
	 * @param <R>
	 * @return
	 */
	public <T1, R> JobFrame withColumn(String columnName, UDF1<T1, R> udf, String inputColumn) {
		Map<String, Column> newData = new HashMap<>(jobFrameData.getColumnMapper());
		Column newColumn = new Column();
		for (int i = 0; i < size(); i ++) {
			Row row = getRow(i);
			Object value = udf.apply((T1) row.getField(inputColumn));
			newColumn.append(value);
		}
		newData.put(columnName, newColumn);
		return new JobFrame(newData);

	}

	/**
	 * lazy
	 * @param columnName
	 * @param udf
	 * @param inputColumns
	 * @param <T1>
	 * @param <T2>
	 * @param <R>
	 * @return
	 */
	public <T1, T2, R> JobFrame withColumn(String columnName, UDF2<T1, T2, R> udf, String... inputColumns) {
		Map<String, Column> newData = new HashMap<>(jobFrameData.getColumnMapper());
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

	/**
	 * lazy
	 * @param columnName
	 * @param udf
	 * @param inputColumns
	 * @param <T1>
	 * @param <T2>
	 * @param <T3>
	 * @param <R>
	 * @return
	 */
	public <T1, T2, T3, R> JobFrame withColumn(String columnName, UDF3<T1, T2, T3, R> udf, String... inputColumns) {
		Map<String, Column> newData = new HashMap<>(jobFrameData.getColumnMapper());
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

	/**
	 * lazy
	 * @param columnName
	 * @return
	 */
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

}
