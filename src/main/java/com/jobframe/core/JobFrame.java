package com.jobframe.core;

import com.jobframe.udf.define.UDF1;
import com.jobframe.udf.define.UDF2;
import com.jobframe.udf.define.UDF3;
import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.Map.Entry;

public class JobFrame {

	private JobFrameData jobFrameData;

	@Setter
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

	public JobFrame() {
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
			Column column = d1.getColumn(columnName);
			Set<Integer> indexes = column.getIndexes(value);
			Map<String, Column> data = d1.getColumnMapper().entrySet()
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
		JobFrame newJobFrame = new JobFrame();

		if (joinType.equals("inner")) {

			BiFunction<JobFrameData, JobFrameData, JobFrameData> tranform = (JobFrameData d1, JobFrameData d2) -> {
				Map<String, Column> joinData = new HashMap<>();
				String[] keyKey = how.split("=");
				String leftKey = keyKey[0];
				String rightKey = keyKey[1];
				Column leftKeyColumn = d1.getColumn(leftKey);
				Column rightKeyColumn = d2.getColumn(rightKey);
				List<Entry<Integer, Integer>> innerKeys = leftKeyColumn.getInnerKeyWith(rightKeyColumn);

				List<Integer> rightKeyList = innerKeys.stream().map(Entry::getValue).collect(Collectors.toList());
				d2.getColumnMapper().forEach((key, value) -> {
					Column column = value.generateColumnFromKeys(rightKeyList);
					joinData.put(key, column);
				});

				List<Integer> leftKeyList = innerKeys.stream().map(Entry::getKey).collect(Collectors.toList());
				d1.getColumnMapper().forEach((key, value) -> {
					Column column = value.generateColumnFromKeys(leftKeyList);
					joinData.put(key, column);
				});
				return new JobFrameData(joinData);
			};
			newJobFrame.setParent(this);
			newJobFrame.setOther(otherFrame);
			newJobFrame.transform = transform;
			return newJobFrame;
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
		JobFrame newJobFrame = new JobFrame();

		BiFunction<JobFrameData, JobFrameData, JobFrameData> transform = (JobFrameData d1, JobFrameData d2) -> {
			List<Integer> newIndexList = new LinkedList<>();
			for (int i = 0; i < size(); i++) {
				if (expression.calculate(d1.getRow(i)).equals(true)) {
					newIndexList.add(i);
				}
			}
			Map<String, Column> newData = new HashMap<>();
			d1.getColumnMapper().forEach((key, value) -> {
				newData.put(key, value.generateColumnFromKeys(newIndexList));
			});
			return new JobFrameData(newData);
		};


		newJobFrame.setParent(this);
		newJobFrame.transform = transform;
		return newJobFrame;
	}

	/**
	 * lazy
	 * @param columns
	 * @return
	 */
	public JobFrame select(String... columns) {
		JobFrame newJobFrame = new JobFrame();

		BiFunction<JobFrameData, JobFrameData, JobFrameData> transform = (JobFrameData d1, JobFrameData d2) -> {
			Map<String, Column> newData = new HashMap<>();
			for (String column : columns) {
				newData.put(column, jobFrameData.getColumnMapper().get(column));
			}
			return new JobFrameData(newData);
		};

		newJobFrame.setParent(this);
		newJobFrame.transform = transform;
		return newJobFrame;
	}

	/**
	 * eager
	 * @return
	 */
	public int size() {
		JobFrameData result = execute();
		Optional<Entry<String, Column>> op = result.getColumnMapper().entrySet().stream().findFirst();
		return op.map(stringColumnEntry -> stringColumnEntry.getValue().size()).orElse(0);
	}

	/**
	 * eager
	 * @return
	 */
	public List<String> columns() {
		JobFrameData result = execute();
		return new ArrayList<>(result.getColumnMapper().keySet());
	}

	/**
	 * lazy
	 * @param columnName
	 * @param expression
	 * @return
	 */
	public JobFrame withColumn(String columnName, Expression expression) {
		JobFrame newJobFrame = new JobFrame();

		BiFunction<JobFrameData, JobFrameData, JobFrameData> transform = (JobFrameData d1, JobFrameData d2) -> {
			Map<String, Column> newData = new HashMap<>(jobFrameData.getColumnMapper());
			Column newColumn = new Column();
			for (int i = 0; i < size(); i++) {
				Object value = expression.calculate(getRow(i));
				newColumn.append(value);
			}
			newData.put(columnName, newColumn);

			return new JobFrameData(newData);
		};

		newJobFrame.setParent(this);
		newJobFrame.transform = transform;

		return newJobFrame;
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
		JobFrame jobFrame = new JobFrame();

		BiFunction<JobFrameData, JobFrameData, JobFrameData> transforming = (JobFrameData d1, JobFrameData d2) -> {
			Map<String, Column> newData = new HashMap<>(d1.getColumnMapper());
			Column newColumn = new Column();
			for (int i = 0; i < size(); i++) {
				Row row = getRow(i);
				Object value = udf.apply((T1) row.getField(inputColumn));
				newColumn.append(value);
			}
			newData.put(columnName, newColumn);
			return new JobFrameData(newData);
		};

		jobFrame.setParent(this);
		jobFrame.setTransform(transforming);
		return jobFrame;
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
		JobFrame jobFrame = new JobFrame();

		BiFunction<JobFrameData, JobFrameData, JobFrameData> transforming = (JobFrameData d1, JobFrameData d2) -> {
			Map<String, Column> newData = new HashMap<>(d1.getColumnMapper());
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
			return new JobFrameData(newData);
		};

		jobFrame.setParent(this);
		jobFrame.setTransform(transforming);
		return jobFrame;

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
		JobFrame jobFrame = new JobFrame();

		BiFunction<JobFrameData, JobFrameData, JobFrameData> transforming = (JobFrameData d1, JobFrameData d2) -> {
			Map<String, Column> newData = new HashMap<>(d1.getColumnMapper());
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
			return new JobFrameData(newData);
		};

		jobFrame.setParent(this);
		jobFrame.setTransform(transforming);
		return jobFrame;

	}

	/**
	 * lazy
	 * @param columnName
	 * @return
	 */
	public JobGroup groupBy(String... columnName) {
		JobGroup newJobGroup = new JobGroup();

		BiFunction<JobFrameData, JobFrameData, JobGroupData> grouping = (JobFrameData d1, JobFrameData d2) -> {
			Map<List<Object>, List<Integer>> groupedInfo = new HashMap<>();
			for (int i = 0; i < size(); i++) {

				// generate key
				Row row = getRow(i);
				List<Object> key = new ArrayList<>();
				for (String c : columnName) {
					key.add(row.getField(c));
				}

				// group index
				List<Integer> grList = groupedInfo.getOrDefault(key, new LinkedList<>());
				grList.add(i);
				groupedInfo.put(key, grList);
			}
			return new JobGroupData(columnName, groupedInfo, d1);
		};

		newJobGroup.setJobFrame(this);
		newJobGroup.setGrouping(grouping);
		return newJobGroup;
	}

}
