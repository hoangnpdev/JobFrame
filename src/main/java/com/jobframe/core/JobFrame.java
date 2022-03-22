package com.jobframe.core;

import com.jobframe.udf.define.UDF1;
import com.jobframe.udf.define.UDF2;
import com.jobframe.udf.define.UDF3;
import lombok.Getter;
import lombok.Setter;

import java.io.RandomAccessFile;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.Map.Entry;
import java.util.stream.IntStream;

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
	 */
	// fixme remove this function
//	public JobFrame(List<List<Object>> datas, List<String> columnNames) {
//		jobFrameData = new JobFrameData();
//		for (String columnName: columnNames) {
//			jobFrameData.addColumn(columnName);
//		}
//		for (List<Object> data: datas) {
//			for (int i = 0; i < columnNames.size(); i++) {
//				jobFrameData.getColumn(columnNames.get(i)).append(data.get(i));
//			}
//		}
//	}


	/**
	 * temp eager
	 * @param data
	 */
	public JobFrame(Map<String, Object> data, Integer size) {
		this.jobFrameData = new JobFrameData(data, size);
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
		return result.at(rowIndex, columnName);
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
			// init column Mapper and left column set
			JobFrameData result = JobFrameData.ref(d1);

			// init row left index
			int i = 0;
			int j = 0;
			Map<Integer, Integer> leftIndex = new HashMap<>();
			Iterator<Object> col = d1.columnAsIterator(columnName);
			for (Iterator<Object> it = col; it.hasNext(); ) {
				Object cell = it.next();
				if (cell.equals(value)) {
					leftIndex.put(i, j);
					i ++;
				}
				j ++;
			}
			result.setRowLeftIndex(leftIndex);
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
	private Map<String, Object> getData() {
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

		BiFunction<JobFrameData, JobFrameData, JobFrameData> transform = (JobFrameData d1, JobFrameData d2) -> {
			Map<String, Object> joinColumnMapper = new HashMap<>();
			// generate column mapper
			d1.getColumnMapper().forEach((k, v) -> {
				joinColumnMapper.put(k, d1);
			});
			d2.getColumnMapper().forEach((k, v) -> {
				joinColumnMapper.put(k, d2);
			});

			// generate leftColumnSet
			Set<String> rightSet = d2.getColumnMapper().keySet();
			Set<String> leftSet = d1.getColumnMapper().keySet();
			Set<String> joinColumnSet = leftSet.stream().filter(e -> !rightSet.contains(e)).collect(Collectors.toSet());


			// generate indexing between JobFrameData instances
			Iterator<Entry<Integer, Integer>> matchedKeyPairs = getMatchedKeyPairs(d1, d2, how, joinType);
			Map<Integer, Integer> rowLeftIndex = new HashMap<>();
			Map<Integer, Integer> rowRightIndex = new HashMap<>();
			int newKey = 0;
			while (matchedKeyPairs.hasNext()) {
				Entry<Integer, Integer> keyPair = matchedKeyPairs.next();
				rowLeftIndex.put(newKey, keyPair.getKey());
				rowRightIndex.put(newKey, keyPair.getValue());
				newKey ++;
			}

			return new JobFrameData(joinColumnMapper, joinColumnSet,
					rowLeftIndex, rowRightIndex);
		};

		newJobFrame.setParent(this);
		newJobFrame.setOther(otherFrame);
		newJobFrame.transform = transform;
		return newJobFrame;
	}

	private Iterator<Entry<Integer, Integer>> getMatchedKeyPairs(
			JobFrameData d1, JobFrameData d2,
			String how, String joinType
	) {
		List<String> leftColumnList = new ArrayList<>();
		List<String> rightColumnList = new ArrayList<>();

		// get pair list
		String[] keyPairList = how.split(";");
		for (String keyPair: keyPairList) {
			String[] keys = keyPair.split("=");
			leftColumnList.add(keys[0].trim());
			rightColumnList.add(keys[1].trim());
		}

		// get common keys
		return findMatchedKeyPairsByJoinType(d1, d2, leftColumnList, rightColumnList, joinType);
	}

	private Iterator<Entry<Integer, Integer>> findMatchedKeyPairsByJoinType(
			JobFrameData leftData, JobFrameData rightData,
			List<String> leftColumnList, List<String> rightColumnList,
			String joinType
	) {
		if (joinType.equals("inner")) {
			return findInnerMatchedKeyPairs(
					leftData, rightData,
					leftColumnList, rightColumnList
			);
		}
//		if (joinType.equals("left")) {
//			return findLeftCommonKey(leftColumnList, rightColumnList);
//		}
//		if (joinType.equals("full")) {
//			return findFullCommonKey(leftColumnList, rightColumnList);
//		}
		throw new RuntimeException("Join type - " + joinType + " not found! ");
	}

	private boolean isMatch(int lid, int rid, JobFrameData leftData, JobFrameData rightData, List<String> leftColumnList, List<String> rightColumnList) {
		boolean result = true;
		int nCol = leftColumnList.size();
		assert nCol == rightColumnList.size();
		for (int i = 0; i < nCol; i ++) {
			result = result && leftData.at(lid, leftColumnList.get(i)).equals(
					rightData.at(rid, rightColumnList.get(i))
			);
		}
		return result;
	}

	private Iterator<Entry<Integer, Integer>> findInnerMatchedKeyPairs (
			JobFrameData leftData, JobFrameData rightData,
			List<String> leftColumnList, List<String> rightColumnList
	) {
		List<Entry<Integer, Integer>> result = new LinkedList<>();
		int leftSize = leftData.size();
		int rightSize = rightData.size();
		IntStream.range(0, leftSize * rightSize)
				.boxed()
				.map(k -> {
					int i = k / rightSize ;
					int j = k % rightSize ;
					if (isMatch(i, j, leftData, rightData, leftColumnList, rightColumnList)) {
						result.add(
								new AbstractMap.SimpleEntry<>(i, j)
						);
					}
					return result;
				});
		// convert this to iterator
		return null;
	}

	private List<Entry<Integer, Integer>> findLeftCommonKey(
			List<Column> leftColumnList, List<Column> rightColumnList
	) {
		List<Entry<Integer, Integer>> result = new LinkedList<>();
		int leftSize = leftColumnList.get(0).size();
		int rightSize = rightColumnList.get(0).size();
		for (int i = 0; i < leftSize; i ++) {
			List<Entry<Integer, Integer>> matchedJoin = new LinkedList<>();
			for (int j = 0; j < rightSize; j ++ ) {
				if (isMatch(i, j, null, null, null, null)) {
					matchedJoin.add(
							new AbstractMap.SimpleEntry<>(i, j)
					);
				}
			}
			if (matchedJoin.isEmpty()) {
				matchedJoin.add(
						new AbstractMap.SimpleEntry<>(
								i,
								null
						)
				);
			}
			result.addAll(matchedJoin);
		}
		return result;
	}

	private List<Entry<Integer, Integer>> findFullCommonKey(
			List<Column> leftColumnList, List<Column> rightColumnList
	) {
		List<Entry<Integer, Integer>> result = new LinkedList<>();
		int leftSize = leftColumnList.get(0).size();
		int rightSize = rightColumnList.get(0).size();
		Set<Integer> usedRightIndexMarker = new HashSet<>();
		for (int i = 0; i < leftSize; i ++) {
			List<Entry<Integer, Integer>> matchedJoin = new LinkedList<>();
			for (int j = 0; j < rightSize; j ++ ) {
				if (isMatch(i, j, null, null, null, null)) {
					matchedJoin.add(
							new AbstractMap.SimpleEntry<>(i, j)
					);
					usedRightIndexMarker.add(j);
				}
			}
			if (matchedJoin.isEmpty()) {
				matchedJoin.add(
						new AbstractMap.SimpleEntry<>(
								i,
								null
						)
				);
			}
			result.addAll(matchedJoin);

		}
		for (int i = 0; i < rightSize; i ++) {
			if(!usedRightIndexMarker.contains(i)) {
				result.add(
					new AbstractMap.SimpleEntry<>(null, i)
				);
			}
		}
		return result;
	}



	/**
	 * eager
	 * @param index
	 * @return
	 */
	public Row getRow(int index) {
		Map<String, Object> rData = new HashMap<>();
		jobFrameData.getColumnMapper().forEach((key, value) -> rData.put(key, ((Column) value).get(index)));
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
			Map<String, Object> newData = new HashMap<>();
			d1.getColumnMapper().forEach((key, value) -> {
				newData.put(key, ((Column) value).generateColumnFromKeys(newIndexList));
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
			Map<String, Object> newData = new HashMap<>();
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
		Optional<Entry<String, Object>> op = result.getColumnMapper().entrySet().stream().findFirst();
		return op.map(stringColumnEntry -> ((Column) stringColumnEntry.getValue()).size()).orElse(0);
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
			Map<String, Object> newData = new HashMap<>(jobFrameData.getColumnMapper());
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
			Map<String, Object> newData = new HashMap<>(d1.getColumnMapper());
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
			Map<String, Object> newData = new HashMap<>(d1.getColumnMapper());
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
			Map<String, Object> newData = new HashMap<>(d1.getColumnMapper());
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

	/**
	 * lazy
	 * @param columnName
	 * @return
	 */
	public JobFrame sort(String columnName) {
		JobFrame jobFrame = new JobFrame();

		BiFunction<JobFrameData, JobFrameData, JobFrameData> transforming = (JobFrameData d1, JobFrameData d2) -> {
			HashMap<String, Object> newData = new HashMap<>();
			List<Integer> valueSortedKeyList = d1.getColumn(columnName).getValueSortedKeyList();
			d1.getColumnMapper().forEach((cKey, cValue) -> {
				newData.put(cKey, ((Column) cValue).generateColumnFromKeys(valueSortedKeyList));
			});
			return new JobFrameData(newData);
		};

		jobFrame.setParent(this);
		jobFrame.setTransform(transforming);
		return jobFrame;
	}

}
