package com.jobframe.core;

import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JobFrameData {

	// data

	private Map<String, Object> columnMapper;

	private Set<String> leftColumnSet;

	private Set<String> rightColumnSet;

	@Setter
	private Map<Integer, Integer> rowLeftIndex; // todo: caching this

	@Setter
	private Map<Integer, Integer> rowRightIndex; // todo: caching this

	// logic

	public JobFrameData() {
		columnMapper = new HashMap<>();
	}

	public JobFrameData(Map<String, Object> columnMapper) {
		throw new UnsupportedOperationException("temp for code");
	}

	public JobFrameData(Map<String, Object> columnMapper, Integer size) {
		this.columnMapper = columnMapper;
		this.rowLeftIndex = new MirrorMap(size);
	}

	public static JobFrameData ref(JobFrameData prev) {
		JobFrameData newFrame = new JobFrameData();
		prev.columnMapper
				.keySet()
				.forEach(key -> {
					newFrame.columnMapper.put(key, prev);
				});
		newFrame.leftColumnSet = prev.columnMapper.keySet();
		return newFrame;
	}

	public void addColumn(String columnName, RandomAccessFile randomAccessFile, Class type) throws FileNotFoundException {
		columnMapper.put(columnName, new Column(randomAccessFile, type));
	}

	public Iterator<Object> columnAsIterator(String columnName) {
		return rowLeftIndex.keySet()
				.stream()
				.map(index -> at(index, columnName))
				.iterator();
	}

	// fixme remove it
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

	public static class MirrorMap implements Map<Integer, Integer> {

		private int size;

		public MirrorMap(int size) {
			this.size = size;
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public boolean isEmpty() {
			if (size < 1) {
				return true;
			}
			return false;
		}

		@Override
		public boolean containsKey(Object key) {
			Integer k = (int) key;
			if (k < size) {
				return true;
			}
			return false;
		}

		@Override
		public boolean containsValue(Object value) {
			Integer v = (int) value;
			if (v < size) {
				return true;
			}
			return false;
		}

		@Override
		public Integer get(Object key) {
			return (Integer) key;
		}


		@Nullable
		@Override
		public Integer put(Integer key, Integer value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Integer remove(Object key) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void putAll(@NotNull Map<? extends Integer, ? extends Integer> m) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}

		@NotNull
		@Override
		public Set<Integer> keySet() {
			return IntStream.range(0, size).boxed().collect(Collectors.toSet());
		}

		@NotNull
		@Override
		public Collection<Integer> values() {
			return IntStream.range(0, size).boxed().collect(Collectors.toSet());
		}

		@NotNull
		@Override
		public Set<Entry<Integer, Integer>> entrySet() {
			return IntStream.range(0, size).boxed()
					.map(i -> new AbstractMap.SimpleEntry<>(i, i))
					.collect(Collectors.toSet());
		}
	}
}
