package com.jobframe.core;

import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.*;

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
		this.columnMapper = columnMapper;
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

	public static class MirrorMap<K, V> implements Map<K, V> {

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
			return false;
		}

		@Override
		public boolean containsKey(Object key) {
			return false;
		}

		@Override
		public boolean containsValue(Object value) {
			return false;
		}

		@Override
		public V get(Object key) {
			return null;
		}

		@Nullable
		@Override
		public V put(K key, V value) {
			return null;
		}

		@Override
		public V remove(Object key) {
			return null;
		}

		@Override
		public void putAll(@NotNull Map<? extends K, ? extends V> m) {

		}

		@Override
		public void clear() {

		}

		@NotNull
		@Override
		public Set<K> keySet() {
			return null;
		}

		@NotNull
		@Override
		public Collection<V> values() {
			return null;
		}

		@NotNull
		@Override
		public Set<Entry<K, V>> entrySet() {
			return null;
		}
	}
}
