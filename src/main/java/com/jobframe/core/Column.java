package com.jobframe.core;

import java.util.*;
import java.util.stream.Collectors;

import java.util.Map.Entry;

public class Column {

	LinkedHashMap<Integer, Object> cells;

	public Column() {
		cells = new LinkedHashMap<>();
	}

	public Column(Map<Integer, Object> data) {
		cells = new LinkedHashMap<>(data);
	}

	public void append(Object data) {
		cells.put(cells.size(), data);
	}

	public int size() {
		return cells.size();
	}

	public Class type() {
		return cells.get(0).getClass();
	}

	public Object get(Integer index) {
		return cells.get(index);
	}

	public Set<Entry<Integer, Object>> entrySet() {
		return cells.entrySet();
	}

	public Set<Integer> getIndexes(Object value) {
		return cells.entrySet()
				.stream()
				.filter(entry -> entry.getValue().equals(value))
				.map(Entry::getKey)
				.collect(Collectors.toSet());
	}

	public Column filterByIndexes(Set<Integer> indexes) {
		Map<Integer, Object> filteredData = cells.entrySet()
				.stream()
				.filter(entry -> indexes.contains(entry.getKey()))
				.collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
		return new Column(filteredData);
	}

	public void resetIndex() {
		List<Object> data = cells.entrySet()
				.stream()
				.sorted(Entry.comparingByKey())
				.map(Entry::getValue)
				.collect(Collectors.toList());
		LinkedHashMap<Integer, Object> newCell = new LinkedHashMap<>();
		for (int idx = 0; idx < data.size(); idx++) {
			newCell.put(idx, data.get(idx));
		}
		cells = newCell;
	}

	public List<Entry<Integer, Integer>> getInnerKeyWith(Column rightKeyColumn) {
		List<Entry<Integer, Integer>> result = new LinkedList<>();

		Set<Entry<Integer, Object>> leftEntrySet = cells.entrySet();
		Set<Entry<Integer, Object>> rightEntrySet = rightKeyColumn.cells.entrySet();

		for (Entry<Integer, Object> leftEntry: leftEntrySet) {
			for (Entry<Integer, Object> rightEntry: rightEntrySet) {
				if (leftEntry.getValue().equals(rightEntry.getValue())) {
					result.add(new AbstractMap.SimpleEntry<>(leftEntry.getKey(), rightEntry.getKey()));
				}
			}
		}

		return result;
	}

	public List<Integer> getValueSortedKeyList() {
		List<Entry<Integer, Object>> entrySet = new ArrayList<>(cells.entrySet());
		entrySet.sort((Entry<Integer, Object> a, Entry<Integer, Object> b) -> {
			Object valueA = a.getValue();
			Object valueB = b.getValue();
			if (valueA instanceof Double) {
				return ((Double) valueA).compareTo((Double) valueB);
			}
			if (valueA instanceof Long) {
				return ((Long) valueA).compareTo((Long) valueB);
			}
			return ((String) valueA).compareTo((String) valueB);
		});
		return entrySet.stream().map(Entry::getKey).collect(Collectors.toList());
	}

	public Column generateColumnFromKeys(List<Integer> indexes) {
		LinkedHashMap<Integer, Object> newColumnData = new LinkedHashMap<>();
		for (int newIndex = 0; newIndex < indexes.size(); newIndex ++) {
			newColumnData.put(newIndex, cells.get(indexes.get(newIndex)));
		}
		return new Column(newColumnData);
	}

	public Collection<Object> values() {
		return cells.values();
	}
}
