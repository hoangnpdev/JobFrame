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
}
