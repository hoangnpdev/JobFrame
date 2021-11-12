package com.jobframe.core;

import java.util.LinkedHashMap;

public class Column {

	LinkedHashMap<Integer, Object> cells;

	public Column() {
		cells = new LinkedHashMap<>();
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
}
