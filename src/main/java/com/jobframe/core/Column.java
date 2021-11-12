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
}
