package com.jobframe.core;

import java.io.*;
import java.util.*;

public class JobFrames {

	public static JobFrame load(String csvPath, List<String> headers) throws FileNotFoundException {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(csvPath));
		List<Class> typeList = findType(bufferedReader);
		List<Column> columnList = new ArrayList<>();
		for (Class clazz: typeList) {
			columnList.add(new Column(clazz));
		}
		bufferedReader.lines().forEach(line -> {
			String[] value = line.split(",");
			for (int i = 0; i < columnList.size(); i ++) {
				try {
					columnList.get(i).append(
							typeList.get(i).cast(value[i])
					);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		});
		Map<String, Column> columnMapper = new HashMap<>();
		for (int i = 0; i < columnList.size(); i ++) {
			columnMapper.put(headers.get(i), columnList.get(i));
		}
		return new JobFrame(columnMapper);
	}

	private static List<Class> findType(BufferedReader bufferedReader) {
		Optional<String> firstLine = bufferedReader.lines().findFirst();
		if (!firstLine.isPresent()) {
			throw new RuntimeException("cant not parse data");
		}
		String line = firstLine.get();
		String[] values = line.split(",");

		List<Class> typeList = new ArrayList<>();
		for (String value: values) {
			typeList.add(getType(value));
		}
		return typeList;
	}

	private static Class getType(String value) {
		if (value.isEmpty())
			return String.class;

		if (isLongType(value))
			return Long.class;

		if (isDoubletype(value))
			return Double.class;

		return String.class;
	}

	private static boolean isLongType(String value) {
		try {
			Long.parseLong(value);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	private static boolean isDoubletype(String value) {
		try {
			Double.parseDouble(value);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}
}
