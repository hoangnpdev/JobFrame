package com.jobframe.core;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class JobFrames {

	private static Logger log = Logger.getLogger(JobFrames.class);

	public static JobFrame load(String csvPath, List<String> headers) throws IOException {
		log(csvPath);
		mkdirs("tmp");
		BufferedReader bufferedReader = new BufferedReader(new FileReader(csvPath));
		List<Class> typeList = findType(new BufferedReader(new FileReader(csvPath)));
		List<Column> columnList = new ArrayList<>();
		for (Class clazz: typeList) {
			columnList.add(new Column(clazz));
		}

		// convert csv to randomAccessFile
		AtomicInteger numberOfLine = new AtomicInteger(0);
		bufferedReader.lines().forEach(line -> {
			numberOfLine.incrementAndGet();
			String[] value = line.split(",");
			for (int i = 0; i < columnList.size(); i ++) {

					columnList.get(i).append(
							parse(value[i], typeList.get(i))
					);

			}
		});
		Column col = columnList.get(1);
		System.out.println(col.get(0));
//		System.out.println(col.get(0) + " " + col.get(1) + " " +  col.get(2));

		// create data for frame
		Map<String, Object> columnMapper = new HashMap<>();
		for (int i = 0; i < columnList.size(); i ++) {
			columnMapper.put(headers.get(i), columnList.get(i));
		}

		// todo: create row index
		return new JobFrame(columnMapper, numberOfLine.get());
	}

	private static List<Class> findType(BufferedReader bufferedReader) throws IOException {
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
		bufferedReader.close();
		return typeList;
	}

	private static Object parse(String value, Class type) {
		if (type.equals(Long.class)) {
			return Long.parseLong(value);
		}
		if (type.equals(Double.class)) {
			return Double.parseDouble(value);
		}
		return value;
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

	private static void mkdirs(String path) {
		File file = new File(path);
		if (file.mkdirs()) {
			log("working directory at: " + file.getAbsolutePath());
		} else {
			log("create tmp dir failed!");
		}
	}

	private static void log(Object object) {
		System.out.println(object);
	}
}
