package com.jobframe.core;

import sun.awt.image.ImageWatched;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import java.util.Map.Entry;

public class Column {

	private static byte[] NULL_BYTE = DatatypeConverter.parseHexBinary("00");

	private Class type;

	private RandomAccessFile cells;

	public Column(Class clazz) throws FileNotFoundException {
		String tmpName = UUID.randomUUID().toString();
		cells = new RandomAccessFile("tmp/" + tmpName + ".col", "rw");
		this.type = clazz;
	}

	public Column(RandomAccessFile randomAccessFile, Class clazz) {
		cells = randomAccessFile;
		this.type = clazz;
	}

	public void append(Object data) throws IOException {
		byte[] raw = toByte(data);
		cells.write(raw, (int) cells.length() - 1, raw.length);
	}

	private int getTypeSize() {
		if (type.getName().equals(String.class.getName()))
			return 64000;
		if (type.getName().equals(Double.class.getName()))
			return 8;
		if (type.getName().equals(Long.class.getName()))
			return 8;
		throw new RuntimeException("type not found!");
	}

	private byte[] toByte(Object data) throws IOException {
		int typeSize = getTypeSize();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(data);
		oos.flush();
		for (int i = 0; i < typeSize - bos.size(); i++) {
			oos.write(NULL_BYTE);
		}
		oos.flush();
		return bos.toByteArray();
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
			Object newValue = indexes.get(newIndex) == null ? null : cells.get(indexes.get(newIndex));
			newColumnData.put(newIndex, newValue);
		}
		return new Column(newColumnData);
	}

	public Collection<Object> values() {
		return cells.values();
	}
}
