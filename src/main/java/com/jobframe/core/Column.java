package com.jobframe.core;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import java.util.Map.Entry;

public class Column {

	private static byte[] NULL_BYTE = DatatypeConverter.parseHexBinary("00");

	private Class type;

	private RandomAccessFile cells;

	// fixme: temporary use for unit testing
	public Column() {

	}

	public Column(Class clazz) throws FileNotFoundException {
		String tmpName = UUID.randomUUID().toString();
		cells = new RandomAccessFile("tmp/" + tmpName + ".col", "rw");
		this.type = clazz;
	}

	public Column(RandomAccessFile randomAccessFile, Class clazz) {
		cells = randomAccessFile;
		this.type = clazz;
	}

	public void append(Object data) {
		try {
			byte[] raw = toByte(data);
			cells.seek(cells.length());
			cells.write(raw);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e.getCause());
		}
	}

	private int getTypeSize() {
		if (type.getName().equals(String.class.getName()))
			return 64000;
		if (type.getName().equals(Double.class.getName()))
			return 84;
		if (type.getName().equals(Long.class.getName()))
			return 82;
		throw new RuntimeException("type not found!");
	}

	private byte[] toByte(Object data) throws IOException {
		int typeSize = getTypeSize();
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(data);
		oos.flush();
		oos.close();
		return bos.toByteArray();
	}

	private Object fromByte(byte[] value) throws IOException, ClassNotFoundException {
		ByteArrayInputStream in = new ByteArrayInputStream(value);
		ObjectInputStream is = new ObjectInputStream(in);
		Object obj = is.readObject();
		is.close();
		return obj;
	}

	public int size() {
		// fixme: current is length in byte
		try {
			return (int) cells.length();
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e.getCause());
		}
	}

	public Class type() {
		return type;
	}

	public Object get(Integer index) {
		byte[] value = new byte[getTypeSize()];
		try {
			cells.seek(index * getTypeSize());
			System.out.println("No. byte read: " + cells.read(value,index * getTypeSize(), getTypeSize()));
			return fromByte(value);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e.getMessage(), e.getCause());
		}
	}

	// fixme
	public Set<Entry<Integer, Object>> entrySet() {
		return null;
//		return cells.entrySet();
	}

	// fixme
	public Set<Integer> getIndexes(Object value) {
		return null;
//		return cells.entrySet()
//				.stream()
//				.filter(entry -> entry.getValue().equals(value))
//				.map(Entry::getKey)
//				.collect(Collectors.toSet());
	}

	// fixme remove it
	public Column filterByIndexes(Set<Integer> indexes) {
//		Map<Integer, Object> filteredData = cells.entrySet()
//				.stream()
//				.filter(entry -> indexes.contains(entry.getKey()))
//				.collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
//		return new Column(filteredData);
		return null;
	}

	// fixme remove it
	public void resetIndex() {
//		List<Object> data = cells.entrySet()
//				.stream()
//				.sorted(Entry.comparingByKey())
//				.map(Entry::getValue)
//				.collect(Collectors.toList());
//		LinkedHashMap<Integer, Object> newCell = new LinkedHashMap<>();
//		for (int idx = 0; idx < data.size(); idx++) {
//			newCell.put(idx, data.get(idx));
//		}
//		cells = newCell;
	}

	// fixme remove it
	public List<Integer> getValueSortedKeyList() {
		return null;
//		List<Entry<Integer, Object>> entrySet = new ArrayList<>(cells.entrySet());
//		entrySet.sort((Entry<Integer, Object> a, Entry<Integer, Object> b) -> {
//			Object valueA = a.getValue();
//			Object valueB = b.getValue();
//			if (valueA instanceof Double) {
//				return ((Double) valueA).compareTo((Double) valueB);
//			}
//			if (valueA instanceof Long) {
//				return ((Long) valueA).compareTo((Long) valueB);
//			}
//			return ((String) valueA).compareTo((String) valueB);
//		});
//		return entrySet.stream().map(Entry::getKey).collect(Collectors.toList());
	}

	// fixme: remove it
	public Column generateColumnFromKeys(List<Integer> indexes) {
//		LinkedHashMap<Integer, Object> newColumnData = new LinkedHashMap<>();
//		for (int newIndex = 0; newIndex < indexes.size(); newIndex ++) {
//			Object newValue = indexes.get(newIndex) == null ? null : cells.get(indexes.get(newIndex));
//			newColumnData.put(newIndex, newValue);
//		}
//		return new Column(newColumnData);
		return null;
	}

	// fixme remove it
	public Collection<Object> values() {
		return null;
//		return cells.values();
	}
}
