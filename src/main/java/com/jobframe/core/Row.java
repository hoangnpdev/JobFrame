package com.jobframe.core;

import java.util.Map;

public class Row {

    private Map<String, Object> data;

    public Row(Map<String, Object> data) {
        this.data = data;
    }

    public Object getField(String name) {
        return data.get(name);
    }
}
