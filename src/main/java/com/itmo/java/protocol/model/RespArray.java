package com.itmo.java.protocol.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {
    List<RespObject> objects;

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    public RespArray(RespObject... objects) {
        this.objects = new LinkedList<>(Arrays.asList(objects));
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        StringJoiner stringJoiner = new StringJoiner(" ");
        for (RespObject object : objects) {
            stringJoiner.add(object.asString());
        }
        
        return stringJoiner.toString();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        try (DataOutputStream outputStream = new DataOutputStream(os)) {
            outputStream.write(CODE);
            outputStream.write(objects.size());
            outputStream.write(CRLF);

            for (RespObject object : objects) {
                object.write(os);
            }
        }
    }

    public List<RespObject> getObjects() {
        return objects;
    }
}
