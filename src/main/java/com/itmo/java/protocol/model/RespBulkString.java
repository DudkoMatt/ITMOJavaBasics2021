package com.itmo.java.protocol.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    private final byte[] data;

    /**
     * Код объекта
     */
    public static final byte CODE = '$';

    public static final int NULL_STRING_SIZE = -1;

    public RespBulkString(byte[] data) {
        this.data = data;
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
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
        return new String(data);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        try (DataOutputStream outputStream = new DataOutputStream(os)) {
            outputStream.write(CODE);

            if (data == null) {
                outputStream.writeInt(NULL_STRING_SIZE);
            } else {
                outputStream.writeInt(data.length);
                outputStream.write(data);
            }

            outputStream.write(CRLF);
        }
    }
}
