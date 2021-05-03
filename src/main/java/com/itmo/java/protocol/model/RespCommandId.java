package com.itmo.java.protocol.model;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Id
 */
public class RespCommandId implements RespObject {
    private final int commandId;

    /**
     * Код объекта
     */
    public static final byte CODE = '!';

    public RespCommandId(int commandId) {
        this.commandId = commandId;
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

    @Override
    public String asString() {
        return String.valueOf(commandId);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(ByteBuffer.allocate(4).putInt(commandId).array());
        os.write(CRLF);
    }
}
