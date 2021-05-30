package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final int END_BYTES_COUNT = 2;

    private final BufferedInputStream is;

    public RespReader(InputStream is) {
        this.is = new BufferedInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        if (is.available() <= 0) {
            return false;
        }

        is.mark(1);
        byte class_code = (byte) is.read();
        is.reset();

        return class_code == RespArray.CODE;
    }

    public boolean hasAvailableData() {
        try {
            return is.available() > 0;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        while (is.available() <= 0) {
            // Ждем данных
        }

        /* if (is.available() <= 0) {
            throw new EOFException("End of stream reached");
        } */

        is.mark(1);
        byte class_code = (byte) is.read();
        is.reset();

        switch (class_code) {
            case RespArray.CODE:
                return readArray();
            case RespBulkString.CODE:
                return readBulkString();
            case RespCommandId.CODE:
                return readCommandId();
            case RespError.CODE:
                return readError();
            default:
                close();
                throw new IOException("Invalid code symbol: " + (char) class_code);
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        if (is.available() <= 0) {
            close();
            throw new EOFException("End of stream reached");
        }

        validateRespClassCode((byte) is.read(), RespError.CODE);
        byte[] data = readUntilCRLF();

        return new RespError(data);
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        if (is.available() <= 0) {
            close();
            throw new EOFException("End of stream reached");
        }

        validateRespClassCode((byte) is.read(), RespBulkString.CODE);

        int bytesToRead = Integer.parseInt(new String(readUntilCRLF()));
        if (bytesToRead == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }

        byte [] stringData = is.readNBytes(bytesToRead);
        is.skip(END_BYTES_COUNT);

        return new RespBulkString(stringData);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        if (is.available() <= 0) {
            close();
            throw new EOFException("End of stream reached");
        }

        validateRespClassCode((byte) is.read(), RespArray.CODE);

        int objectsToRead = Integer.parseInt(new String(readUntilCRLF()));
        RespObject[] objects = new RespObject[objectsToRead];

        for (int i = 0; i < objectsToRead; i++) {
            objects[i] = readObject();
        }

        return new RespArray(objects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        if (is.available() <= 0) {
            close();
            throw new EOFException("End of stream reached");
        }

        validateRespClassCode((byte) is.read(), RespCommandId.CODE);
        int commandId = ByteBuffer.wrap(is.readNBytes(4)).getInt();
        is.skip(END_BYTES_COUNT);

        return new RespCommandId(commandId);
    }

    private void validateRespClassCode(byte providedCode, byte expectedCode) throws IOException {
        if (providedCode != expectedCode) {
            throw new IOException(String.format("Invalid error code. Supposed: '%c'. Got: '%c'", (char) expectedCode, (char) providedCode));
        }
    }

    private byte[] readUntilCRLF() throws IOException {
        LinkedList<Byte> data = new LinkedList<>();
        boolean isPreviousCR = false;
        byte c = (byte) is.read();

        while (!isPreviousCR || c != LF) {
            if (isPreviousCR) {
                data.add(CR);
            }

            if (c == CR) {
                isPreviousCR = true;
            } else {
                data.add(c);
            }

            c = (byte) is.read();
        }

        byte[] result = new byte[data.size()];

        for (int i = 0; i < data.size(); i++) {
            result[i] = data.get(i);
        }

        return result;
    }

    @Override
    public void close() throws IOException {
        is.close();
    }
}
