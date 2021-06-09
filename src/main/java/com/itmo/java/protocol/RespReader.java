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

    private final BufferedInputStream is;

    public RespReader(InputStream is) {
        this.is = new BufferedInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return hasNextCode(RespArray.CODE);
    }

    public boolean hasNextCode(byte code) throws IOException {
        is.mark(1);
        byte classCode;

        try {
            classCode = readNextByteFromIOStream();
        } catch (IOException e) {
            return false;
        }

        is.reset();
        return classCode == code;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        is.mark(1);
        byte classCode = readNextByteFromIOStream();
        is.reset();

        switch (classCode) {
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
                throw new IOException("Invalid code symbol: " + (char) classCode);
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        validateRespClassCode(readNextByteFromIOStream(), RespError.CODE);
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
        validateRespClassCode(readNextByteFromIOStream(), RespBulkString.CODE);

        int bytesToRead = Integer.parseInt(new String(readUntilCRLF()));
        if (bytesToRead == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }

        byte[] stringData = readNextNBytesFromIOStream(bytesToRead);
        readCRLFFromIOStream();

        return new RespBulkString(stringData);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        validateRespClassCode(readNextByteFromIOStream(), RespArray.CODE);

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
        validateRespClassCode(readNextByteFromIOStream(), RespCommandId.CODE);
        int commandId = ByteBuffer.wrap(readNextNBytesFromIOStream(4)).getInt();
        readCRLFFromIOStream();

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
        byte c = readNextByteFromIOStream();

        while (!isPreviousCR || c != LF) {
            isPreviousCR = c == CR;
            data.add(c);
            c = readNextByteFromIOStream();
        }

        byte[] result = new byte[data.size() - 1];

        for (int i = 0; i < data.size() - 1; i++) {
            result[i] = data.get(i);
        }

        return result;
    }

    private byte readNextByteFromIOStream() throws IOException {
        int data = is.read();
        if (data == -1) {
            close();
            throw new EOFException("End of stream reached");
        }

        return (byte) data;
    }

    private byte[] readNextNBytesFromIOStream(int n) throws IOException {
        byte[] data = is.readNBytes(n);

        if (data.length != n) {
            throw new IOException(String.format("Cannot read %d bytes, read only %d", n, data.length));
        }

        return data;
    }

    private void readCRLFFromIOStream() throws IOException {
        int byteCR = is.read();
        int byteLF = is.read();

        if (byteCR == -1 || byteLF == -1) {
            throw new EOFException("End of stream reached");
        }

        if ((byte) byteCR != CR || (byte) byteLF != LF) {
            throw new IOException("Error occurred during reading");
        }
    }

    @Override
    public void close() throws IOException {
        is.close();
    }
}
