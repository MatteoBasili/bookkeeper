package org.apache.bookkeeper.bookie.utils;

import io.netty.buffer.*;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Arrays;

import static org.mockito.Mockito.*;

public class Utils {

    public static final String BC_TEST_FILE = "bc_test_file.txt";

    public static final String BC_FC_CONTENT = "Hello world!";
    public static final String BB_CONTENT = "Ciao mondo!!";

    private Utils() {
        // Classe di utilità
    }

    // ===================== BYTEBUF ALLOCATORS ===================== //

    public static ByteBufAllocator unpooledByteBufAllocator() {
        return UnpooledByteBufAllocator.DEFAULT;
    }

    public static ByteBufAllocator invalidByteBufAllocator() {
        ByteBufAllocator bba = mock(ByteBufAllocator.class);

        // Mock generico per tutti i ByteBuf
        when(bba.buffer()).thenReturn(null);
        when(bba.buffer(anyInt())).thenReturn(null);
        when(bba.buffer(anyInt(), anyInt())).thenReturn(null);
        when(bba.compositeBuffer()).thenReturn(null);
        when(bba.compositeBuffer(anyInt())).thenReturn(null);
        when(bba.compositeDirectBuffer()).thenReturn(null);
        when(bba.compositeDirectBuffer(anyInt())).thenReturn(null);
        when(bba.compositeHeapBuffer()).thenReturn(null);
        when(bba.compositeHeapBuffer(anyInt())).thenReturn(null);
        when(bba.directBuffer()).thenReturn(null);
        when(bba.directBuffer(anyInt())).thenReturn(null);
        when(bba.directBuffer(anyInt(), anyInt())).thenReturn(null);
        when(bba.heapBuffer()).thenReturn(null);
        when(bba.heapBuffer(anyInt())).thenReturn(null);
        when(bba.heapBuffer(anyInt(), anyInt())).thenReturn(null);
        when(bba.ioBuffer()).thenReturn(null);
        when(bba.ioBuffer(anyInt())).thenReturn(null);
        when(bba.ioBuffer(anyInt(), anyInt())).thenReturn(null);

        return bba;
    }

    // ===================== FILE CHANNELS ===================== //

    private static FileChannel createFileChannel(OpenOption... options) throws IOException {
        Path path = Paths.get(BC_TEST_FILE);
        if (Files.exists(path)) Files.delete(path);
        Files.createFile(path);
        Files.write(path, BC_FC_CONTENT.getBytes());
        FileChannel fc = FileChannel.open(path, options);
        fc.position(BC_FC_CONTENT.length());
        return fc;
    }

    public static FileChannel validFileChannel() throws IOException {
        return createFileChannel(StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    public static FileChannel closedFileChannel() throws IOException {
        FileChannel fc = validFileChannel();
        fc.close();
        return fc;
    }

    public static FileChannel readOnlyFileChannel() throws IOException {
        return createFileChannel(StandardOpenOption.READ);
    }

    public static FileChannel writeOnlyFileChannel() throws IOException {
        return createFileChannel(StandardOpenOption.WRITE);
    }

    public static FileChannel invalidPositionFileChannel() throws IOException {
        FileChannel fc = spy(validFileChannel());
        when(fc.position()).thenReturn(-1L);
        return fc;
    }

    public static FileChannel spiedFileChannel() throws IOException {
        return spy(validFileChannel());
    }

    // ===================== BYTEBUFFERS ===================== //

    public static ByteBuf emptyByteBuf() {
        return Unpooled.buffer(BC_FC_CONTENT.length() + BB_CONTENT.length() + 1, BC_FC_CONTENT.length() + BB_CONTENT.length() + 1);
    }

    public static ByteBuf fullByteBuf() {
        ByteBuf buffer = Unpooled.buffer(BC_FC_CONTENT.length() + BB_CONTENT.length(), BC_FC_CONTENT.length() + BB_CONTENT.length());
        buffer.writeBytes(BC_FC_CONTENT.getBytes());
        buffer.writeBytes(BB_CONTENT.getBytes());
        return buffer;
    }

    public static ByteBuf semiFullByteBuf() {
        ByteBuf buffer = Unpooled.buffer(BC_FC_CONTENT.length() + BB_CONTENT.length(), BC_FC_CONTENT.length() + BB_CONTENT.length() );
        buffer.writeBytes(BB_CONTENT.getBytes());
        return buffer;
    }

    public static ByteBuf byteBufWithLength(int len) {
        ByteBuf buffer = Unpooled.buffer(len, len);
        byte[] data = new byte[len];
        Arrays.fill(data, (byte) 'a');
        buffer.writeBytes(data);
        return buffer;
    }

    public static ByteBuf emptyByteBufWithLength(int len) {
        return Unpooled.buffer(len, len);
    }

    public static ByteBuf invalidReadIndexByteBuf() {
        ByteBuf buffer = spy(byteBufWithLength(BB_CONTENT.length()));
        when(buffer.readerIndex()).thenReturn(-1);
        return buffer;
    }

    public static ByteBuf invalidWriteIndexByteBuf() {
        ByteBuf buffer = spy(semiFullByteBuf());
        int wIdx = BB_CONTENT.length();
        when(buffer.writerIndex()).thenReturn(wIdx);
        when(buffer.readerIndex()).thenReturn(wIdx + 1);
        return buffer;
    }

    public static ByteBuf deallocatedByteBuf() {
        ByteBuf buffer = byteBufWithLength(BC_FC_CONTENT.length() + BB_CONTENT.length());
        buffer.release(); // refCnt = 0
        return buffer;
    }

    public static ByteBuf byteBufWithContent() {
        byte[] data = BB_CONTENT.getBytes();
        ByteBuf buffer = Unpooled.buffer(data.length, data.length);
        buffer.writeBytes(data);
        return buffer;
    }

    // ===================== PULIZIA ===================== //

    public static void deleteFile() throws IOException {
        Path path = Paths.get(BC_TEST_FILE);
        if (Files.exists(path)) {
            Files.delete(path);
        }
    }

}
