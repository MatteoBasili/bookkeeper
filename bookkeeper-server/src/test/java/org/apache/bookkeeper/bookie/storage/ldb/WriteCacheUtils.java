package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteCacheUtils {

    private WriteCacheUtils() {
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
}
