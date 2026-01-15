package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.exceptions.base.MockitoAssertionError;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.BufferedChannelUtils.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Test unitari per il metodo write di {@link BufferedChannel}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelWriteTest {

    /**
     * Genera i casi di test parametrizzati.
     * Ogni Arguments contiene:
     * - ByteBufAllocator (Netty)
     * - FileChannel
     * - writeCapacity
     * - readCapacity
     * - unpersistedBytesBound
     * - src
     * - Classe di eccezione attesa (null se non ci si aspetta eccezione)
     */
    private static Stream<Arguments> testCases() {
        try {
            return Stream.of(
                    // -------------------- Varia src -------------------- //
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 128, emptyByteBuf(), null),                               // W1: Superato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 128, byteBufWithLength(127), null),                       // W2: Superato
//                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 128, byteBufWithLength(128), null),                       // W3: Fallito --> Buffer scritto, ma unpersistedBytes non coerente
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 128, byteBufWithLength(129), null),                       // W4: Superato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 128, invalidReadIndexByteBuf(), Exception.class),         // W5: Superato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 128, deallocatedByteBuf(), Exception.class),              // W6: Superato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 128, null, Exception.class),                                 // W7: Superato

                    // -------------------- Istanze fallite del costruttore -------------------- //
                    Arguments.of(invalidByteBufAllocator(), validFileChannel(), 256, 256, 128, byteBufWithContent(), Exception.class),               // W8: Superato
//                    Arguments.of(unpooledByteBufAllocator(), invalidPositionFileChannel(), 256, 256, 128, byteBufWithLength(129), Exception.class),           // W9 (T7): Fallito --> La write non ha lanciato l'eccezione attesa
//                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, -1, byteBufWithContent(), Exception.class),                           // W10 (T15): Fallito --> La write non ha lanciato l'eccezione attesa

                    // -------------------- FileChannel non valido -------------------- //
                    Arguments.of(unpooledByteBufAllocator(), readOnlyFileChannel(), 256, 256, 128, byteBufWithLength(129), Exception.class),            // W11: Superato

                    // -------------------- writeCapacity non valida -------------------- //
//                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 0, 256, 128, byteBufWithContent(), Exception.class),                            // W12: Errore --> Timeout

                    // -------------------- unpersistedBytesBound nullo -------------------- //
//                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 0, byteBufWithContent(), null)                                        // W13: Fallito --> I byte non vengono scritti sul FileChannel

                    // -------------------- Aggiunti dopo l'analisi con Jacoco (BC_BB_CONTENT di lunghezza pari) -------------------- //
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), BC_BB_CONTENT.length() / 2, 256, 0, byteBufWithContent(), null),      // J-W1: Superato
//                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), (BC_BB_CONTENT.length() / 2) + 1, 256, 0, byteBufWithContent(), null)          // J-W2: Fallito --> I byte non vengono scritti sul FileChannel

                    // -------------------- Aggiunti dopo l'analisi con PIT (BC_BB_CONTENT.length() > 4) -------------------- //
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), BC_BB_CONTENT.length() - 2, 256, 1, byteBufWithContent(), null),      // P-W1: Superato
//                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), BC_BB_CONTENT.length() - 2, 256, 3, byteBufWithContent(), null)               // P-W2: Fallito --> Il contenuto scritto nel file channel è diverso da quello aspettato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, BC_BB_CONTENT.length(), byteBufWithContent(), null),         // P-W3: Superato
                    Arguments.of(unpooledByteBufAllocator(), spiedFileChannel(), 256, 256, 128, byteBufWithLength(200), null),                         // P-W4: Superato
                    Arguments.of(unpooledByteBufAllocator(), spiedFileChannel(), 256, 256, 128, byteBufWithLength(100), null)                         // P-W5: Superato
            );
        } catch (IOException e) {
            throw new RuntimeException("Errore nella preparazione dei casi di test", e);
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void testWrite(ByteBufAllocator allocator,
                   FileChannel fc,
                   int writeCapacity,
                   int readCapacity,
                   long unpersistedBytesBound,
                   ByteBuf src,
                   Class<Exception> expectedException) {

        BufferedChannel bc;
        long initialFcPosition;

        try {
            bc = new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
            Assertions.assertNotNull(bc, "BufferedChannel non dovrebbe essere null");
            initialFcPosition = fc.position();
        } catch (Exception e) {
            throw new RuntimeException("Errore inatteso nella creazione di BufferedChannel", e);
        }

        if (expectedException != null) {
            Assertions.assertThrows(
                    expectedException, () -> bc.write(src),
                    "La write non ha lanciato l'eccezione attesa: " + expectedException.getSimpleName()
            );
            return;
        }

        try {
            long expectedPosition, expectedUnpersistedBytes, expectedWriteBufferStartPosition;

            // Converte il contenuto sorgente in stringa e ne calcola la lunghezza
            String expectedWrittenContent = src.toString(StandardCharsets.UTF_8);
            int expectedWrittenContentLength = expectedWrittenContent.length();

            // Esegui la scrittura
            bc.write(src);

            // -------------------- Controlla il contenuto scritto (aggiornato dopo PIT) -------------------- //
            int expectedFcWrittenBytesLength;
            int expectedWbWrittenBytesLength;
            String expectedFcWrittenContent;
            String expectedWbWrittenContent;
            String actualFcWrittenContent;
            String actualWbWrittenContent;

            if (unpersistedBytesBound < 1) {  // CASO 1: tutto è flushato nel file channel
                expectedFcWrittenContent = expectedWrittenContent;
                expectedWbWrittenContent = "";

            } else if (expectedWrittenContentLength <= writeCapacity) {
                if (expectedWrittenContentLength < unpersistedBytesBound) {  // CASO 2: tutto rimane scritto nel buffer
                    expectedFcWrittenContent = "";
                    expectedWbWrittenContent = expectedWrittenContent;

                    // Verifica forceWrite non lanciata (solo se spy)
                    if (shouldVerifyForceWrite(fc)) {
                        verifyNotForceWrite(fc);
                    }

                } else {  // CASO 3: tutto è flushato nel file channel
                    expectedFcWrittenContent = expectedWrittenContent;
                    expectedWbWrittenContent = "";

                    // Verifica forceWrite (solo se spy)
                    if (shouldVerifyForceWrite(fc)) {
                        verifyForceWrite(fc);
                    }
                }
            } else {
                int lastBytesWrittenOnWbLength = expectedWrittenContentLength % writeCapacity;
                if (lastBytesWrittenOnWbLength == 0) {  // CASO 4: tutto è flushato nel file channel
                    expectedFcWrittenContent = expectedWrittenContent;
                    expectedWbWrittenContent = "";

                } else {
                    int bytesWrittenOnFcLength = expectedWrittenContentLength - lastBytesWrittenOnWbLength;
                    if (lastBytesWrittenOnWbLength >= unpersistedBytesBound) {  // CASO 5: tutto è flushato nel file channel
                        expectedFcWrittenContent = expectedWrittenContent;
                        expectedWbWrittenContent = "";

                        // Verifica forceWrite (solo se spy)
                        if (shouldVerifyForceWrite(fc)) {
                            verifyForceWrite(fc);
                        }

                    } else {  // CASO 6: l'ultima parte è nel buffer, mentre la restante è flushata nel file channel
                        expectedFcWrittenContent = expectedWrittenContent.substring(0, bytesWrittenOnFcLength);
                        expectedWbWrittenContent = expectedWrittenContent.substring(bytesWrittenOnFcLength, expectedWrittenContentLength);

                        // Verifica forceWrite non lanciata (solo se spy)
                        if (shouldVerifyForceWrite(fc)) {
                            verifyNotForceWrite(fc);
                        }
                    }
                }
            }

            expectedFcWrittenBytesLength = expectedFcWrittenContent.length();
            expectedWbWrittenBytesLength = expectedWbWrittenContent.length();

            ByteBuffer bb = ByteBuffer.allocate(expectedWrittenContentLength);
            fc.read(bb, initialFcPosition);
            bb.flip();
            actualFcWrittenContent = new String(bb.array(), 0, bb.limit());

            ByteBuf actualWrittenBuffer = Unpooled.buffer(expectedWrittenContentLength);
            bc.writeBuffer.getBytes(0, actualWrittenBuffer, expectedWbWrittenBytesLength);
            actualWbWrittenContent = actualWrittenBuffer.toString(StandardCharsets.UTF_8);

            Assertions.assertEquals(expectedFcWrittenContent, actualFcWrittenContent, "Il contenuto scritto nel file channel è diverso da quello aspettato");
            Assertions.assertEquals(expectedWbWrittenContent, actualWbWrittenContent, "Il contenuto scritto nel write buffer è diverso da quello aspettato");

            // =====================================  Controlla i campi della classe ====================================== //
            expectedPosition                 = initialFcPosition + expectedWrittenContentLength;
            expectedUnpersistedBytes         = expectedWbWrittenBytesLength;
            expectedWriteBufferStartPosition = initialFcPosition + expectedFcWrittenBytesLength;

            Assertions.assertEquals(expectedPosition,                   bc.position, "Posizione non corretta");
            Assertions.assertEquals(expectedUnpersistedBytes,           bc.unpersistedBytes.get(), "Unpersisted Bytes non corretti");
            Assertions.assertEquals(expectedWriteBufferStartPosition,   bc.writeBufferStartPosition.get(), "Posizione iniziale del write Buffer non corretta");

        } catch (Exception e) {
            throw new RuntimeException("Errore inatteso durante l'esecuzione della write di BufferedChannel", e);
        }
    }

    private boolean shouldVerifyForceWrite(FileChannel fc) {
        return org.mockito.Mockito.mockingDetails(fc).isSpy();
    }

    private void verifyForceWrite(FileChannel fc) throws IOException {
        try {
            verify(fc).force(false);
        } catch (MockitoAssertionError e) {
            throw new AssertionError("forceWrite non è stato chiamato come previsto", e);
        }
    }

    private void verifyNotForceWrite(FileChannel fc) throws IOException {
        try {
            verify(fc, never()).force(false);
        } catch (MockitoAssertionError e) {
            throw new AssertionError("forceWrite è stato chiamato quando invece non era previsto", e);
        }
    }

    // ============================ PULIZIA DOPO OGNI TEST ============================ //

    /**
     * Elimina il file di test dopo ogni esecuzione per evitare interferenze.
     */
    @AfterEach
    void cleanup() throws IOException {
        deleteFile();
    }

}
