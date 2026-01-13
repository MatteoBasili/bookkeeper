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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.BufferedChannelUtils.*;

/**
 * Test unitari per il metodo read di {@link BufferedChannel}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelReadTest {

    /**
     * Genera i casi di test parametrizzati.
     * Ogni Arguments contiene:
     * - Istanza del costruttore
     * - Contenuto del writeBuffer
     * - dest
     * - pos
     * - length
     * - Classe di eccezione attesa (null se non ci si aspetta eccezione)
     */
    private static Stream<Arguments> testCases() {

        try {
            BufferedChannelInstance validInstance =
                    new BufferedChannelInstance(unpooledByteBufAllocator(),
                            validFileChannel(), 256, 256, 128);

            BufferedChannelInstance invalidAllocatorInstance =
                    new BufferedChannelInstance(invalidByteBufAllocator(),
                            validFileChannel(), 256, 256, 128);

            BufferedChannelInstance invalidPositionInstance =
                    new BufferedChannelInstance(unpooledByteBufAllocator(),
                            invalidPositionFileChannel(), 256, 256, 128);

            BufferedChannelInstance writeOnlyFileChannelInstance =
                    new BufferedChannelInstance(unpooledByteBufAllocator(),
                            writeOnlyFileChannel(), 256, 256, 128);

            BufferedChannelInstance invalidReadCapacityInstance =
                    new BufferedChannelInstance(unpooledByteBufAllocator(),
                            validFileChannel(), 256, 0, 128);

            return Stream.of(
                    // -------------------- Varia dest -------------------- //
                    Arguments.of(validInstance, null, emptyByteBuf(), 0, BC_FC_CONTENT.length(), null),                                                   // R1: Superato
                    Arguments.of(validInstance, null, fullByteBuf(), 0, BC_FC_CONTENT.length(), Exception.class),                                         // R2: Superato
                    Arguments.of(validInstance, null, invalidWriteIndexByteBuf(), 0, BC_FC_CONTENT.length(), Exception.class),                            // R3: Fallito --> La read non ha lanciato l'eccezione attesa
                    Arguments.of(validInstance, null, deallocatedByteBuf(), 0, BC_FC_CONTENT.length(), Exception.class),                                  // R4: Superato
                    Arguments.of(validInstance, null, null, 0, BC_FC_CONTENT.length(), Exception.class),                                                  // R5: Superato

                    // -------------------- Variano pos e length -------------------- //
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), -1, 1, Exception.class),                                                   // R6: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 1, -1, Exception.class),                                                   // R7: Fallito --> La read non ha lanciato l'eccezione attesa
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, 0, null),                                                               // R8: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, 1, null),                                                               // R9: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length() - 1, null),                                      // R10: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length(), null),                                          // R11: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 1, BC_FC_CONTENT.length() - 1, null),                                      // R12: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), BC_FC_CONTENT.length(), 1, null),                                          // R13: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), BC_FC_CONTENT.length() + BC_BB_CONTENT.length() - 1, 1, null),             // R14: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length() + 1, null),                                      // R15: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length() + BC_BB_CONTENT.length(), null),                 // R16: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length() + BC_BB_CONTENT.length() + 1, Exception.class),  // R17: Superato
                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), BC_FC_CONTENT.length() + BC_BB_CONTENT.length(), 1, Exception.class),      // R18: Superato

                    // -------------------- Istanze fallite del costruttore -------------------- //
                    Arguments.of(invalidAllocatorInstance, null, emptyByteBuf(), 0, 1, Exception.class),                                                  // R19 (T2): Fallito --> La read non ha lanciato l'eccezione attesa
                    Arguments.of(invalidPositionInstance, null, emptyByteBuf(), 0, 1, Exception.class),                                                   // R20 (T7): Superato

                    // -------------------- FileChannel non valido -------------------- //
                    Arguments.of(writeOnlyFileChannelInstance, null, emptyByteBuf(), 0, 1, Exception.class),                                              // R21: Superato

                    // -------------------- readCapacity non valida -------------------- //
                    Arguments.of(invalidReadCapacityInstance, null, emptyByteBuf(), 0, 1, Exception.class)                                               // R22: Superato

                    // -------------------- Aggiunti dopo l'analisi con Jacoco -------------------- //

            );

        } catch (IOException e) {
            throw new RuntimeException("Errore nella preparazione dei casi di test", e);
        }
    }

    /**
     * Rappresenta la configurazione di un BufferedChannel.
     */
    private static final class BufferedChannelInstance {

        final ByteBufAllocator allocator;
        final FileChannel fileChannel;
        final int writeCapacity;
        final int readCapacity;
        final long unpersistedBytesBound;

        BufferedChannelInstance(
                ByteBufAllocator allocator,
                FileChannel fileChannel,
                int writeCapacity,
                int readCapacity,
                long unpersistedBytesBound) {

            this.allocator = allocator;
            this.fileChannel = fileChannel;
            this.writeCapacity = writeCapacity;
            this.readCapacity = readCapacity;
            this.unpersistedBytesBound = unpersistedBytesBound;
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void testRead(
            BufferedChannelInstance instance,
            String writeBufferContent,
            ByteBuf destination,
            long position,
            int length,
            Class<Exception> expectedException) {

        BufferedChannel channel = createBufferedChannel(instance, writeBufferContent);

        int initialWriterIndex =
                destination != null ? destination.writerIndex() : 0;

        if (expectedException != null) {
            Assertions.assertThrows(
                    expectedException, () -> channel.read(destination, position, length),
                    "La read non ha lanciato l'eccezione attesa: " + expectedException.getSimpleName()
                    );
            return;
        }

        try {
            channel.read(destination, position, length);

            String expected = computeExpectedRead(channel, position, length);
            String actual = extractWrittenString(destination, initialWriterIndex, length);

            Assertions.assertEquals(expected, actual, "Il buffer di destinazione non contiene il contenuto atteso");
        } catch (Exception e) {
            throw new RuntimeException("Errore inatteso durante l'esecuzione della read di BufferedChannel", e);
        }
    }

    // ============================ METODI DI SUPPORTO ============================ //

    /**
     * Crea e inizializza un BufferedChannel.
     */
    private BufferedChannel createBufferedChannel(
            BufferedChannelInstance instance, String writeBufferContent) {

        try {
            BufferedChannel channel = new BufferedChannel(
                    instance.allocator,
                    instance.fileChannel,
                    instance.writeCapacity,
                    instance.readCapacity,
                    instance.unpersistedBytesBound
            );
            Assertions.assertNotNull(channel, "BufferedChannel non dovrebbe essere null");

            if (writeBufferContent != null) {
                channel.writeBuffer.writeBytes(
                        writeBufferContent.getBytes(StandardCharsets.UTF_8));
            }

            return channel;
        } catch (Exception e) {
            throw new RuntimeException("Errore nella creazione del BufferedChannel", e);
        }
    }

    /**
     * Calcola la stringa che ci si aspetta venga letta dal metodo read.
     */
    private String computeExpectedRead(
            BufferedChannel channel, long position, int length) {

        long writeBufferStart = channel.writeBufferStartPosition.get();

        // Caso 1: lettura inizia dal FileChannel; poi, se devo andare oltre, leggo anche dal write buffer
        if (position < writeBufferStart) {

            int fileStart = (int) position;
            int fileBytes = Math.min(
                    BC_FC_CONTENT.length() - fileStart, length);

            int writeBufferBytes =
                    Math.max(length - fileBytes, 0);

            return BC_FC_CONTENT.substring(
                    fileStart, fileStart + fileBytes)
                    + BC_BB_CONTENT.substring(0, writeBufferBytes);
        }

        // Caso 2: lettura solo dal write buffer
        int writeBufferOffset =
                (int) (position - writeBufferStart);

        return BC_BB_CONTENT.substring(
                writeBufferOffset,
                writeBufferOffset + length);
    }

    /**
     * Estrae i byte realmente scritti nel buffer di destinazione.
     */
    private String extractWrittenString(
            ByteBuf dest, int startIndex, int length) {

        ByteBuf tmp = Unpooled.buffer(length);
        dest.getBytes(startIndex, tmp, length);
        return tmp.toString(StandardCharsets.UTF_8);
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
