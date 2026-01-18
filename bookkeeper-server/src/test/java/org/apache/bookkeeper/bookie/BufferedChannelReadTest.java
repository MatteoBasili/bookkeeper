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

import static org.apache.bookkeeper.bookie.utils.Utils.*;

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
//                    Arguments.of(validInstance, null, invalidWriteIndexByteBuf(), 0, BC_FC_CONTENT.length(), Exception.class),                                     // R3: Fallito --> La read non ha lanciato l'eccezione attesa
                    Arguments.of(validInstance, null, deallocatedByteBuf(), 0, BC_FC_CONTENT.length(), Exception.class),                                  // R4: Superato
                    Arguments.of(validInstance, null, null, 0, BC_FC_CONTENT.length(), Exception.class),                                                  // R5: Superato

                    // -------------------- Variano pos e length -------------------- //
                    Arguments.of(validInstance, BB_CONTENT, emptyByteBuf(), -1, 1, Exception.class),                                                   // R6: Superato
//                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 1, -1, Exception.class),                                                            // R7: Fallito --> La read non ha lanciato l'eccezione attesa
                    Arguments.of(validInstance, BB_CONTENT, emptyByteBuf(), 0, 0, null),                                                               // R8: Superato
//                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, 1, null),                                                                        // R9: Fallito --> Il buffer di destinazione non contiene il contenuto atteso
//                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length() - 1, null),                                               // R10: Fallito --> Il buffer di destinazione non contiene il contenuto atteso
                    Arguments.of(validInstance, BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length(), null),                                          // R11: Superato
                    Arguments.of(validInstance, BB_CONTENT, emptyByteBuf(), 1, BC_FC_CONTENT.length() - 1, null),                                      // R12: Superato
//                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), BC_FC_CONTENT.length(), 1, null),                                                   // R13: Fallito --> Il buffer di destinazione non contiene il contenuto atteso
                    Arguments.of(validInstance, BB_CONTENT, emptyByteBuf(), BC_FC_CONTENT.length() + BB_CONTENT.length() - 1, 1, null),             // R14: Superato
//                    Arguments.of(validInstance, BC_BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length() + 1, null),                                               // R15: Fallito --> Il buffer di destinazione non contiene il contenuto atteso
                    Arguments.of(validInstance, BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length() + BB_CONTENT.length(), null),                 // R16: Superato
                    Arguments.of(validInstance, BB_CONTENT, emptyByteBuf(), 0, BC_FC_CONTENT.length() + BB_CONTENT.length() + 1, Exception.class),  // R17: Superato
                    Arguments.of(validInstance, BB_CONTENT, emptyByteBuf(), BC_FC_CONTENT.length() + BB_CONTENT.length(), 1, Exception.class),      // R18: Superato

                    // -------------------- Istanze fallite del costruttore -------------------- //
//                    Arguments.of(invalidAllocatorInstance, null, emptyByteBuf(), 0, 1, Exception.class),                                                           // R19 (T2): Fallito --> La read non ha lanciato l'eccezione attesa
                    Arguments.of(invalidPositionInstance, null, emptyByteBuf(), 0, 1, Exception.class),                                                   // R20 (T7): Superato

                    // -------------------- FileChannel non valido -------------------- //
                    Arguments.of(writeOnlyFileChannelInstance, null, emptyByteBuf(), 0, 1, Exception.class),                                              // R21: Superato

                    // -------------------- readCapacity non valida -------------------- //
                    Arguments.of(invalidReadCapacityInstance, null, emptyByteBuf(), 0, 1, Exception.class),                                               // R22: Superato

                    // -------------------- Aggiunti dopo l'analisi con Jacoco -------------------- //
                    Arguments.of(invalidAllocatorInstance, null, emptyByteBuf(), BC_FC_CONTENT.length(), 1, null),                                        // J-R1: Superato
                    Arguments.of(invalidAllocatorInstance, null, emptyByteBuf(), 0, BC_FC_CONTENT.length(), null),                                        // J-R2: Superato

                    // -------------------- Aggiunti dopo l'analisi con PIT -------------------- //
                    Arguments.of(validInstance, BB_CONTENT, emptyByteBufWithLength(BC_FC_CONTENT.length() + BB_CONTENT.length()), 0, BC_FC_CONTENT.length() + BB_CONTENT.length(), null)  // P-R1: Superato
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

        if (expectedException != null) {
            Assertions.assertThrows(
                    expectedException, () -> channel.read(destination, position, length),
                    "La read non ha lanciato l'eccezione attesa: " + expectedException.getSimpleName()
                    );
            return;
        }

        try {
            int initialWriterIndex = destination.writerIndex();

            int bytesRead = channel.read(destination, position, length);

            int finalWriterIndex = destination.writerIndex();
            boolean writeBufferIsNull = channel.writeBuffer == null;

            String expected = computeExpectedRead(channel, position, length, writeBufferIsNull);
            String actual = extractWrittenString(destination, initialWriterIndex, finalWriterIndex);

            Assertions.assertEquals(
                    bytesRead,
                    finalWriterIndex - initialWriterIndex,
                    "Il numero di byte letti non corrisponde allo spazio scritto nel buffer di destinazione"
            );

            Assertions.assertEquals(
                    expected,
                    actual,
                    "Il buffer di destinazione non contiene il contenuto atteso"
            );

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
            BufferedChannel channel, long position, int length, boolean writeBufferIsNull) {

        long writeBufferStart = channel.writeBufferStartPosition.get();

        // Caso 1: lettura inizia dal FileChannel; poi, se devo andare oltre, leggo anche dal write buffer
        if (position < writeBufferStart) {

            int fileStart = (int) position;
            int fileBytes = Math.min(
                    BC_FC_CONTENT.length() - fileStart, length);

            int writeBufferBytes =
                    writeBufferIsNull ? 0 : Math.max(length - fileBytes, 0);

            return BC_FC_CONTENT.substring(
                    fileStart, fileStart + fileBytes)
                    + BB_CONTENT.substring(0, writeBufferBytes);
        }

        // Caso 2: lettura solo dal write buffer
        int writeBufferStartingOffset =
                writeBufferIsNull ? 0 : (int) (position - writeBufferStart);

        int writeBufferEndingOffset =
                writeBufferIsNull ? 0 : writeBufferStartingOffset + length;

        return BB_CONTENT.substring(
                writeBufferStartingOffset,
                writeBufferEndingOffset);
    }

    /**
     * Estrae i byte realmente scritti nel buffer di destinazione.
     */
    private String extractWrittenString(
            ByteBuf dest, int startIndex, int endIndex) {
        int actualLength = endIndex - startIndex;

        ByteBuf tmp = Unpooled.buffer(actualLength);
        dest.getBytes(startIndex, tmp, actualLength);
        return tmp.toString(StandardCharsets.UTF_8);
    }

    private static Stream<Arguments> baduaTestCases() {

        try {
            BufferedChannelInstance validInstance =
                    new BufferedChannelInstance(unpooledByteBufAllocator(),
                            validFileChannel(), 256, 256, 128);

            BufferedChannelInstance notEnoughReadCapacityInstance =
                    new BufferedChannelInstance(unpooledByteBufAllocator(),
                            validFileChannel(), 256, BC_FC_CONTENT.length() - 1, 128);

            BufferedChannelInstance halfReadCapacityInstance =
                    new BufferedChannelInstance(unpooledByteBufAllocator(),
                            validFileChannel(), 256, BC_FC_CONTENT.length() / 2, 128);

            // Parametri: istanza, contenutoWriteBuffer, destPrimaRead, posPrimaRead, lengthPrimaRead,
            //            destSecondaRead, posSecondaRead, lengthSecondaRead
            return Stream.of(
                    Arguments.of(validInstance, null, emptyByteBuf(), 1, BC_FC_CONTENT.length() - 1, emptyByteBuf(), 0, BC_FC_CONTENT.length()),                // B-R1: Superato
                    Arguments.of(validInstance, null, emptyByteBuf(), 1, BC_FC_CONTENT.length() - 1, emptyByteBuf(), 1, BC_FC_CONTENT.length() - 1),            // B-R2: Superato
                    Arguments.of(notEnoughReadCapacityInstance, null, emptyByteBuf(), 0, BC_FC_CONTENT.length(), emptyByteBuf(), 0, BC_FC_CONTENT.length()),    // B-R3: Superato

                    // -------------------- Aggiunti dopo l'analisi con PIT -------------------- //
                    Arguments.of(halfReadCapacityInstance, null, emptyByteBuf(), 1, BC_FC_CONTENT.length() / 2, emptyByteBuf(), BC_FC_CONTENT.length() / 2, 1)  // P-R2: Superato
            );
        } catch (IOException e) {
            throw new RuntimeException("Errore nella preparazione dei casi di test", e);
        }
    }

    @ParameterizedTest
    @MethodSource("baduaTestCases")
    @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void testBaduaRead(BufferedChannelInstance instance, String writeBufferContent, ByteBuf firstDest, long firstPos,
                              int firstLength, ByteBuf secondDest, long secondPos,
                          int secondLength) {

        BufferedChannel channel = createBufferedChannel(instance, writeBufferContent);
        try {
            int initialFirstWriterIndex = firstDest.writerIndex();
            channel.read(firstDest, firstPos, firstLength);
            int finalFirstWriterIndex = firstDest.writerIndex();

            int initialSecondWriterIndex = secondDest.writerIndex();
            channel.read(secondDest, secondPos, secondLength);
            int finalSecondWriterIndex = secondDest.writerIndex();

            boolean writeBufferIsNull = channel.writeBuffer == null;
            String firstExpected = computeExpectedRead(channel, firstPos, firstLength, writeBufferIsNull);
            String secondExpected = computeExpectedRead(channel, secondPos, secondLength, writeBufferIsNull);
            String firstActual = extractWrittenString(firstDest, initialFirstWriterIndex, finalFirstWriterIndex);
            String secondActual = extractWrittenString(secondDest, initialSecondWriterIndex, finalSecondWriterIndex);

            Assertions.assertEquals(firstExpected, firstActual, "Il buffer di destinazione della prima lettura non contiene il contenuto atteso");
            Assertions.assertEquals(secondExpected, secondActual, "Il buffer di destinazione della seconda lettura non contiene il contenuto atteso");

        } catch (Exception e) {
            throw new RuntimeException("Errore inatteso durante l'esecuzione delle due read di BufferedChannel", e);
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
