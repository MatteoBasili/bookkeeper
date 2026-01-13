package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.BufferedChannelUtils.*;

/**
 * Test unitari per il costruttore di {@link BufferedChannel}.
 * Questi test verificano sia la gestione corretta dei parametri validi,
 * sia il comportamento in caso di parametri non validi.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BufferedChannelConstructorTest {

    /**
     * Genera i casi di test parametrizzati.
     * Ogni Arguments contiene:
     * - ByteBufAllocator (Netty)
     * - FileChannel
     * - writeCapacity
     * - readCapacity
     * - unpersistedBytesBound
     * - Classe di eccezione attesa (null se non ci si aspetta eccezione)
     */
    private static Stream<Arguments> testCases() {
        try {
            return Stream.of(
                    // -------------------- Varia l'allocatore -------------------- //
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 128, null),              // T1: Superato
                    Arguments.of(invalidByteBufAllocator(), validFileChannel(), 256, 256, 128, Exception.class),             // T2: Fallito --> Il costruttore non ha lanciato l'eccezione attesa
                    Arguments.of(null, validFileChannel(), 256, 256, 128, Exception.class),                         // T3: Superato

                    // -------------------- Varia il file channel -------------------- //
                    Arguments.of(unpooledByteBufAllocator(), closedFileChannel(), 256, 256, 128, Exception.class),  // T4: Superato
                    Arguments.of(unpooledByteBufAllocator(), readOnlyFileChannel(), 256, 256, 128, null),           // T5: Superato
                    Arguments.of(unpooledByteBufAllocator(), writeOnlyFileChannel(), 256, 256, 128, null),          // T6: Superato
                    Arguments.of(unpooledByteBufAllocator(), invalidPositionFileChannel(), 256, 256, 128, Exception.class),  // T7: Fallito --> Il costruttore non ha lanciato l'eccezione attesa
                    Arguments.of(unpooledByteBufAllocator(), null, 256, 256, 128, Exception.class),                 // T8: Superato

                    // -------------------- Varia writeCapacity -------------------- //
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), -1, 256, 128, Exception.class),    // T9: Superato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 0, 256, 128, null),                // T10: Superato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 1, 256, 128, null),                // T11: Superato

                    // -------------------- Varia readCapacity -------------------- //
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, -1, 128, Exception.class),    // T12: Superato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 0, 128, null),                // T13: Superato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 1, 128, null),                // T14: Superato

                    // -------------------- Varia unpersistedBytesBound -------------------- //
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, -1, Exception.class),             // T15: Fallito --> Il costruttore non ha lanciato l'eccezione attesa
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 0, null),                // T16: Superato
                    Arguments.of(unpooledByteBufAllocator(), validFileChannel(), 256, 256, 1, null)                 // T17: Superato
            );
        } catch (IOException e) {
            throw new RuntimeException("Errore nella preparazione dei casi di test", e);
        }
    }

    /**
     * Test parametrizzato del costruttore.
     * Controlla sia i casi validi che quelli che devono generare eccezioni.
     */
    @ParameterizedTest
    @MethodSource("testCases")
    @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void testConstructor(ByteBufAllocator allocator,
                         FileChannel fc,
                         int writeCapacity,
                         int readCapacity,
                         long unpersistedBytesBound,
                         Class<Exception> expectedException) {

        if (expectedException != null) {
            // Caso in cui ci si aspetta un'eccezione
            assertConstructorFails(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound, expectedException);
        } else {
            // Caso in cui il costruttore dovrebbe riuscire
            assertConstructorSucceeds(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
        }

    }

    // ============================ METODI DI SUPPORTO ============================ //

    /**
     * Verifica che il costruttore lanci l'eccezione prevista.
     */
    private void assertConstructorFails(ByteBufAllocator allocator,
                                        FileChannel fc,
                                        int writeCapacity,
                                        int readCapacity,
                                        long unpersistedBytesBound,
                                        Class<Exception> expectedException) {

        Assertions.assertThrows(
                expectedException,
                () -> new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound),
                "Il costruttore non ha lanciato l'eccezione attesa: " + expectedException.getSimpleName()
        );
    }

    /**
     * Verifica che il costruttore crei correttamente l'oggetto
     * e che tutti i campi interni siano coerenti.
     */
    private void assertConstructorSucceeds(ByteBufAllocator allocator,
                                           FileChannel fc,
                                           int writeCapacity,
                                           int readCapacity,
                                           long unpersistedBytesBound) {

        try {
            BufferedChannel bc = new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
            Assertions.assertNotNull(bc, "BufferedChannel non dovrebbe essere null");

            // -------------------- Controlli sui campi della classe -------------------- //
            Assertions.assertEquals(fc.position(), bc.position, "Position non corretta");
            Assertions.assertEquals(0, bc.unpersistedBytes.get(), "Unpersisted bytes non inizializzato correttamente");
            Assertions.assertEquals(unpersistedBytesBound, bc.unpersistedBytesBound, "Unpersisted bytes bound non corretto");

            // Non conoscendo il buffer interno esatto utilizzato,
            // controlliamo solo che l'allocator impiegato sia quello previsto
            Assertions.assertSame(allocator, bc.writeBuffer.alloc(), "Allocator della writeBuffer non corrisponde");

            Assertions.assertEquals(fc.position(), bc.writeBufferStartPosition.get(), "Write buffer start position non corretta");
            Assertions.assertEquals(writeCapacity, bc.writeCapacity, "Write capacity non corretta");

            // -------------------- Controlli sui campi della superclasse BufferedReadChannel -------------------- //
            Assertions.assertEquals(0, bc.cacheHitCount, "Cache hit count iniziale non corretto");
            Assertions.assertEquals(0, bc.invocationCount, "Invocation count iniziale non corretto");

            // La superclasse non riceve alcun allocator nel costruttore,
            // quindi non possiamo controllare quale allocator usa il readBuffer.
            // Perciò, ci limitiamo solo a verificare che il readBuffer esista e che
            // la sua posizione non superi quella del file channel.
            Assertions.assertNotNull(bc.readBuffer, "Read buffer non dovrebbe essere null");
            Assertions.assertTrue(
                    bc.readBufferStartPosition <= fc.position(),
                    "La posizione iniziale del readBuffer non può superare la posizione del file channel"
            );


            Assertions.assertEquals(readCapacity, bc.readCapacity, "Read capacity non corretta");
            Assertions.assertFalse(bc.sealed, "BufferedChannel non dovrebbe essere sealed all'inizio");

            // -------------------- Controlli sui campi della superclasse BufferedChannelBase -------------------- //
            Assertions.assertEquals(fc, bc.fileChannel, "FileChannel interno non corrisponde");

        } catch (Exception e) {
            throw new RuntimeException("Errore inatteso nella costruzione di BufferedChannel", e);
        }
    }

    // ============================ PULIZIA DOPO OGNI TEST ============================ //

    /**
     * Elimina il file di test dopo ogni esecuzione per evitare interferenze.
     */
    @AfterEach
    void deleteTestFile() throws IOException {
        deleteFile();
    }

}
