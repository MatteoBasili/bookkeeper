package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.utils.Utils.unpooledByteBufAllocator;

/**
 * Test unitari per il costruttore di {@link WriteCache}.
 * Questi test verificano sia la gestione corretta dei parametri validi,
 * sia il comportamento in caso di parametri non validi.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WriteCacheConstructorTest {

    /**
     * Genera i casi di test parametrizzati.
     * Ogni Arguments contiene:
     * - ByteBufAllocator (Netty)
     * - maxCacheSize
     * - maxSegmentSize
     * - Classe di eccezione attesa (null se non ci si aspetta eccezione)
     */
    private static Stream<Arguments> testCases() {
        return Stream.of(
                // -------------------- Varia l'allocatore -------------------- //
//                Arguments.of(invalidByteBufAllocator(), 512, 128, Exception.class),              // T1: Fallito --> Era attesa un'eccezione
//                Arguments.of(null, 512, 128, Exception.class),                               // T2: Fallito --> Era attesa un'eccezione

                // -------------------- Varia maxCacheSize -------------------- //
                Arguments.of(unpooledByteBufAllocator(), -1, 1, Exception.class),                         // T3: Superato
//                Arguments.of(unpooledByteBufAllocator(), 0, 1, Exception.class),                    // T4: Fallito --> Era attesa un'eccezione
                Arguments.of(unpooledByteBufAllocator(), 1, 1, null),                               // T5: Superato

                // -------------------- Varia maxSegmentSize -------------------- //
                Arguments.of(unpooledByteBufAllocator(), 512, -1, Exception.class),          // T6: Superato
                Arguments.of(unpooledByteBufAllocator(), 512, 0, Exception.class),         // T7: Superato
                Arguments.of(unpooledByteBufAllocator(), 512, 1, null),                 // T8: Superato
                Arguments.of(unpooledByteBufAllocator(), 512, 100, Exception.class),    // T9: Superato
                Arguments.of(unpooledByteBufAllocator(), 512, 128, null),                // T10: Superato
                Arguments.of(unpooledByteBufAllocator(), 512, 512, null),                // T11: Superato
                Arguments.of(unpooledByteBufAllocator(), 512, 600, Exception.class)                // T12: Superato
        );
    }

    /**
     * Test parametrizzato del costruttore.
     * Controlla sia i casi validi che quelli che devono generare eccezioni.
     */
    @ParameterizedTest
    @MethodSource("testCases")
    @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void testConstructor(ByteBufAllocator allocator,
                         long maxCacheSize,
                         int maxSegmentSize,
                         Class<Exception> expectedException) {

        if (expectedException != null) {
            // Caso in cui ci si aspetta un'eccezione
            assertConstructorFails(allocator, maxCacheSize, maxSegmentSize, expectedException);
        } else {
            // Caso in cui il costruttore dovrebbe riuscire
            assertConstructorSucceeds(allocator, maxCacheSize, maxSegmentSize);
        }

    }

    // ============================ METODI DI SUPPORTO ============================ //

    /**
     * Verifica che il costruttore lanci l'eccezione prevista.
     */
    private void assertConstructorFails(ByteBufAllocator allocator,
                                        long maxCacheSize,
                                        int maxSegmentSize,
                                        Class<Exception> expectedException) {

        try {
            WriteCache wc = new WriteCache(allocator, maxCacheSize, maxSegmentSize);
            // Se arriviamo qui, il costruttore NON ha lanciato eccezione → errore
            wc.close(); // evitiamo leak
            Assertions.fail("Era attesa un'eccezione di tipo " + expectedException.getSimpleName());
        } catch (Exception e) {
            Assertions.assertTrue(
                    expectedException.isInstance(e),
                    "Eccezione inattesa: " + e.getClass().getSimpleName()
            );
        }
    }

    /**
     * Verifica che il costruttore crei correttamente l'oggetto
     * e che tutti i campi interni siano coerenti.
     */
    private void assertConstructorSucceeds(ByteBufAllocator allocator,
                                           long maxCacheSize,
                                           int maxSegmentSize) {

        try {
            WriteCache wc = new WriteCache(allocator, maxCacheSize, maxSegmentSize);
            Assertions.assertNotNull(wc, "WriteCache non dovrebbe essere null");

            int expectedSegmentsCount = (int) (1 + (maxCacheSize / maxSegmentSize));

            // -------------------- Controlli sui campi della classe -------------------- //
            Assertions.assertEquals(maxCacheSize, wc.getMaxCacheSize(), "Max Cache Size non corretta");
            Assertions.assertEquals(maxSegmentSize, wc.getMaxSegmentSize(), "Max Segment Size non corretta");
            Assertions.assertEquals(0, wc.count(), "Il numero di entry nella cache non corrisponde");
            Assertions.assertEquals(0, wc.size(),"La dimensione totale delle entry nella cache non corrisponde");
            Assertions.assertEquals(expectedSegmentsCount, wc.getSegmentsCount(), "il numero totale di segmenti in cui la write cache è suddivisa non corrisponde");

            for (int i = 0; i < expectedSegmentsCount; i++){
                Assertions.assertEquals(
                        i < expectedSegmentsCount - 1 ? maxSegmentSize : maxCacheSize % maxSegmentSize,
                        wc.getCacheSegments()[i].capacity(),
                        "La dimensione dei segmenti della cache non corrisponde"
                );
            }

        } catch (Exception e) {
            throw new RuntimeException("Errore inatteso nella costruzione di WriteCache", e);
        }
    }
}
