package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.bookkeeper.bookie.utils.Utils.*;
import static org.apache.bookkeeper.bookie.utils.Utils.deallocatedByteBuf;
import static org.apache.bookkeeper.bookie.utils.Utils.invalidReadIndexByteBuf;
import static org.apache.bookkeeper.bookie.utils.Utils.unpooledByteBufAllocator;

/**
 * Test unitari per il metodo put di {@link WriteCache}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WriteCachePutTest {

    /**
     * Genera i casi di test parametrizzati.
     * Ogni Arguments contiene:
     * - Istanza del costruttore
     * - ledgerId
     * - entryId
     * - entry
     * - Valore di ritorno atteso
     * - Classe di eccezione attesa (null se non ci si aspetta eccezione)
     */
    private static Stream<Arguments> testCases() {

        WriteCacheInstance validInstance1 = new WriteCacheInstance(unpooledByteBufAllocator(), 64, 32);  // maxSegmentSize >= entryByte
        WriteCacheInstance validInstance2 = new WriteCacheInstance(unpooledByteBufAllocator(), 32, 16);   // maxSegmentSize < entryByte
        WriteCacheInstance zeroMaxCacheSizeInstance = new WriteCacheInstance(unpooledByteBufAllocator(),  0, 1);

        return Stream.of(
                // -------------------- Varia ledgerId -------------------- //
                Arguments.of(validInstance1, -1, 1, fullByteBuf(), Exception.class, false),                      // P1: Superato
                Arguments.of(validInstance1, 0, 1, fullByteBuf(), null, true),                     // P2: Superato

                // -------------------- Varia entryId -------------------- //
                Arguments.of(validInstance1, 1, -1, fullByteBuf(), Exception.class, false),                              // P3: Superato
                Arguments.of(validInstance1, 1, 0, fullByteBuf(), null, true),                     // P4: Superato

                // -------------------- Varia entry -------------------- //
                Arguments.of(validInstance1, 1, 1, emptyByteBuf(), null, true),       // P5: Superato
                Arguments.of(validInstance1, 1, 1, fullByteBuf(), null, true),            // P6: Superato
                Arguments.of(validInstance2, 1, 1, fullByteBuf(), null, false),                            // P7: Superato
                Arguments.of(validInstance1, 1, 1, invalidReadIndexByteBuf(), Exception.class, false),             // P8: Superato
                Arguments.of(validInstance1, 1, 1, deallocatedByteBuf(), Exception.class, false),         // P9: Superato
                Arguments.of(validInstance1, 1, 1, null, Exception.class, false),                      // P10: Superato

                // -------------------- Istanze fallite del costruttore -------------------- //
                Arguments.of(zeroMaxCacheSizeInstance, 1, 1, fullByteBuf(), null, false)       // P11: Superato
        );
    }

    /**
     * Rappresenta la configurazione di una WriteCache.
     */
    private static final class WriteCacheInstance {

        final ByteBufAllocator allocator;
        final long maxCacheSize;
        final int maxSegmentSize;

        WriteCacheInstance(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize) {
            this.allocator = allocator;
            this.maxCacheSize = maxCacheSize;
            this.maxSegmentSize = maxSegmentSize;
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void testPut(
            WriteCacheInstance instance,
            long ledgerId,
            long entryId,
            ByteBuf entry,
            Class<Exception> expectedException,
            boolean expectedReturn) {

        WriteCache wc = new WriteCache(instance.allocator, instance.maxCacheSize, instance.maxSegmentSize);
        Assertions.assertNotNull(wc, "WriteCache non dovrebbe essere null");

        if (expectedException != null) {
            Assertions.assertThrows(
                    expectedException, () -> wc.put(ledgerId, entryId, entry),
                    "La put non ha lanciato l'eccezione attesa: " + expectedException.getSimpleName()
            );
            return;
        }

        try {
            boolean actualReturn = wc.put(ledgerId, entryId, entry);
            Assertions.assertEquals(expectedReturn, actualReturn, "Il valore di ritorno della put non corrisponde");

            long expectedSize = expectedReturn ? entry.readableBytes() : 0;
            long expectedCount = expectedReturn ? 1 : 0;

            Assertions.assertEquals(expectedSize, wc.size(), "La dimensione totale delle entry nella cache non corrisponde");
            Assertions.assertEquals(expectedCount, wc.count(), "Il numero di entry nella cache non corrisponde");

            // Verifica lastEntryMap
            long actualStoredEntryId = wc.getLastEntryMap().get(ledgerId);
            long expectedStoredEntryId = actualReturn ? entryId : -1;
            Assertions.assertEquals(expectedStoredEntryId, actualStoredEntryId, "Il valore del ledgerId scritto non corrisponde");

            // Verifica index solo se la put ha avuto successo
            ConcurrentLongLongPairHashMap.LongPair actualStoredPair = wc.getIndex().get(ledgerId, entryId);
            if (actualReturn) {
                ConcurrentLongLongPairHashMap.LongPair expectedStoredPair =
                        new ConcurrentLongLongPairHashMap.LongPair(0, entry.readableBytes());

                Assertions.assertNotNull(actualStoredPair, "La coppia (ledgerId, entryId) non è stata scritta nella cache");
                Assertions.assertEquals(expectedStoredPair, actualStoredPair, "Il valore della coppia (ledgerId, entryId) scritta non corrisponde");
            } else {
                // Quando la put fallisce, actualStoredPair deve essere null
                Assertions.assertNull(actualStoredPair, "La coppia (ledgerId, entryId) non dovrebbe esistere nella cache");
            }

        } catch (Exception e) {
            throw new RuntimeException("Errore inatteso durante l'esecuzione della put di WriteCache", e);
        }
    }
}
