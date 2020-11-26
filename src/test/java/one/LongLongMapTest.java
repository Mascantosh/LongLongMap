package one;

import one.exception.MapFullException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Random;

class LongLongMapTest {
    private static Unsafe unsafe;

    @BeforeAll
    static void setUp() throws NoSuchFieldException, IllegalAccessException {
        Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        unsafe = (Unsafe) theUnsafe.get(null);
    }

    @Test
    void put() throws MapFullException {
        final long bytes = 32 << 8;
        final long startAddress = unsafe.allocateMemory(bytes);

        final LongLongMap longLongMap = new LongLongMap(unsafe, startAddress, bytes);

        for (int i = 1; i <= (bytes >> 5); ++i) {
            Assertions.assertEquals(0L, longLongMap.put(i, i * i));
        }
    }

    @Test
    void prevValue() throws MapFullException {
        final long bytes = 32 << 8;
        final long startAddress = unsafe.allocateMemory(bytes);

        final LongLongMap longLongMap = new LongLongMap(unsafe, startAddress, bytes);
        final long key = 1;
        final long initValue = 0;
        final long prevValue = 10;
        final long newValue = 20;

        Assertions.assertEquals(initValue, longLongMap.put(key, prevValue));
        Assertions.assertEquals(prevValue, longLongMap.put(key, newValue));
    }

    @Test
    void get() throws MapFullException {
        final long bytes = 32 << 8;
        final long startAddress = unsafe.allocateMemory(bytes);

        final LongLongMap longLongMap = new LongLongMap(unsafe, startAddress, bytes);

        for (int i = 1; i <= (bytes >> 5); ++i) {
            final int value = i * i;
            longLongMap.put(i, value);
            Assertions.assertEquals(value, longLongMap.get(i));
        }
    }

    @Test
    void sizeNotPowerOfTwo() throws MapFullException {
        final long bytes = (32 << 15) + 32;
        final long startAddress = unsafe.allocateMemory(bytes);

        final Random random = new Random();
        final LongLongMap longLongMap = new LongLongMap(unsafe, startAddress, bytes);

        for (long i = 1; i <= (bytes >> 5); ++i) {
            final long key = random.nextLong();

            final long value = key * key;
            longLongMap.put(key, value);

            Assertions.assertEquals(value, longLongMap.get(key));
        }
    }

    @Test
    void stressTest() throws MapFullException {
        final long bytes = 32 << 15;
        final long startAddress = unsafe.allocateMemory(bytes);

        final Random random = new Random();
        final LongLongMap longLongMap = new LongLongMap(unsafe, startAddress, bytes);

        for (long i = 1; i <= (bytes >> 5); ++i) {
            final long key = random.nextLong();

            final long value = key * key;
            longLongMap.put(key, value);

            Assertions.assertEquals(value, longLongMap.get(key));
        }
    }
}
