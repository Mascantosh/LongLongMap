package one;

import one.exception.MapFullException;
import sun.misc.Unsafe;

import java.util.function.LongUnaryOperator;

/**
 * Требуется написать LongLongMap который по произвольному long ключу хранить произвольное long значение
 * Важно: все данные (в том числе дополнительные, если их размер зависит от числа элементов) требуется хранить в выделенном заранее блоке в разделяемой памяти, адрес и размер которого передается в конструкторе
 * для доступа к памяти напрямую необходимо (и достаточно) использовать следующие два метода:
 * sun.misc.Unsafe.getLong(long), sun.misc.Unsafe.putLong(long, long)
 */

/**
 * This map use 32 bytes per record and open addressing to resolve collisions
 * Structure of record is next:
 * key = 8 bytes
 * value = 8 bytes
 * address to next record = 8 bytes
 * address to origin record = 8 bytes
 * <p>
 * Explanation what is origin record:
 * As this map uses open addressing to resolve collisions we can have a case when our bucket in which we excepted certain hash contains key with different hash
 * For instance we have next map (all other records are omitted except keys) and insert a key = 1:
 * indexes: 0 1 2 3
 * keys:    4 8
 * We see that on bucket with index 1 existing key have different hash from inserting key and after placement key 1 we write to key 8 in field origin address location of key 1
 * it gives us boost from linear time complexity to O(k) where k is number of collisions on get operations because we know in which address will lay record with the same hash
 * <p>
 * P.S
 * I also think about close addressing if split our map on 2 regions we can save in such implementation about 8 bytes,
 * but we have a problem if we have a lot collisions on one value and second region is full how we need to allocate new record?
 * It's not fair to throw MapFullException because we have a lot space in first region and as I don't know how to solve it (I suppose it possibly, but too tricky and it's not worth it)
 * So I decide implement open addressed collisions resolver
 */
public class LongLongMap {
    private final Unsafe unsafe;
    private final long startAddress;
    private final long addressSize;

    private final LongUnaryOperator hasher;

    private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;

    //Pow of 2
    private static final byte POWER_OF_STRUCTURE_SIZE = 5;
    private static final byte STRUCTURE_SIZE = 1 << POWER_OF_STRUCTURE_SIZE;

    private static final byte VALUE_OFFSET = LONG_BYTES;
    private static final byte NEXT_OFFSET = LONG_BYTES << 1;
    private static final byte ORIGIN_OFFSET = LONG_BYTES | (LONG_BYTES << 1);

    /**
     * @param unsafe  для доступа к памяти
     * @param address адрес начала выделенной области памяти
     * @param size    размер выделенной области в байтах (~100GB)
     */
    LongLongMap(Unsafe unsafe, long address, long size) {
        this.unsafe = unsafe;
        this.startAddress = address;
        this.addressSize = size;

        final long mapSize = size >> POWER_OF_STRUCTURE_SIZE;

        hasher = initHasher(mapSize);

        //Remove garbage after allocation
        unsafe.setMemory(address, size, (byte) 0);

    }

    /**
     * Метод должен работать со сложностью O(1) при отсутствии коллизий, но может деградировать при их появлении
     *
     * @param k произвольный ключ
     * @param v произвольное значение
     * @return предыдущее значение или 0
     * @throws MapFullException if there is no space to put new value
     */

    long put(long k, long v) throws MapFullException {
        return putV(hasher.applyAsLong(k), k, v);
    }

    /**
     * Метод должен работать со сложностью O(1) при отсутствии коллизий, но может деградировать при их появлении
     *
     * @param k ключ
     * @return значение или 0
     */
    long get(long k) {
        return getV(hasher.applyAsLong(k), k);
    }

    private long getV(long hash, long key) {
        final long keyAddress = mapAddressOnHash(hash);
        final long keyOnAddress = getKeyOnAddress(keyAddress);

        if (keyOnAddress == key || keyOnAddress == 0) {
            return getValueOnAddress(keyAddress);
        }

        final long nextAddress = hashesNotEquals(keyOnAddress, hash) ? getOriginOnAddress(keyAddress) : getNextOnAddress(keyAddress);

        return nextAddress == 0 ? 0L : getCollision(nextAddress, key);
    }

    /**
     * Iterates over collision and find key
     *
     * @param address from which address start collision search
     * @param key to search
     * @return 0 or key value
     */
    private long getCollision(long address, long key) {
        long curKey = getKeyOnAddress(address);

        while (curKey != key && getNextOnAddress(address) != 0) {
            address = getNextOnAddress(address);
            curKey = getKeyOnAddress(address);
        }

        if (key == curKey) {
            return getValueOnAddress(address);
        }

        return 0L;
    }

    /**
     * Find free address for placement searching works in two sides
     *
     * @param collisionAddress from which address we will search an empty address
     * @return address for insertion
     * @throws MapFullException if there is no place for new key
     */
    private long findFreeAddressForCollision(long collisionAddress) throws MapFullException {
        long leftAddress = collisionAddress;
        long rightAddress = collisionAddress;
        long leftKey = getKeyOnAddress(leftAddress);
        long rightKey = getKeyOnAddress(rightAddress);
        final long endMapAddress = startAddress + addressSize;
        boolean crossMapEnd = false;

        //Move to one right pointer if map size is odd because in even size pointers never meet in same address
        if (((addressSize >> POWER_OF_STRUCTURE_SIZE) & 1) == 1) {
            rightAddress += STRUCTURE_SIZE;
            rightAddress = (crossMapEnd = (rightAddress == endMapAddress)) ? startAddress : rightAddress;

            rightKey = getKeyOnAddress(rightAddress);
        }

        //I know that we can do this is one while loop, but I split it on two ones because a general formulas to evaluate current addresses for left and right pointers are more costly
        //In first while loop we move pointers while one of them or both achieve map borders and
        //In second we continues move pointers while we don't find any free address or pointers are meet
        if (!crossMapEnd) {
            while (leftKey != 0 && rightKey != 0 && leftAddress > startAddress && rightAddress < endMapAddress) {
                leftAddress -= STRUCTURE_SIZE;
                rightAddress += STRUCTURE_SIZE;
                leftKey = getKeyOnAddress(leftAddress);
                rightKey = getKeyOnAddress(rightAddress);
            }

            if (leftKey != 0 && rightKey != 0) {
                if (leftAddress == startAddress) {
                    leftAddress = endMapAddress - STRUCTURE_SIZE;
                } else {
                    rightAddress = startAddress;
                }
            }
        }

        while (leftKey != 0 && rightKey != 0 && leftAddress != rightAddress) {
            leftAddress -= STRUCTURE_SIZE;
            rightAddress += STRUCTURE_SIZE;
            leftKey = getKeyOnAddress(leftAddress);
            rightKey = getKeyOnAddress(rightAddress);
        }

        if (leftKey == 0) {
            return leftAddress;
        }

        if (rightKey == 0) {
            return rightAddress;
        }

        throw new MapFullException("Unable to invoke put method because map is full");
    }

    private long putV(long hash, long key, long value) throws MapFullException {
        long address = mapAddressOnHash(hash);
        final long prevKey = getKeyOnAddress(address);

        //Put in clear bucket or replace existing key which equals our key
        if (prevKey == 0 || prevKey == key) {
            return putKeyAndValue(address, key, value);
        }
        //If we have collision and hashes doesn't match that means we need to find original address
        if (hashesNotEquals(prevKey, hash)) {
            final long originAddress = getOriginOnAddress(address);

            //If originAddress doesn't exists that means we put our origin value first time
            if (originAddress == 0) {
                final long freeAddressForCollision = findFreeAddressForCollision(address);
                putOriginOnAddress(address, freeAddressForCollision);
                return putKeyAndValue(freeAddressForCollision, key, value);
            } else {
                address = originAddress;
            }
        }

        return putCollision(address, key, value);
    }

    /**
     * Iterate over collisions to insertion or replacement
     *
     * @param address of collision
     * @param key to insert
     * @param value to insert
     * @return previous value
     * @throws MapFullException if there is no place for new key
     */
    private long putCollision(long address, long key, long value) throws MapFullException {
        long curKey = getKeyOnAddress(address);

        while (key != curKey && getNextOnAddress(address) != 0) {
            address = getNextOnAddress(address);
            curKey = getKeyOnAddress(address);
        }

        if (key == curKey) {
            return putKeyAndValue(address, key, value);
        }

        final long freeAddressForCollision = findFreeAddressForCollision(address);

        putNextOnAddress(address, freeAddressForCollision);

        return putKeyAndValue(freeAddressForCollision, key, value);
    }

    //From here and below utils function

    private long putKeyAndValue(long address, long key, long value) {
        final long prevValue = getValueOnAddress(address);

        putKeyOnAddress(address, key);
        putValueOnAddress(address, value);

        return prevValue;
    }

    private void putKeyOnAddress(long address, long key) {
        putInStructure(address, 0, key);
    }

    private void putValueOnAddress(long address, long value) {
        putInStructure(address, VALUE_OFFSET, value);
    }

    private void putNextOnAddress(long address, long value) {
        putInStructure(address, NEXT_OFFSET, value);
    }

    private void putOriginOnAddress(long address, long value) {
        putInStructure(address, ORIGIN_OFFSET, value);
    }

    private void putInStructure(long address, long offset, long value) {
        unsafe.putLong(address + offset, value);
    }

    private long getKeyOnAddress(long address) {
        return unsafe.getLong(address);
    }

    private long getValueOnAddress(long address) {
        return unsafe.getLong(address + VALUE_OFFSET);
    }

    private long getNextOnAddress(long address) {
        return unsafe.getLong(address + NEXT_OFFSET);
    }

    private long getOriginOnAddress(long address) {
        return unsafe.getLong(address + ORIGIN_OFFSET);
    }

    private long mapAddressOnHash(long hash) {
        return startAddress + hashToOffset(hash);
    }

    private long hashToOffset(long hash) {
        return hash << POWER_OF_STRUCTURE_SIZE;
    }

    private boolean hashesNotEquals(long prevKey, long curHash) {
        return hasher.applyAsLong(prevKey) != curHash;
    }

    private LongUnaryOperator initHasher(long mapSize) {
        return ((mapSize - 1) & mapSize) == 0
                ? value -> value & (mapSize - 1)
                : value -> value % mapSize;
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        for (long i = startAddress; i < startAddress + addressSize; i += STRUCTURE_SIZE) {
            stringBuilder.append(getMapRecord(i));
        }

        return stringBuilder.toString();
    }

    private String getMapRecord(long address) {
        return getKeyOnAddress(address) == 0
                ? ""
                : "Address = " + address + "\n" +
                "Key = " + getKeyOnAddress(address) + "\n" +
                "Value = " + getValueOnAddress(address) + "\n" +
                "Next = " + getNextOnAddress(address) + "\n" +
                "Origin = " + getOriginOnAddress(address) + "\n";
    }
}
