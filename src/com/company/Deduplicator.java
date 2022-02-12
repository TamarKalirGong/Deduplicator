package com.company;

import java.io.*;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.LongStream;

import static com.company.Constants.*;

public class Deduplicator {

    private static final int SHA1_DIGEST_LENGTH = 20;

    private final byte[] hashBuffer = new byte[SHA1_DIGEST_LENGTH];
    private final Map<HashValue, Integer> seenHashes = new HashMap<>();
    private final List<HashValue> hashValues = new ArrayList<>();
    private final List<Integer> chunk_to_hash_mapping = new ArrayList<>();
    private final List<ChunkDescriptor> unique_chunks = new ArrayList<>();
    private final long[] gear = generate_gear();

    private long[] generate_gear() {
        final Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        return LongStream.generate(random::nextLong)
                .map(num -> num > 0 ? num : -num).limit(256).toArray();
    }

    public DedupInfo dedup(RandomAccessFile input) throws IOException, NoSuchAlgorithmException, DigestException {
        byte[] buffer = new byte[MAX_BLOCK_SIZE];
        long cursor = 0;
        int current_chunk_size;
        int nread;
        while((nread = input.read(buffer, 0, MAX_BLOCK_SIZE)) != -1) {
            current_chunk_size = decide_chunk_size(buffer, nread);
            // QSystem.out.println(current_chunk_size);
            process_chunk(buffer, current_chunk_size, cursor);
            cursor += current_chunk_size;
            input.seek(cursor);
        }
        return new DedupInfo(chunk_to_hash_mapping, unique_chunks, seenHashes.size());
    }

    private int decide_chunk_size(byte[] buffer, int nread) {
        long fingerprint = 0;
        int cursor;
        if (nread <= MIN_BLOCK_SIZE)
            return nread;

        for (cursor = MIN_BLOCK_SIZE; cursor < buffer.length; cursor++) {
            fingerprint = (fingerprint << 1) + gear[buffer[cursor] & 0xFF];
            if ((fingerprint % (8 * KB)) == 0)
                    return cursor;
        }
        return MAX_BLOCK_SIZE;
        //return 10;
    }

    private boolean encountered_hash(HashValue hashValue) {
        return seenHashes.containsKey(hashValue);
    }

    private int process_new_hash(HashValue hashValue, byte[] data, int data_length, long pos) {
        hashValues.add(hashValue);
        final int chunk_index = seenHashes.size();
        seenHashes.put(hashValue, seenHashes.size());
        unique_chunks.add(new ChunkDescriptor(pos, data_length));
        return chunk_index;
    }

    private void process_chunk(byte[] buffer, int chunk_size, long pos) throws NoSuchAlgorithmException, DigestException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        md.reset();
        md.update(buffer, 0, chunk_size);
        HashValue hashValue = new HashValue();
        md.digest(hashValue.hashBuffer, 0, SHA1_DIGEST_LENGTH);
        int chunk_index;
        if (encountered_hash(hashValue)) {
            chunk_index = seenHashes.get(hashValue);
        }else{
            chunk_index = process_new_hash(hashValue, buffer, chunk_size, pos);
        }
        chunk_to_hash_mapping.add(chunk_index);
    }

    public static class HashValue {
        byte[] hashBuffer = new byte[SHA1_DIGEST_LENGTH];

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HashValue hashValue = (HashValue) o;
            return Arrays.equals(hashBuffer, hashValue.hashBuffer);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(hashBuffer);
        }
    }

    public static class DedupInfo {
        List<Integer> hash_indexes;
        List<ChunkDescriptor> unique_chunks_positions;
        int num_of_unique_hashes;

        public DedupInfo(List<Integer> hash_indexes, List<ChunkDescriptor> unique_chunks_positions, int num_of_unique_hashes) {
            this.hash_indexes = hash_indexes;
            this.unique_chunks_positions = unique_chunks_positions;
            this.num_of_unique_hashes = num_of_unique_hashes;
        }
    }

    public static class ChunkDescriptor {
        long pos;
        int length;

        public ChunkDescriptor(long pos, int length) {
            this.pos = pos;
            this.length = length;
        }
    }
}
