package com.company;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.company.Constants.MAX_BLOCK_SIZE;

public class Deduplicator {

    private static final int SHA1_DIGEST_LENGTH = 20;

    private final byte[] hashBuffer = new byte[SHA1_DIGEST_LENGTH];
    private final Map<HashValue, Integer> seenHashes = new HashMap<>();
    private final List<HashValue> hashValues = new ArrayList<>();
    private final List<Integer> chunk_to_hash_mapping = new ArrayList<>();

    public DedupInfo dedup(RandomAccessFile input) throws IOException, NoSuchAlgorithmException, DigestException {
        byte[] buffer = new byte[MAX_BLOCK_SIZE];
        int cursor = 0;
        int current_chunck_size;
        while(input.read(buffer, 0, MAX_BLOCK_SIZE) != -1) {
            current_chunck_size = decide_chunck_size(buffer);
            process_chunck(buffer, current_chunck_size);
            cursor += current_chunck_size;
            input.seek(cursor);
        }
        return new DedupInfo(chunk_to_hash_mapping, seenHashes.size());
    }

    private int decide_chunck_size(byte[] buffer) {
        return 10;
    }

    private boolean encountered_hash(HashValue hashValue) {
        return seenHashes.containsKey(hashValue);
    }

    private int process_new_hash(HashValue hashValue, byte[] data, int data_length) throws IOException {
        hashValues.add(hashValue);
        final int chunck_index = seenHashes.size();
        seenHashes.put(hashValue, seenHashes.size());
        String file_name = chunck_index + ".cnk";
        try (FileOutputStream writer = new FileOutputStream(file_name)) {
            writer.write(data, 0, data_length);
        }
        return chunck_index;
    }

    private void process_chunck(byte[] buffer, int chunck_size) throws NoSuchAlgorithmException, IOException, DigestException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        md.reset();
        md.update(buffer, 0, chunck_size);
        HashValue hashValue = new HashValue();
        md.digest(hashValue.hashBuffer, 0, SHA1_DIGEST_LENGTH);
        int chunck_index;
        if (encountered_hash(hashValue)) {
            chunck_index = seenHashes.get(hashValue);
        }else{
            chunck_index = process_new_hash(hashValue, buffer, chunck_size);
        }
        chunk_to_hash_mapping.add(chunck_index);
    }


    public static class BlockPointer {
        final long offset;
        final long size;

        public BlockPointer(long offset, long size) {
            this.offset = offset;
            this.size = size;
        }
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
        int num_of_unique_hashes;

        public DedupInfo(List<Integer> hash_indexes, int num_of_unique_hashes) {
            this.hash_indexes = hash_indexes;
            this.num_of_unique_hashes = num_of_unique_hashes;
        }
    }
}