package com.company;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.company.DeduplicationWriter.name_for_chunk_file;
import static com.company.Deduplicator.ChunkDescriptor;
import static com.company.Deduplicator.DedupInfo;


public class DeduplicationReader {
    ByteBuffer metadata_buffer = ByteBuffer.allocate(4);

    List<ChunkDescriptor> read_chunks(int num_of_unique_hashes, RandomAccessFile input, byte[] buffer) throws IOException {
        final List<ChunkDescriptor> chunkDescriptors = new ArrayList<>();
        long pos = 8;
        for(int i = 0; i < num_of_unique_hashes; i++) {
            int chunk_length = input.readInt();
            pos += 4;
            ChunkDescriptor chunkDescriptor = new ChunkDescriptor(pos, chunk_length);
            pos += chunk_length;
            input.seek(pos);
            chunkDescriptors.add(chunkDescriptor);
        }
        return chunkDescriptors;
    }

    public DedupInfo read_deduplicated_file(RandomAccessFile input) throws IOException {
        byte[] buffer = new byte[Constants.MAX_BLOCK_SIZE];
        int num_of_unique_hashes = input.readInt();
        int num_of_chunks = input.readInt();
        List<ChunkDescriptor> chunks = read_chunks(num_of_unique_hashes, input, buffer);
        List<Integer> hash_indexes = read_hash_indices(num_of_chunks, input);
        return new DedupInfo(hash_indexes, chunks, num_of_unique_hashes);
    }

    private List<Integer> read_hash_indices(int num_of_chunks, RandomAccessFile input) {
        return IntStream.range(0, num_of_chunks).map((index) -> {
            try {
                return input.readInt();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).boxed().collect(Collectors.toList());
    }
}
