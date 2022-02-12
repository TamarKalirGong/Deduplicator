package com.company;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.company.DeduplicationWriter.name_for_chunck_file;

public class DeduplicationReader {
    ByteBuffer metadata_buffer = ByteBuffer.allocate(4);

    private int read_int(FileInputStream input) throws IOException {
        input.read(metadata_buffer.array(), 0, 4);
        return metadata_buffer.getInt(0);
    }

    void read_chunks_and_generate_chunk_files(int num_of_unique_hashes, FileInputStream input, byte[] buffer) {
        IntStream.range(0, num_of_unique_hashes).forEach(index -> {
            try {
                int chunk_length = read_int(input);
                input.read(buffer, 0, chunk_length);
                try (FileOutputStream writer = new FileOutputStream(name_for_chunck_file(index))) {
                    writer.write(buffer, 0, chunk_length);
                }
            }catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Deduplicator.DedupInfo read_deduplicated_file(FileInputStream input) throws IOException {
        byte[] buffer = new byte[Constants.MAX_BLOCK_SIZE];
        int num_of_unique_hashes = read_int(input);
        int num_of_chunks = read_int(input);
        read_chunks_and_generate_chunk_files(num_of_unique_hashes, input, buffer);
        List<Integer> hash_indexes = read_hash_indices(num_of_chunks, input);
        return new Deduplicator.DedupInfo(hash_indexes, num_of_unique_hashes);
    }

    private List<Integer> read_hash_indices(int num_of_chunks, FileInputStream input) {
        return IntStream.range(0, num_of_chunks).map((index) -> {
            try {
                return read_int(input);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).boxed().collect(Collectors.toList());
    }
}
