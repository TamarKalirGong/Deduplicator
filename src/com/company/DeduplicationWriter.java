package com.company;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;

import static com.company.Constants.MAX_BLOCK_SIZE;

public class DeduplicationWriter {
    public static void write_deduplicated_file(Deduplicator.DedupInfo dedupInfo, FileOutputStream output) throws IOException {
        write_header(dedupInfo.num_of_unique_hashes, dedupInfo.hash_indexes.size(), output);
        write_unique_chunks(dedupInfo.num_of_unique_hashes, output);
        write_chunk_indices(dedupInfo.hash_indexes, output);
    }

    private static void write_header(int num_of_unique_hashes, int number_of_chunks, FileOutputStream output) throws IOException {
        ByteBuffer metadata_buffer = ByteBuffer.allocate(4);
        output.write(metadata_buffer.putInt(0, num_of_unique_hashes).array());
        output.write(metadata_buffer.putInt(0, number_of_chunks).array());
    }

    private static void write_chunk_indices(List<Integer> hash_indexes, FileOutputStream output) {
        ByteBuffer index_buffer = ByteBuffer.allocate(4);
        hash_indexes.forEach(index -> {
            try {
                output.write(index_buffer.putInt(0, index).array());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static String name_for_chunck_file(int chunck_index) {
        return chunck_index + ".cnk";
    }

    static void write_unique_chunks(int num_of_unique_hashes, FileOutputStream output) {
        byte[] chunck_buffer = new byte[MAX_BLOCK_SIZE];
        ByteBuffer length_buffer = ByteBuffer.allocate(4);
        IntStream stream = IntStream.range(0, num_of_unique_hashes);

        stream.forEach(index -> {
            try {
                int chunck_length;
                FileInputStream chunck_file = new FileInputStream(name_for_chunck_file(index));
                chunck_length = chunck_file.read(chunck_buffer, 0, MAX_BLOCK_SIZE);
                output.write(length_buffer.putInt(0, chunck_length).array());
                output.write(chunck_buffer, 0, chunck_length);
            }catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void write_undeduplicated_file(Deduplicator.DedupInfo dedupInfo, FileOutputStream output) {
        byte[] chunck_buffer = new byte[MAX_BLOCK_SIZE];

        dedupInfo.hash_indexes.forEach(chunk_index -> {
            try {
                FileInputStream chunck_file = new FileInputStream(name_for_chunck_file(chunk_index));
                int nread = chunck_file.read(chunck_buffer);
                output.write(chunck_buffer, 0, nread);
            }catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
