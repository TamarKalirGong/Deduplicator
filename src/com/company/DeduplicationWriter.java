package com.company;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;

import static com.company.Constants.MAX_BLOCK_SIZE;

public class DeduplicationWriter {
    public static void write_deduplicated_file(Deduplicator.DedupInfo dedupInfo, FileOutputStream output, RandomAccessFile input) throws IOException {
        write_header(dedupInfo.num_of_unique_hashes, dedupInfo.hash_indexes.size(), output);
        write_unique_chunks(dedupInfo.num_of_unique_hashes, dedupInfo.unique_chunks_positions, output, input);
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

    public static String name_for_chunk_file(int chunk_index) {
        return chunk_index + ".cnk";
    }

    static void write_unique_chunks(int num_of_unique_hashes, List<Deduplicator.ChunkDescriptor> chunks, FileOutputStream output, RandomAccessFile input) throws IOException {
        byte[] chunk_buffer = new byte[MAX_BLOCK_SIZE];
        ByteBuffer length_buffer = ByteBuffer.allocate(4);

        for(int i = 0; i < num_of_unique_hashes; i++) {
            Deduplicator.ChunkDescriptor chunk = chunks.get(i);

            input.seek(chunk.pos);
            input.read(chunk_buffer, 0, chunk.length);
            output.write(length_buffer.putInt(0, chunk.length).array());
            output.write(chunk_buffer, 0, chunk.length);
        }
    }

    public static void write_undeduplicated_file(Deduplicator.DedupInfo dedupInfo, FileOutputStream output, RandomAccessFile input) throws IOException {
        byte[] chunk_buffer = new byte[MAX_BLOCK_SIZE];
        for(int i = 0; i < dedupInfo.hash_indexes.size(); i++) {
            Deduplicator.ChunkDescriptor chunk_descriptor = dedupInfo.unique_chunks_positions.get(dedupInfo.hash_indexes.get(i));
            input.seek(chunk_descriptor.pos);
            input.read(chunk_buffer, 0, chunk_descriptor.length);
            output.write(chunk_buffer, 0, chunk_descriptor.length);
        }
    }
}
