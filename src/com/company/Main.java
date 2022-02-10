package com.company;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Main {

    final static int INPUT_FILE_PARAMERTER_INDEX = 2;
    final static int OUTPUT_FILE_PARAMERTER_INDEX = 3;

    private static void generate_test_file(){
        try {
            FileOutputStream file = new FileOutputStream("test.fil");
            String as = "aaaaaaaaaa";
            String bs = "bbbbbbbbbb";
            file.write(as.getBytes(StandardCharsets.UTF_8));
            file.write(bs.getBytes(StandardCharsets.UTF_8));
            file.write(as.getBytes(StandardCharsets.UTF_8));
            file.write(bs.getBytes(StandardCharsets.UTF_8));
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
	    if (args.length != 4) {
            System.out.println("Deduper -dedup <input_file_name> <output_filename>");
            return;
        }
	    try {
            final RandomAccessFile input = new RandomAccessFile(args[INPUT_FILE_PARAMERTER_INDEX], "r"); //autoclose
            final Deduplicator deduplicator = new Deduplicator();
            final Deduplicator.DedupInfo dedupInfo = deduplicator.dedup(input);
            DeduplicationReaderWriter.write_deduplicated_file(dedupInfo, new FileOutputStream(args[OUTPUT_FILE_PARAMERTER_INDEX]));
        }catch (Exception e) {
	        e.printStackTrace();
        }
        //generate_test_file();
    }
}
