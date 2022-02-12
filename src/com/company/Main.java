package com.company;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;

public class Main {

    final static int MODE_PARAMERTER_INDEX = 1;
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
        Instant start = Instant.now();
	    if (args.length != 4) {
            System.out.println("Deduper -dedup <input_file_name> <output_filename>");
            return;
        }
	    try {
	        if(args[MODE_PARAMERTER_INDEX].equals("0")) {
                deduplicate(args);
            }else{
	            undedup(args);
            }
        }catch (Exception e) {
	        e.printStackTrace();
        }
        //generate_test_file();
        Instant end = Instant.now();
	    Duration duration = Duration.between(start, end);
	    System.out.println("time: " + duration.toSeconds() + " secs");
    }

    private static void deduplicate(String[] args) throws IOException, DigestException, NoSuchAlgorithmException {
        final RandomAccessFile input = new RandomAccessFile(args[INPUT_FILE_PARAMERTER_INDEX], "r"); //autoclose
        final Deduplicator deduplicator = new Deduplicator();
        final Deduplicator.DedupInfo dedupInfo = deduplicator.dedup(input);
        DeduplicationWriter.write_deduplicated_file(dedupInfo, new FileOutputStream(args[OUTPUT_FILE_PARAMERTER_INDEX]), input);
    }

    private static void undedup(String[] args) throws IOException {
        final RandomAccessFile input = new RandomAccessFile(args[INPUT_FILE_PARAMERTER_INDEX], "r"); //autoclose
        Deduplicator.DedupInfo dedupInfo = new DeduplicationReader().read_deduplicated_file(new RandomAccessFile(args[INPUT_FILE_PARAMERTER_INDEX], "r"));
        DeduplicationWriter.write_undeduplicated_file(dedupInfo, new FileOutputStream(args[OUTPUT_FILE_PARAMERTER_INDEX]), input);
    }

}
