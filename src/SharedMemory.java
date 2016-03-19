import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class SharedMemory {
	
	static String input;
	static String output;
	private static File inputFile;
	
	public static void main(String[] args) throws InterruptedException, IOException{
		
		input = "TestInput.txt";
		System.out.println("File Processing Started...\nPlease Wait ! This may take time.\n");
		inputFile = new File(input);
		
		System.out.println("Splitting Files into chunks");
		long start = System.currentTimeMillis();
		readerAndSplitter(input); //Reading a big file and splitting into smaller chunks according to blockSize
		System.out.println("[You can find them in the Application's root directory as 'UNSORTED_CHUNKS]\n");
		ThreadManager tmg = new ThreadManager();
		tmg.Manager(start); //object declaration
		
	}	
	
	/**
	 * we don't want to open up much more than 1024 temporary files, better run
       out of memory first. (Even 1024 is stretching it.). On the other hand, we don't want to create many temporary files
       for no reason. 
       If blocksize is smaller than half the free memory, grow it.
	 * @param filetobesorted
	 * @return
	 */
	public static long estimateBestSizeOfBlocks(File filetobesorted) {
        long sizeoffile = filetobesorted.length();
        final int MAXTEMPFILES = 1024;
        long blocksize = sizeoffile / MAXTEMPFILES ;
        long freemem = Runtime.getRuntime().freeMemory();
        if( blocksize < freemem/2)
            blocksize = freemem/2;
        else {
            if(blocksize >= freemem) 
              System.err.println("Ran out of memory! ");
        }
        return blocksize;
    }
	
	/**
	 * Read the big file and split it into multiple usnorted chunks
	 * and call the function "fileChunkMaker(String, Integer)"
	 * @author SamAK 
	 * @param input
	 */
	public static void readerAndSplitter(String input){
		BufferedReader reader = null;
		PrintWriter outputStream = null;
		long blocksize = estimateBestSizeOfBlocks(new File(input));// in bytes
		int rowCount = 1;
		int fileNo = 1;
		ArrayList<String> rows = new ArrayList<String>();
		try {
			reader  = new BufferedReader(new FileReader(input));
		} 
		catch (FileNotFoundException e){
			e.printStackTrace();
		}
		String line = "";
		
		try {		
			while (line!=null){
				long currentblocksize = 0;// in bytes
				while(currentblocksize < blocksize && (line = reader.readLine()) != null){
					rows.add(line);
					currentblocksize += line.length(); 
				}
				fileChunkMaker(rows, fileNo);
				fileNo++;
				rows.clear();
			}
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Receive the unsorted chunks from "readerAndSplitter(String input)" 
	 * and write them into multiple chunk files.
	 * @param rows
	 * @param fileNo
	 */
	private static void fileChunkMaker(ArrayList<String> rows, int fileNo) {
		
		int noOfFileChunks = fileNo; //No of file chunks created
		FileWriter writer = null;
		int size = rows.size();
		try {
			writer = new FileWriter("UNSORTED_CHUNKS/"+fileNo+".txt");
			for (String str: rows) {
	            writer.write(str);
	            if(str.contains(" "))//This prevent creating a blank like at the end of the file**
	                writer.write("\n");
	        }
	        writer.close();
	      //System.out.println(noOfFileChunks);
		} catch (IOException e1) {
			e1.printStackTrace();
		}			
	}	
}