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
		
		/* Displaying the System/Hardware/CPU Information on which this code will run */
		SystemInformation info = new SystemInformation();
		info.info();
		System.out.println("------------------------------------------------------------------");
		System.out.println("\nFILE PROCESSING STARTED...\n------------------------------------------------------------------\n"
				+ "Please Wait ! This may take time.\n");
		inputFile = new File(input);
		
		System.out.println("1.Splitting Files into chunks");
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
		long freeMem;
        long sizeoffile = filetobesorted.length();
        final int MAXTEMPFILES = 1024;
        long blocksize = sizeoffile / MAXTEMPFILES ;
        /* Total amount of free memory available to the JVM */
        System.out.println("Free memory (bytes): " + (freeMem=Runtime.getRuntime().freeMemory()));
        if( blocksize < freeMem/2)
            blocksize = freeMem/2;
        else {
            if(blocksize >= freeMem) 
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
	public static void readerAndSplitter(String input) throws FileNotFoundException{
		BufferedReader reader = null;
		PrintWriter outputStream = null;
		File file = new File(input);
		long blocksize = estimateBestSizeOfBlocks(file);// in bytes
		int rowCount = 1;
		int fileNo = 1;
		ArrayList<String> rows = new ArrayList<String>(); //It will store all the lines and provide for each chunk
		
		try {
			reader  = new BufferedReader(new FileReader(input));
		String line = "";
		
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
		System.out.println("Unsorted Chunks created..");
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
		String folder = "UNSORTED_CHUNKS/"; // Directory where unsorted splitted file chunks will be stored
		File dir = new File(folder);
		
		if(!dir.exists()){ //In case directory does not exists it will create a new directory
			dir.mkdir();
		}
		
		int size = rows.size();
		try {
			writer = new FileWriter(folder+fileNo+".txt");
			for (String str: rows) {
	            writer.write(str);
	            if(str.contains(" "))//This prevent creating a blank like at the end of the file**
	                writer.write("\n");
	        }
	        writer.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}			
	}	
}