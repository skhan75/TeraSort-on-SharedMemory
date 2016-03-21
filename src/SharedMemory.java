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
	public static long blocksSizer(File filetobesorted) {
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
	public static void readerAndSplitter(String input) throws FileNotFoundException{
		BufferedReader br = null; //Declaring buffered reader for storing lines
		PrintWriter outputStream = null;
		long blocksize = blocksSizer(new File(input));// in bytes
		int rowCount = 1;
		int fileNo = 1;
		ArrayList<String> rows = new ArrayList<String>(); //It will store all the lines and provide for each chunk
		
		try {
			br  = new BufferedReader(new FileReader(input));
		String line = "";
			while (line!=null){
				line = br.readLine();
				long currentblocksize = 0;// in bytes
				while(currentblocksize < blocksize && (line) != null){
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
		} catch (IOException e) {
			e.printStackTrace();
		}			
	}	
}