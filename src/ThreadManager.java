import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.PriorityQueue;


public class ThreadManager implements Runnable{

	private File[] unSortedFiles;
	static Comparator<String> comparator;
	final static File outputFile = new File("output.txt"); //File name where final output will be saved
	int noOfFiles;
	
	/**
	 *Contructor*/
	ThreadManager(){
		File f = new File("UNSORTED_CHUNKS/");
	    unSortedFiles = f.listFiles();
	}
	
	public void Manager(final long start) throws IOException, InterruptedException {
		
		/*Comaparator to compare text lines in two files and returning the smaller one*/
		comparator = new Comparator<String>() {
            public int compare(String r1, String r2){
                return r1.compareTo(r2);
            }
		};
		noOfFiles = unSortedFiles.length;
		
		
		//Threads pool service started (8 concurrent running threads)
		ExecutorService executorService = Executors.newFixedThreadPool(8);
		executorService.execute(new Runnable() { //executing threads
			
		    public void run() {
		        ArrayList<String> sortedLines = null;
				int noOfFiles = unSortedFiles.length;
				System.out.println("Total no. of Chunks created: "+noOfFiles+"\n");
				System.out.println("Now Reading & Sorting Unsorted Chunks into Sorted Chunks. Please Wait...\n");
				for(int i=0; i<unSortedFiles.length; i++)  {	
					try {	
						sortedLines = chunksReadAndSort(i);
						writeSortedChunks(sortedLines,i);		
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				//Threads started for Sorting and Merging all the Sorted chunks into 1 single file
				System.out.println("Now Sorted Chunks will Sort and Merge into 1 File using Threads");
				System.out.println("\nMulti-Threading started...\n");
				
				for(int i=1;i<=8;i++){
					System.out.println("Thread# "+i+" started..");
					ThreadManager tmg = new ThreadManager();
					Thread t =  new Thread(tmg);
					t.start();	
				}
				System.out.println("\nFiles Sorted and Merged successfully !!");
				System.out.println("\nProcess Completed..\n[Please find the file in the root folder as 'output.txt']");
				long stop = System.currentTimeMillis();
				long diff = stop - start;
				System.out.println("\nTOTAL TIME ELAPSED: "+diff/1000+" seconds");	        
		    }
		});
		executorService.shutdown();	
	}
	
	/**
	 * Thread's Run function
	 */
	@Override
	public void run() {
		try {
			mergeSortedFiles(outputFile, comparator);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/** #1. CHUNK READER:
	 * Reads Unsorted Chunks and Sort them using Merge Sort
	 * @param i
	 * @return
	 * @throws IOException
	 */
	private ArrayList<String> chunksReadAndSort(int i) throws IOException {
		
		String fname;
		
		ArrayList<String> lines = null;
		ArrayList<String> StringList = null;
		BufferedReader br = null;
		String line = "";
		lines	= new ArrayList<String>();
		fname = unSortedFiles[i].getName();
		br = new BufferedReader(new FileReader("UNSORTED_CHUNKS/"+fname));
		
		while((line = br.readLine())!=null){
			String key = line.substring(0, 10); // Extracting first 10 byte string key
			String val = line.substring(10, line.length());
			lines.add(key+val);
		}
	
		StringList = sort(lines,i); // Calling the sort() function
		//System.out.print(i +"/r"); 
		
		return StringList;		
	}

	/** #2. CHUNK SORTER:
	 * Sorts all the lines in a text file chunk
	 * @param i 
	 * @param nHash
	 * @return
	 */
	private ArrayList<String> sort(ArrayList<String> lines, int i) {
		
		ArrayList<String> keys = null; // ArrayList containing all the Unsorted Keys
		ArrayList<String> sortedKeys; // ArrayList containing all the Sorted Keys
		ArrayList<String> sortedLines = null; // ArrayList containing all the Sorted Lines (keys+values)
		Hashtable<String, String> whole = new Hashtable<String, String>();
		String key;
		String val = null;
		int count = 0;
		
		keys = new ArrayList<String>(); 
		
		for(String ss: lines){
			
			//Declaring object for keys
			key = ss.substring(0, 10);
			val = ss.substring(10,ss.length());
			keys.add(key);
			whole.put(key,val);
				
			count++;
		}
		sortedKeys = mergeSort(keys);
		sortedLines = new ArrayList<String>();
		for(String s1: sortedKeys){
			String value = whole.get(s1);
			sortedLines.add(s1+" "+value);
			
		}
		//System.out.println("No of sorted lines: "+sortedLines.size());
		return sortedLines;
	}

	/** #3.SORTING THROUGH MERGE SORT ALGO:
	 * Sorting each individual text file chunks using this Merge sort function
	 * @param whole
	 * @return
	 */
	public ArrayList<String> mergeSort(ArrayList<String> whole) {
	    ArrayList<String> left = new ArrayList<String>();
	    ArrayList<String> right = new ArrayList<String>();
	    int center;
	 
	    if (whole.size() == 1) {    
	        return whole;
	    } else {
	        center = whole.size()/2;
	        // copy the left half of whole into the left.
	        for (int i=0; i<center; i++) {
	                left.add(whole.get(i));
	        }
	        //copy the right half of whole into the new arraylist.
	        for (int i=center; i<whole.size(); i++) {
	                right.add(whole.get(i));
	        }
	        // Sort the left and right halves of the arraylist.
	        left  = mergeSort(left);
	        right = mergeSort(right);
	 
	        // Merge the results back together.
	        merge(left, right, whole);
	    }
	    return whole;
	}
	
	/** #3.1 MERGING 
	 * Merging left and the right ArrayLists and comparing
	 * @param left
	 * @param right
	 * @param whole
	 */
	private void merge(ArrayList<String> left, ArrayList<String> right, ArrayList<String> whole) {
	    int leftIndex = 0;
	    int rightIndex = 0;
	    int wholeIndex = 0;
	 
	    /* While both left and right ArrayLists exists i.e. are not null, 
	     * keep taking the smaller of left.get(leftIndex)
	     * or right.get(rightIndex) and adding it at both.get(bothIndex).*/
	    
	    while (leftIndex < left.size() && rightIndex < right.size()) {
	        if ( (left.get(leftIndex).compareTo(right.get(rightIndex))) < 0) {
	            whole.set(wholeIndex, left.get(leftIndex));
	            leftIndex++;
	        } else {
	            whole.set(wholeIndex, right.get(rightIndex));
	            rightIndex++;
	        }
	        wholeIndex++;
	    }
	    ArrayList<String> rest;
	    int restIndex;
	    
	    if (leftIndex >= left.size()) { 
	        rest = right; // The left ArrayList has been use up...
	        restIndex = rightIndex;
	    } else {
	        rest = left; // The right ArrayList has been used up...
	        restIndex = leftIndex;
	    }
	 
	    // Copy the rest of whichever ArrayList (left or right) was not used up.
	    for (int i=restIndex; i<rest.size(); i++) {
	        whole.set(wholeIndex, rest.get(i));
	        wholeIndex++;
	    }
	}
	
	/** #4. WRITE SORTED CHUNK FILES
	 * Writing sorted chunks in new directory
	 * @param sortedLines
	 * @param i
	 * @throws IOException
	 */
	private void writeSortedChunks(ArrayList<String> sortedLines, int i) throws IOException {
		
		String folder = "SORTED_CHUNKS/";
		File dir = new File(folder); //If the folder doesn't exists it will create one
		if(!dir.exists()){
			dir.mkdir();
		}
		
		File tmpfile = new File(folder+(i+1)+".txt");
		
		BufferedWriter fbw = new BufferedWriter(new FileWriter(tmpfile));
		for(String str: sortedLines) {
			fbw.write(str);
            fbw.newLine();
		}
		fbw.close();
	}
	
	/** #5. MERGING CHUNKS IN ONE BIG FILE
	 * This will Sort all the chunks with one another and Merge them into single big file.
	 * @param outputfile
	 * @param cmp
	 * @return
	 * @throws IOException
	 */
	public static int mergeSortedFiles(File outputfile, final Comparator<String> cmp) throws IOException {
		
		List<File> files = new ArrayList<File>();
		File folder = new File ("SORTED_CHUNKS/");
		File[] allFiles = folder.listFiles();
		
		for(File file: allFiles){
			if(file.getName() != ".DS_Store" && file.isFile()){ 
				files.add(file);
			}
		}
		
		//outputFile = oneMerge(files);
		
        PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(11, 
            new Comparator<BinaryFileBuffer>() {
              public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
                return cmp.compare(i.peek(), j.peek());
              }
            }
        );
        
        for (File f : files) {
            BinaryFileBuffer bfb = new BinaryFileBuffer(f);
            pq.add(bfb);    
        }
        
        BufferedWriter fbw = new BufferedWriter(new FileWriter(outputfile));
        int rowcounter = 0;
        try {
            while(pq.size()>0) {
                BinaryFileBuffer bfb = pq.poll();
                String r = bfb.pop();
                fbw.write(r);
                fbw.newLine();
                ++rowcounter;
                if(bfb.empty()) {
                    bfb.fbr.close();
                 // Deleting old chunks to save space, because we dont need them anymore
                    bfb.originalfile.delete(); 
                } else {
                    pq.add(bfb); // add it back
                }
            }
        } finally { 
            fbw.close();
            for(BinaryFileBuffer bfb : pq ) bfb.close();
        }
        return rowcounter;
	}	
	
	/*public static File oneMerge(List<File> files){
		List<File> left = new ArrayList<File>();
		List<File> right = new ArrayList<File>();
		File out = new File("out.txt");
		PrintWriter output=new PrintWriter(out);
		int index = 0;
		int center;
		
		if (files.size() == 1) {   
			out = 
	        return (files.get(index));
	    } 
		else {
	        center = files.size()/2;
	        // copy the left half of whole into the left.
	        for (int i=0; i<center; i++) {
	        		files.get(i)
	                left.add(files.get(i));
	        }
	        //copy the right half of whole into the new arraylist.
	        for (int i=center; i<files.size(); i++) {
	                right.add(files.get(i));
	        }
	        // Sort the left and right halves of the arraylist.
	        left  = mergeSort(left);
	        right = mergeSort(right);
	 
	        // Merge the results back together.
	        merge(left, right, files);
	    }
	    return files;
	}
				
		
	}*/
}

