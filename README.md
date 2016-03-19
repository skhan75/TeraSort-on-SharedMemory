# TeraSort-on-SharedMemory
"in.txt" is a gensort generated text. You can create one with this http://www.ordinal.com/gensort.html

Creating a Gensort string file:
-------------------------------
1. Go to directory where you've installed gensort.
2. Type ./gensort -a <no_of_Records> <filename.txt>  
3. Each line of the record contains 100 bytes
4. So <no_of_Records> * 100 = size of file.
5. Example: ./gensort -a 10000000 pennyInput.txt --> will create a 1GB text file

The sorting is done on keys i.e. first 10 bytes of each records and then the corresponding value is appended.

Validating the Sorted output file:
----------------------------------
Using ./valsort <filename> you can test the sorted file
