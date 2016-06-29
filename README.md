# Database-Merging-SQLite3
This repository contains a Python script to merge SQLite database files. Databases should have identical tables and identical schemas of the tables. Data entries in the databases can be different, or identical.
  -Databases with mismatched tables will most likely NOT merge.
  -Databases that share the same table names, but with different column names/types will most likely NOT merge.
  
## Instructions
1. Make sure that you have Python 3.5.* installed.
2. Make sure that the script and the databases you wish to merge are all in the same folder.
3. Open the Python script, "mergeScript.py" in a text editor.
4. Navigate to the section of code labeled, "Input Parameters".
5. Change the value of the variable, "mainDB" with the name of your main database and its file extension. The value must be wrapped in quotes.
6. Add the names of the other databases to merge into the list variable titled, "otherDBs". Make sure that the values are wrapped in quotes and are comma separated. 
7. Open up the Python terminal and run the script.
