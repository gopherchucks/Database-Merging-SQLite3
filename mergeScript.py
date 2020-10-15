################################################################################
# SQLite Merge Script                                                          #
#                                                                              #
# @author         Charles Duso                                                 #
# @description    Merges databases that have the same tables and schema.       #
# @date           August 7th, 2016                                             #
################################################################################

############################# Import Libraries #################################
################################################################################

import sqlite3
import time
import sys
from time import gmtime, strftime

############################ Global Variables ##################################
################################################################################

dbCount = 0  # Variable to count the number of databases
listDB = []  # Variable to store the names of the databases
listTable = []  # Variable to store table names


############################ Function Definitions ##############################
################################################################################

# Attaches a database to the currently connected database
#
# @param db_name the name of the database file (i.e. "example.db")
# @return none
def attach_database(db_name):
    global dbCount
    global listDB
    print("Attaching database: %s" % db_name)
    curs.execute("ATTACH DATABASE ? as ? ;", (db_name, 'db' + str(dbCount)))
    listDB.append('db' + str(dbCount))
    dbCount += 1


# Closes the current database connection
#
# @return none
def close_connection():
    curs.close()
    conn.close()


# Gets the table names of a database
#
# @param db_name the name of the database file (i.e. "example.db")
# @return a string array of the table names
def get_table_names():
    curs.execute("SELECT name FROM sqlite_master WHERE type='table';")
    temp = curs.fetchall()
    tables = []
    for i in range(0, len(temp)):
        tables.append(temp[i][0])
    return tables


# Gets the column names of a table
#
# @param db_name the name of the database file (i.e. "example.db")
# @return a string array of the column names - strips primary ids column
def get_column_names(table_name):
    curs.execute("PRAGMA table_info(%s);" % str(table_name))
    temp = curs.fetchall()
    columns = []
    for i in range(0, len(temp)):
        if ("id" in temp[i][1]) | ("ID" in temp[i][1]):
            continue
        else:
            columns.append(temp[i][1])
    return columns


# Compares two lists to see if they have identical data
#
# @param list1 the first list parameter for comparison
# @param list2 the second list parameter for comparison
# @return will return a boolean (0 lists !=, 1 lists ==)
def compare_lists(list1, list2):
    if len(list1) != len(list2):
        return 0
    else:
        for i in range(0, len(list1)):
            if list1[i] != list2[i]:
                return 0
    return 1


# Converts a list of string objects to a string of comma separated items.
#
# @param listObj the list to convert
# @return a string containing the list items - separated
#         by commas.
def list_to_string(list_obj):
    list_string = ""
    for i in range(0, len(list_obj)):
        if i == (len(list_obj) - 1):
            list_string = list_string + list_obj[i]
        else:
            list_string = list_string + list_obj[i] + ", "
    return list_string


# Merges a table from an attached database to the source table
#
# @param table_name the name of the table to merge
# @param column_names the names of the columns to include in the merge
# @param db_name_table_name the name of the attached database and the table
#                        i.e. "db_name.table_name"
# @return none
def merge_table(table_name, column_names, db_name):
    db_name_table_name = db_name + "." + table_name
    try:
        curs.execute("INSERT INTO %s (%s) SELECT %s FROM %s;" %
                     (table_name, column_names, column_names, db_name_table_name))
        conn.commit()
    except:
        pass


############################## Input Parameters ################################
################################################################################

mainDB = ''     # This is where the main database is
# referenced. Where all items will be
# merged to.

otherDBs = []

if len(otherDBs) == 0:
    print("ERROR: No databases have been added for merging.")
    sys.exit()

############################## Merge Script ####################################
################################################################################

# Initialize Connection and get main list of tables
conn = sqlite3.connect(mainDB)  # Connect to the main database
curs = conn.cursor()  # Connect a cursor
listTable = get_table_names()  # Get the table names
close_connection()

# Compare databases
startTime = time.time()
print("Comparing databases. Started at: " + strftime("%H:%M", gmtime()))

for i in range(0, len(otherDBs)):
    conn = sqlite3.connect(otherDBs[i])
    curs = conn.cursor()
    temp = get_table_names()  # Get the current list of tables
    if len(listTable) > len(temp):
        print("Table is missing from non-primary database: %s" % otherDBs[i])
        print("Database will NOT BE MERGED with the main database.")
        otherDBs.remove(otherDBs[i])  # Remove the table to avoid errors
        continue

    if len(listTable) < len(temp):
        print("Extra table(s) in non-primary database: %s" % otherDBs[i])
        print("TABLES that are NOT in main database will NOT be added.")

    if listTable != temp:
        print("Tables do not match in non-primary database: %s" % otherDBs[i])
        print("The database will NOT BE MERGED with the main database.")
        otherDBs.remove(otherDBs[i])  # Remove the table to avoid errors
        continue

    close_connection()

if len(otherDBs) == 0:
    print("ERROR: No databases to merge. Databases were either removed due to \
          inconsistencies, or databases were not added properly.")
    sys.exit()

print("Finished comparing databases. Time elapsed: %.3f" % (time.time() -
                                                            startTime))

# Attach databases
startTime = time.time()
print("Merging databases. Started at: " + strftime("%H:%M", gmtime()))

conn = sqlite3.connect(mainDB)  # Attach main database
curs = conn.cursor()  # Attach cursor

for i in range(0, len(otherDBs)):
    attach_database(otherDBs[i])  # Attach other databases

# Merge databases
for i in range(0, len(listDB)):
    for j in range(0, len(listTable)):
        columns = list_to_string(get_column_names(listTable[j]))  # get columns
        merge_table(listTable[j], columns, listDB[i])

conn.commit()  # Commit changes
close_connection()  # Close connection
print("Databases finished merging. Time elapsed: %.3f" % (time.time() -
                                                          startTime))
