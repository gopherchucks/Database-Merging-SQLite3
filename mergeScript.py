################################################################################
# SQLite Merge Script                                                          #
#                                                                              #
# @author         Charles Duso                                                 #
# @description    Merges databases that have the same tables and schema.       #
# @date           August 7th, 2016                                              #
################################################################################

############################# Import Libraries #################################
################################################################################

import sqlite3
import time
import sys
from time import gmtime, strftime

############################ Global Variables ##################################
################################################################################

dbCount = 0                    # Variable to count the number of databases
listDB = []                    # Variable to store the names of the databases
listTable = []                 # Variable to store table names

############################ Function Definitions ##############################
################################################################################

# Attaches a database to the currently connected database
#
# @param dbName the name of the database file (i.e. "example.db")
# @return none
def attachDatabase( dbName ):
    global dbCount
    global listDB
    print(("Attaching database: %s") % dbName)
    curs.execute("ATTACH DATABASE ? as ? ;", (dbName, 'db' + str(dbCount)))
    listDB.append('db' + str(dbCount))
    dbCount += 1

# Closes the current database connection
#
# @return none
def closeConnection():
    curs.close()
    conn.close()

# Gets the table names of a database
#
# @param dbName the name of the database file (i.e. "example.db")
# @return a string array of the table names
def getTableNames():
    curs.execute("SELECT name FROM sqlite_master WHERE type='table';")
    temp = curs.fetchall()
    tables = []
    for i in range(0, len(temp)):
        tables.append(temp[i][0])
    return tables

# Gets the column names of a table
#
# @param dbName the name of the database file (i.e. "example.db")
# @return a string array of the column names - strips primary ids column
def getColumnNames( tableName ):
    curs.execute("PRAGMA table_info(%s);" % str(tableName))
    temp = curs.fetchall()
    columns = []
    for i in range(0, len(temp)):
        if (("id" in temp[i][1]) | ("ID" in temp[i][1])):
            continue
        else:
            columns.append(temp[i][1])
    return columns

# Compares two lists to see if they have identical data
#
# @param list1 the first list parameter for comparison
# @param list2 the second list parameter for comparison
# @return will return a boolean (0 lists !=, 1 lists ==)
def compareLists( list1, list2 ):
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
def listToString( listObj ):
    listString = ""
    for i in range(0, len(listObj)):
        if (i == (len(listObj) - 1)):
            listString = listString + listObj[i]
        else:
            listString = listString + listObj[i] + ", "
    return listString

# Merges a table from an attached database to the source table
#
# @param tableName the name of the table to merge
# @param columnNames the names of the columns to include in the merge
# @param dbNameTableName the name of the attached database and the table
#                        i.e. "databaseName.tableName"
# @return none
def mergeTable( tableName, columnNames, dbName ):
    dbNameTableName = dbName + "." + tableName
    try:
        curs.execute("INSERT INTO %s (%s) SELECT %s FROM %s;" %
                        (tableName, columnNames, columnNames, dbNameTableName))
        conn.commit()
    except:
        pass


############################## Input Parameters ################################
################################################################################

mainDB = ''     # This is where the main database is
                                     # referenced. Where all items will be
                                     # merged to.

otherDBs = []

if (len(otherDBs) == 0):
    print("ERROR: No databases have been added for merging.")
    sys.exit()

############################## Merge Script ####################################
################################################################################

# Initialize Connection and get main list of tables
conn = sqlite3.connect(mainDB)       # Connect to the main database
curs = conn.cursor()                 # Connect a cursor
listTable = getTableNames()          # Get the table names
closeConnection()

# Compare databases
startTime = time.time()
print("Comparing databases. Started at: " + strftime("%H:%M", gmtime()))

for i in range(0, len(otherDBs)):
    conn = sqlite3.connect(otherDBs[i])
    curs = conn.cursor()
    temp = getTableNames()              # Get the current list of tables
    if (len(listTable) > len(temp)):
        print("Table is missing from non-primary database: %s" % otherDBs[i])
        print("Database will NOT BE MERGED with the main database.")
        otherDBs.remove(otherDBs[i])    # Remove the table to avoid errors
        continue

    if (len(listTable) < len(temp)):
        print("Extra table(s) in non-primary database: %s" % otherDBs[i])
        print("TABLES that are NOT in main database will NOT be added.")

    if (listTable != temp):
        print("Tables do not match in non-primary database: %s" % otherDBs[i])
        print("The database will NOT BE MERGED with the main database.")
        otherDBs.remove(otherDBs[i])    # Remove the table to avoid errors
        continue

    closeConnection()

if (len(otherDBs) == 0):
    print("ERROR: No databases to merge. Databases were either removed due to \
          inconsistencies, or databases were not added properly.")
    sys.exit()

print("Finished comparing databases. Time elapsed: %.3f" % (time.time() -
                                                            startTime))

# Attach databases
startTime = time.time()
print("Merging databases. Started at: " + strftime("%H:%M", gmtime()))

conn = sqlite3.connect(mainDB)      # Attach main database
curs = conn.cursor()                # Attach cursor

for i in range(0, len(otherDBs)):
    attachDatabase(otherDBs[i])     # Attach other databases

# Merge databases
for i in range(0, len(listDB)):
    for j in range(0, len(listTable)):
        columns = listToString(getColumnNames(listTable[j])) # get columns
        mergeTable(listTable[j], columns, listDB[i])

conn.commit()                       # Commit changes
closeConnection()                   # Close connection
print("Databases finished merging. Time elapsed: %.3f" % (time.time() -
                                                        startTime))
