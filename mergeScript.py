################################################################################
# SQLite Merge Script                                                          #
#                                                                              #
# @author         Charles Duso                                                 #
# @description    Merges databases that have the same tables and schema.       #
# @date           June 29th, 2016                                              #
################################################################################

############################# Import Libraries #################################
################################################################################

import sqlite3
import time
from time import gmtime, strftime

############################ Global Variables ##################################
################################################################################

dbCount = 0                    # Variable to count the number of databases                
listDB = []                    # Variable to store the names of the databases

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
        if ((("id" in temp[i][1]) | ("ID" in temp[i][1])) & ( \
            "INTEGER" in temp[i][2])):
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
    curs.execute("INSERT INTO %s (%s) SELECT %s FROM %s;" %
                 (tableName, columnNames, columnNames, dbNameTableName))
    conn.commit()


############################## Input Parameters ################################
################################################################################

# Create the initial database connection - everything will be merged to here
conn = sqlite3.connect('testDB_1.db') # Enter the name of the database
curs = conn.cursor()                # Creates a cursor for use on the database

# Attach databases
attachDatabase('testDB_2.db')       # Enter the name of the database
                                    # (i.e. "example.db")
#attachDatabase('')
#attachDatabase('')
#attachDatabase('')

############################## Merge Script ####################################
################################################################################

# Compare databases
startTime = time.time()
print("Comparing databases. Started at: " + strftime("%H:%M", gmtime()))
#
#
#
print("Finished comparing databases. Time elapsed: %.3f" % (time.time() - 
                                                            startTime))

# Merge databases
startTime = time.time()
print("Merging databases. Started at: " + strftime("%H:%M", gmtime()))
#
#
#
conn.commit()
closeConnection()
print("Databases finished merging. Time elapsed: %.3f" % (time.time() -
                                                        startTime))


