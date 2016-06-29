################################################################################
# SQLite Merge Script                                                          #
#                                                                              #
# @author         Charles Duso                                                 #
# @description    Merges databases that have the same tables and schema.       #
# @date           June 28th, 2016                                              #
################################################################################

############################# Import Libraries #################################
################################################################################

import sqlite3

############################ Global Variables ##################################
################################################################################

dbCount = 1                # Variable to count the number of databases

############################ Function Definitions ##############################
################################################################################

# Attaches a database to the currently connected database
#
# @param dbName the name of the database file (i.e. "example.db")
# @return none
def attachDatabase( dbName ):
    curs.execute("ATTACH DATABASE ? as ? ;", (dbName, 'db' + str(dbCount)))
    dbCount++

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
# @return a string array of the column names - strips primary ids  
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
def compareLists(list1, list2):
    if len(list1) != len(list2):
        return 0
    else:
        for i in range(0, len(list1)):
            if list1[i] != list2[i]:
                return 0
    return 1

# Merges a table from an attached database to the source table
#
# @param tableName the name of the table to merge
# @param columnNames the names of the columns to include in the merge
# @param dbNameTableName the name of the attached database and the table
#                        i.e. "databaseName.tableName"
# @return none
def mergeTable(tableName, columnNames, dbNameTableName):
    curs.execute("INSERT INTO %s (%s) SELECT %s FROM %s;" %
                 (tableName, columnNames, columnNames, dbNameTableName))
    conn.commit()
    

############################## Merge Script ####################################
################################################################################

# Create the initial database connection - everything will be merged to here
conn = sqlite3.connect('') # Enter the name of the database
curs = conn.cursor()       # Creates a cursor for use on the database

# Attach databases

attachDatabase('')         # Enter the name of the database (i.e. "example.db")
attachDatabase('')
attachDatabase('')
attachDatabase('')
