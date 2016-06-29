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
# @return a string array of the table names and their respective column names
def getColumnNames( tableName ):
    curs.execute("PRAGMA table_info(%s);" % str(tableName))
    temp = curs.fetchall()
    columns = []
    for i in range(0, len(temp)):
        columns.append(temp[i][1])
    return columns
    

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
