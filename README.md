# Create_db

create_db.py is a Python script that creates an SQLite database (symBlobsMpk.db) and uses it to store and analyze information about files in a directory and its subdirectories.

The script does the following:

    Searches for 'blobs' and 'nodes' directories in the specified directory and its subdirectories.
    Checks if the SQLite database symBlobsMpk.db exists, and creates it if not.
    Connects to the SQLite database and creates three tables: blobs, mpk, and symlinks, dropping them first if they already exist.
    Processes each file in the 'blobs' and 'nodes' directories. For each file, it calculates the SHA-1 and MD5 hashes, and stores relevant file information in the database.
    For 'nodes' directory, it unpacks the .mpk files using msgpack, extracts relevant information, and stores it in the mpk table of the database.
    Handles symbolic links, inserting the information into the symlinks table of the database.
    At the end, it closes the connection to the SQLite database.

Usage

bash: python create_db.py /path/to/your/directory

Requirements

    Python 3.7 or higher
    os, sys, sqlite3, hashlib, msgpack Python libraries

Note

The script creates an SQLite database named symBlobsMpk.db in the same directory where the script is executed. Ensure that you have the necessary permissions to create and write to this file in that directory.
