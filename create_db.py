import os
import sys
import sqlite3
import hashlib
import msgpack

directory = sys.argv[1]

# Function to recursively search for the 'blobs' directory
def find_blobs_directory(directory):
    blobs_directories = []
    for root, dirs, files in os.walk(directory):
        rdirs = root.split(os.path.sep)
        if not 'metadata' in rdirs and rdirs[-1] == 'blobs':
            blobs_directories.append(root)
            dirs[:] = []  # inplace modification to prune os.walk
    return blobs_directories

def find_mpk_directories(directory):
    mpk_directories = []
    for root, dirs, files in os.walk(directory):
        rdirs = root.split(os.path.sep)
        if not 'metadata' in rdirs and rdirs[-1] == 'nodes':
            mpk_directories.append(root)
            dirs[:] = []
    return mpk_directories

# Check if the database file already exists
if not os.path.exists('symBlobsMpk.db'):
    # Create a SQLite database if it doesn't exist
    conn = sqlite3.connect('symBlobsMpk.db')
    conn.close()

# Connect to the SQLite database
conn = sqlite3.connect('symBlobsMpk.db')
c = conn.cursor()

# Check if the 'blobs' table exists and drop it if it does
c.execute("DROP TABLE IF EXISTS blobs")

# Create a new table to store blobs information
c.execute('''CREATE TABLE blobs
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             name TEXT,
             path TEXT,
             size INTEGER,
             sha1 TEXT,
             md5 TEXT,
             seen INTEGER DEFAULT 0)''')

# Check if the 'mpk' table exists and drop it if it does
c.execute("DROP TABLE IF EXISTS mpk")

# Create a new table to store mpk information
c.execute('''CREATE TABLE mpk
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             mpk_name TEXT,
             mpk_path TEXT,
             user_ocis_name TEXT,
             user_ocis_type TEXT,
             blobsize TEXT,
             blobid TEXT,
             parentid TEXT,
             sha1 TEXT,
             md5 TEXT,
             seen INTEGER DEFAULT 0)''')

# Check if the 'symlinks' table exists and drop it if it does
c.execute("DROP TABLE IF EXISTS symlinks")

# Create a new table to store symlink information
c.execute('''CREATE TABLE symlinks
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             name TEXT,
             target TEXT,
             seen INTEGER DEFAULT 0)''')

# Function to process MPK files and insert information into the database
def process_mpk(filepath):
    # Read and unpack the MPK file
    with open(filepath, 'rb') as f:
        mpk_data = f.read()

    mpk_dict = msgpack.unpackb(mpk_data, raw=False)
    print(mpk_dict)

    # Check if the unpacked dictionary has a ocis.type
    if 'user.ocis.type' in mpk_dict:
        # Get the blobsize, blobid, and parentid values from the dictionary
        blobsize = mpk_dict.get('user.ocis.blobsize', b'')
        blobid = mpk_dict.get('user.ocis.blobid', b'')  # Provide a default value if not present
        parentid = mpk_dict.get('user.ocis.parentid', b'')  # Provide a default value if not present
        user_ocis_name = mpk_dict.get('user.ocis.name', b'')
        user_ocis_type = mpk_dict.get('user.ocis.type', b'')
        
        # Convert blobid to string
        blobid = blobid.decode()

        # Calculate the SHA-1 hash of the MPK file
        sha1_hash = hashlib.sha1(mpk_data).hexdigest()

        # Calculate the MD5 hash of the MPK file
        md5_hash = hashlib.md5(mpk_data).hexdigest()

        # Convert the data to strings
        mpk_path = filepath
        mpk_name = os.path.basename(filepath)

        user_ocis_name = user_ocis_name.decode()
        user_ocis_type = user_ocis_type.decode()
        blobsize = blobsize.decode()
        parentid = parentid.decode()
        try:
            sha1_hash = sha1_hash.decode()
        except:
            pass
        try:
            md5_hash = md5_hash.decode()
        except:
            pass
        seen = 0

        # Insert mpk information into the database
        c.execute(
            "INSERT INTO mpk (mpk_name, mpk_path, user_ocis_name, user_ocis_type, blobsize, blobid, parentid, sha1, md5, seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (mpk_name, mpk_path, user_ocis_name, user_ocis_type, blobsize, blobid, parentid, sha1_hash, md5_hash, seen))
        conn.commit()
    else:
        print(filepath, "Mpk without Blobs Size")

    # Check if the file is a revision file
    if '.REV.' in filepath:
        # Unpack the MPK file as before
        mpk_dict = msgpack.unpackb(mpk_data, raw=False)

        # Check for a blob id in the MPK file
        if 'user.ocis.blobid' in mpk_dict:
            # Get the blob id
            blobid = mpk_dict.get('user.ocis.blobid', b'').decode()

            # Search for the blob in the 'blobs' table
            c.execute("SELECT * FROM blobs WHERE name=?", (blobid,))
            blob = c.fetchone()

            # If the blob is not found, print an error message
            if blob is None:
                print(f"BLOB NOT FOUND FOR {filepath}")

            else:
                print(f"BLOB FOUND FOR {filepath}")

        else:
            print(f"NO BLOB ID FOUND IN {filepath}")
    else:
        print(filepath, "Mpk without Blobs Size")

# Function to process symlink files and insert information into the database
def process_symlink(filepath):
    # check if the parent directory is a symlink
    parent_directory = os.path.dirname(filepath)
    if os.path.islink(parent_directory):
        # if the parent directory is a symlink, skip processing this symlink
        return

    link_name = os.path.basename(filepath)
    original_path = os.readlink(filepath)
    absolute_path = os.path.abspath(os.path.join(os.path.dirname(filepath), original_path))
    print("Link Name:", link_name)
    print("Original Path:", original_path)
    print("Absolute Path:", absolute_path)

    # Insert symlink information into the database
    c.execute("INSERT INTO symlinks (name, target, seen) VALUES (?, ?, ?)",
              (filepath, absolute_path, 0))
    conn.commit()



# Find the 'blobs' directory
blobs_directories = find_blobs_directory(directory)

# Check if the 'blobs' directory was found
if blobs_directories:
    for blobs_directory in blobs_directories:
        for root, dirs, files in os.walk(blobs_directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                if os.path.isfile(filepath):
                    # Get the blob name by removing the blobs directory from the filepath
                    blob_name = os.path.relpath(filepath, start=blobs_directory).replace("/", "")

                    blob_path = os.path.abspath(filepath)

                    # Get the size of the blob using os.stat()
                    blob_size = os.stat(filepath).st_size

                    # Calculate the SHA-1 hash of the blob
                    sha1_hash = hashlib.sha1()
                    with open(filepath, 'rb') as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            sha1_hash.update(chunk)
                    sha1 = sha1_hash.hexdigest()

                    # Calculate the MD5 hash of the blob
                    md5_hash = hashlib.md5()
                    with open(filepath, 'rb') as f:
                        for chunk in iter(lambda: f.read(4096), b""):
                            md5_hash.update(chunk)
                    md5 = md5_hash.hexdigest()

                    # Insert blob information into the database
                    c.execute("INSERT INTO blobs (name, path, size, sha1, md5, seen) VALUES (?, ?, ?, ?, ?, ?)",
                              (blob_name, blob_path, blob_size, sha1, md5, 0))
                    conn.commit()

else:
    print("The 'blobs' directory was not found.")
    
mpk_directories = find_mpk_directories(directory)
for mpk_directory in mpk_directories:
    for root, dirs, files in os.walk(mpk_directory, followlinks=True):
        for filename in files:
            filepath = os.path.join(root, filename)
            if filepath.endswith(".mpk"):
                process_mpk(filepath)
            elif os.path.islink(filepath):
                process_symlink(filepath)
        for dirname in dirs:
            dirpath = os.path.join(root, dirname)
            if os.path.islink(dirpath):
                process_symlink(dirpath)




# Close the database connection
conn.close()
