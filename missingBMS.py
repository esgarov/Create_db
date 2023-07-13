import os
import sqlite3
import hashlib

def check_type():
    conn = sqlite3.connect('symBlobsMpk.db')
    cursor = conn.cursor()

    cursor.execute("SELECT user_ocis_name, user_ocis_type, blobid, blobsize, parentid, mpk_name FROM mpk")
    mpk_entries = cursor.fetchall()

    blobs_counter = 0
    error_messages = []
    directory_counter = 0
    
    for entry in mpk_entries:
        user_ocis_name, user_ocis_type, blobid, blobsize, parentid, mpk_name = entry
        if user_ocis_type == "1":
            is_blob, err = check_blobs(entry, cursor)
            blobs_counter += is_blob
            if err is not None:
                error_messages.append(err)
        elif user_ocis_type == "2":
            is_dir = check_directory(entry, cursor)
            directory_counter += is_dir
            
        else:
            print(f"Unknown user_ocis_type: {user_ocis_type}")
            
    conn.commit()
    
    blobs_result = {"counter": blobs_counter, "error": None}
    directory_result = {"counter": directory_counter, "error": None}
    cursor.execute("SELECT * FROM blobs WHERE seen = 0")
    unseen_blobs = cursor.fetchall()
    for unseen in unseen_blobs:
        error_messages.append(f"Blob without MPK: {unseen[2]}")

    cursor.close()
    conn.close()

    if error_messages:
        blobs_result["error"] = error_messages

    return blobs_result, directory_result
    
def check_directory(entry, cursor):
    user_ocis_name, user_ocis_type, blobid, blobsize, parentid, mpk_name = entry
    if blobsize != "":
        print(f"user_ocis_type is 2, but blobsize {blobsize} is not empty in mpk {mpk_name}")
        return 0
    if blobid != "":
        print(f"user_ocis_type is 2, but blobid {blobid} is not empty in mpk {mpk_name}")
        return 0
        
    return 1
        
def check_blobs(entry, cursor):

        user_ocis_name, user_ocis_type, blobid, blobsize, parentid, mpk_name = entry

        blob_name = blobid

        cursor.execute("SELECT path, size, sha1, md5 FROM blobs WHERE name = ?", (blob_name,))
        blob_entry = cursor.fetchone()
        
        
        if blob_entry is None:
            
            return 0, f"Error: Blob name not found - {blob_name} in mpk {mpk_name}"
        else:
            

            cursor.execute("UPDATE blobs SET seen = 1 WHERE name = ?", (blob_name,))

            blob_path = blob_entry[0]

            sha1_hash = hashlib.sha1()
            with open(blob_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha1_hash.update(chunk)
            sha1 = sha1_hash.hexdigest()

            md5_hash = hashlib.md5()
            with open(blob_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    md5_hash.update(chunk)
            md5 = md5_hash.hexdigest()

            if blob_entry[1] != int(blobsize):
                error_messages.append(f"Error: Blob size mismatch - {blob_name} {blob_entry[1]} {blobsize}")

            if blob_entry[2] != sha1:
                error_messages.append(f"Error: Blob sha1 mismatch - {blob_name}")

            if blob_entry[3] != md5:
                error_messages.append(f"Error: Blob md5 mismatch - {blob_name}")
        
            return 1, None
    
    



def check_mpks():
    conn = sqlite3.connect('symBlobsMpk.db')
    cursor = conn.cursor()

    cursor.execute("SELECT target FROM symlinks")
    symlink_entries = cursor.fetchall()

    mpk_counter = 0
    noname_counter = 0
    error_messages = []

    for entry in symlink_entries:
        target = entry[0]

        if target is None:
            continue

        mpk_name = os.path.basename(target) + '.mpk'

        cursor.execute("SELECT mpk_name FROM mpk WHERE mpk_name = ?", (mpk_name,))
        mpk_entry = cursor.fetchone()

        if mpk_entry is None:
            error_messages.append(f"Error: MPK name not found - {mpk_name}")
        else:
            mpk_counter += 1

            cursor.execute("UPDATE mpk SET seen = 1 WHERE mpk_name = ?", (mpk_name,))

    conn.commit()
    cursor.execute("SELECT mpk_name, user_ocis_name FROM mpk WHERE seen = 0")
    unseen_mpks = cursor.fetchall()
    for unseen in unseen_mpks:
        if unseen[1] == '':
            noname_counter += 1
        else:
            error_messages.append(f"MPK without Symlink: {unseen[0]} - symlink name should be: {unseen[1]}")

    mpks_result = {"counter": mpk_counter, "error": None, "nonameCounter": noname_counter}

    cursor.close()
    conn.close()

    if error_messages:
        mpks_result["error"] = error_messages

    return mpks_result


def check_symlinks():
    conn = sqlite3.connect('symBlobsMpk.db')
    cursor = conn.cursor()

    cursor.execute("SELECT mpk_path, parentid, user_ocis_name FROM mpk")
    mpk_entries = cursor.fetchall()

    symlink_counter = 0
    error_messages = []

    for entry in mpk_entries:
        mpk_path = entry[0]
        parentid = entry[1]
        user_ocis_name = entry[2]

        mpk_path = mpk_path[:-4]

        mpk_nodes_part, mpk_remaining_part = mpk_path.split('/nodes/', 1)

        parentid_first8 = parentid[:8]
        parentid_rest = parentid[8:]

        parentid_parts = [parentid_first8[i:i+2] for i in range(0, 8, 2)]
        parentid_dirs = '/'.join(parentid_parts)

        symlink_name = os.path.join('/var/lib/ocis/storage/users/spaces', mpk_nodes_part, 'nodes', parentid_dirs, parentid_rest, user_ocis_name)

        cursor.execute("SELECT name FROM symlinks WHERE name = ?", (symlink_name,))
        symlink_entry = cursor.fetchone()

        if symlink_entry is not None:
            symlink_counter += 1

            cursor.execute("UPDATE symlinks SET seen = 1 WHERE name = ?", (symlink_name,))
            conn.commit()

        #print(symlink_name)

    symlink_result = {"counter": symlink_counter, "error": None}
    cursor.execute("SELECT name FROM symlinks WHERE seen = 0")
    unseen_symlinks = cursor.fetchall()
    for symlink in unseen_symlinks:
        error_messages.append(f"Unseen symlink: {symlink[0]}")

    cursor.close()
    conn.close()

    if error_messages:
        symlink_result["error"] = error_messages

    return symlink_result


# Calling the functions and printing the results

blobs_result, directory_result = check_type()
if blobs_result["error"]:
    for error in blobs_result["error"]:
        print(error)
else:
    print(f"blobsCounter: {blobs_result['counter']}")

print()

if directory_result["error"]:
    for error in directory_result["error"]:
        print(error)
else:
    print(f"directoryCounter: {directory_result['counter']}")

print()
mpks_result = check_mpks()
if mpks_result["error"]:
    for error in mpks_result["error"]:
        print(error)
else:
    print(f"mpkCounter: {mpks_result['counter']}")
    print(f"nonameCounter: {mpks_result['nonameCounter']}")

print()

symlink_result = check_symlinks()
if symlink_result["error"]:
    for error in symlink_result["error"]:
        print(error)
else:
    print(f"symlinkCounter: {symlink_result['counter']}")

# Comparing the counters and printing errors if they are not equal

counters = [blobs_result['counter'], mpks_result['counter'], symlink_result['counter']]
if len(set(counters)) == 1:
    print(f"All counters are equal. {mpks_result['counter']}")

else:
    print("Counters are not equal:")
    print(f"blobsCounter: {blobs_result['counter']}")
    print(f"mpkCounter: {mpks_result['counter']}")
    print(f"symlinkCounter: {symlink_result['counter']}")
