# symBlobsMpk Check

This Python script is used for checking consistency in the SQLite database and on the filesystem for specific entries. It verifies the consistency of blobs, directories, MPKs (MetaPack files), and symlinks.

## How it Works

The script works by opening an SQLite database named `symBlobsMpk.db` and fetching entries from different tables such as `mpk`, `blobs`, and `symlinks`. It then checks each entry and validates it according to the type. Errors are collected in a list which are then printed out.

## Main Functions

- `check_type()`: This function checks the type of the mpk entries and calls the respective function (`check_blobs()` or `check_directory()`) based on the type. The function returns counters and errors for blobs and directories.

- `check_directory()`: This function checks if the directory entries in the mpk table are consistent.

- `check_blobs()`: This function checks if the blob entries in the mpk table are consistent with the blob entries in the blobs table and the actual file in the filesystem.

- `check_mpks()`: This function checks if the symlink entries in the symlinks table are consistent with the entries in the mpk table.

- `check_symlinks()`: This function checks if the mpk entries in the mpk table are consistent with the symlink entries in the symlinks table.

## Running the Script

Make sure you have Python installed on your machine and run the script using the command:

```sh
python3 missingBMS.py
