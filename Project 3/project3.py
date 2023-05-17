# Joshua Anthony Domantay
# Kevin Chaja
# COMP 467 - 21333
# 17 May 2023
# Import / export data

import os
import sys
import socket
from datetime import datetime
import argparse
import csv
import pymongo

work_folder = "import_files"

def read_file(file_name):
    iofile = open(os.path.join(work_folder, file_name), 'r')
    lines = iofile.readlines()
    iofile.close()
    return lines

def quicksort(arr, low, high):
    if low < high:
        mid = partition(arr, low, high)
        quicksort(arr, low, (mid - 1))
        quicksort(arr, (mid + 1), high)

def partition(arr, low, high):
    pivot = arr[high]
    i = low - 1
    for j in range(low, high):
        if(int(arr[j]) <= int(pivot)):
            i += 1
            temp = arr[i]
            arr[i] = arr[j]
            arr[j] = temp
    i += 1
    temp = arr[i]
    arr[i] = arr[high]
    arr[high] = temp
    return i

def get_xytech_info(file_name, verbose):
    # Get lines from xytech file
    lines = read_file(file_name)

    # Get info
    info = {
        "producer" : "",
        "operator" : "",
        "job" : "",
        "notes" : "",
    }
    server_paths = {}
    for i in range(len(lines)):
        if "/" in lines[i].strip().lower():     # Get xytech file paths
            server_paths[get_location_from_server(lines[i].strip())] = lines[i].strip()
            if verbose:
                print(lines[i].strip() + " -> " + get_location_from_server(lines[i].strip()))
        if "notes:" == lines[i].strip().lower():
            info["notes"] = lines[i + 1].strip()
            if verbose:
                print("notes ->", lines[i + 1].strip())
        elif ":" in lines[i]:       # Producer, operator, job
            line_info = lines[i].strip().split(":")
            if len(line_info[1]) == 0:      # No need to add location
                continue
            key = line_info[0].strip().lower()
            val = line_info[1].strip()
            info[key] = val
            if verbose:
                print(key, "->",  val)
    
    return info, server_paths

def get_location_from_server(job):
    index = job.index("production") + len("production") + 1
    return job[index:]

def process_work_files(work_files, server_paths, verbose):
    data = []
    new_data = []
    for work_file in work_files:
        new_data = get_work_info(work_file)
        for i in new_data:
            if verbose:
                print(i[0], "->", i[1], "->", i[2])
            data.append(i)
    
    # Switch local paths to server paths
    for work in data:
        work[1] = get_server_path(work[1], server_paths, verbose)

    return data

def get_work_info(file_name):
    # Get lines from work file
    lines = read_file(file_name)
    file_type = file_name.split("_")[0].lower()
    
    # Read each line
    data = []
    for line in lines:
        line_info = line.strip().split(" ")     # TODO: Does not take into account if file or folder has space
        if line_info[0] != "":
            new_row = []
            frames = []

            new_row.append("_".join(file_name.split("_")[1:]))      # Get user on file
            if(file_type == "baselight"):
                new_row.append(line_info[0])        # Get path
                old_frames = line_info[1:]
            else:       # Flame
                new_row.append(line_info[1])        # Get path. No need to get storage from flame
                old_frames = line_info[2:]

            for frame in old_frames:    # Read frames
                if frame.isdigit():     # Avoid <err> or <null>
                    frames.append(frame)
            new_row.append(frames)
            data.append(new_row)
    return compress_frames(data)

def compress_frames(data):
    for i in range(len(data)):
        # Sort first just in case
        frames = data[i][2]
        quicksort(frames, 0, (len(frames) - 1))

        new_frames = []
        start_frame = frames[0]
        last_frame = frames[0]
        for frame in frames:
            if((int(frame) - int(last_frame)) > 1):     # Difference with last frame is greater than 1 = not consecutive
                if(start_frame != last_frame):
                    new_frames.append(start_frame + "-" + last_frame)       # Range
                else:
                    new_frames.append(start_frame)      # No consecutive frame
                start_frame = frame
            last_frame = frame
        if(start_frame != last_frame):
            new_frames.append(str(start_frame) + "-" + str(last_frame))     # Range
        else:
            new_frames.append(start_frame)      # No consecutive frame
        data[i][2] = new_frames
    return data

def get_server_path(local_path, server_paths, verbose):
    split_path = local_path.split("/")
    for i in range(len(split_path)):
        processed_path = '/'.join(split_path[i:])
        if server_paths.get(processed_path):
            if verbose:
                print(local_path, "-> local path to server path ->", server_paths.get(processed_path))
            return server_paths.get(processed_path)
    if verbose:
        print("ERROR translating to server path ->", local_path)
    return local_path       # Should not be reached

def validate_args(args):
    # Check "work" folder (import_files)
    if not os.path.exists(work_folder):
        print("Folder " + work_folder + " does not exist")
        return 2
    
    # Prioritize video to process
    # Check if video file exists
    if args.video is not None:
        if not os.path.exists(os.path.join(work_folder, args.video)):
            print("Video does not exist")
            return 2
        else:
            return 0

    # Check work files
    if args.workFiles is None:
        print("No work file selected")
        return 2
    else:
        for workFile in args.workFiles:
            if not os.path.exists(os.path.join(work_folder, workFile)):
                print("Work file is missing", workFile)
                return 2

    # Check Xytech file
    if args.xytechFile is None:
        print("No Xytech file selected")
        return 2
    else:
        if not os.path.exists(os.path.join(work_folder, args.xytechFile)):
            print("Xytech file is missing")
            return 2

    # Check output
    if args.output is None:
        print("No output selected")
        return 2
    else:
        if((args.output.lower() != "csv") and (args.output.lower() != "database") and (args.output.lower() != "db")):
            print("Selected output is invalid. Use: 'csv', 'database', or 'db'")
            return 2
    
    return 0

def write_to_csv(xytech_info, jobs, verbose):
    csv_file_name = "output.csv"
    with open(csv_file_name, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(xytech_info.values())
        if verbose:
            print("Write to csv ->", xytech_info.values())
        writer.writerow([])
        if verbose:
            print("Write to csv ->")
        writer.writerow([])
        if verbose:
            print("Write to csv ->")
        for job in jobs:
            for frames in job[2]:
                writer.writerow([job[1], frames])
                if verbose:
                    print("Write to csv ->", job[1], "=", frames)
    csv_file.close()

def write_to_db(xytech_info, jobs, work_files, verbose):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["project2"]

    # First collection: <user that ran script> <machine> <name of user on file> <date of file> <submitted date>
    col = db["script_users"]
    to_add = []
    for work_file in work_files:
        work_file_content = work_file.split("_")
        info = {
            "user" : socket.gethostname(),
            "machine" : work_file_content[0],
            "user_on_file" : work_file_content[1],
            "date_of_file" : int(work_file_content[2].split(".")[0]),
            "submitted_date" : datetime.today().strftime("%Y%m%d")
        }
        to_add.append(info)
        if verbose:
            print("Write to MongoDB (db = project2, col = script_users)->", info)
    col.insert_many(to_add)

    # Second collection: <name of user on file> <date of file> <location> <frame/ranges>
    col = db["jobs"]
    to_add = []
    for job in jobs:
        for frames in job[2]:
            info = {
                "user_on_file" : job[0].split("_")[0],
                "date_of_file" : int(job[0].split("_")[1].split(".")[0]),
                "location" : job[1],
                "frames" : frames
            }
            to_add.append(info)
            if verbose:
                print("Write to MongoDB (db = project2, col = jobs)->", info)
    col.insert_many(to_add)

def workflow(args):
    xytech_info, xytech_paths = get_xytech_info(args.xytechFile, args.verbose)
    jobs = process_work_files(args.workFiles, xytech_paths, args.verbose)

    if args.output.lower() == "csv":
        write_to_csv(xytech_info, jobs, args.verbose)
    else:
        write_to_db(xytech_info, jobs, args.workFiles, args.verbose)

def process_video(args):
    print("Hello")

def main(args):
    valid_args = validate_args(args)
    if(valid_args != 0):
        return valid_args

    if args.video is None:
        workflow(args)
    else:
        process_video(args)

    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs="+", dest="workFiles", help="files to process")        # Instead of *, use + because it requires at least 1 file
    parser.add_argument("--xytech", dest="xytechFile", help="xytech file to process")
    parser.add_argument("--verbose", action="store_true", help="show verbose")
    parser.add_argument("--output", dest="output", help="output to csv or database")
    parser.add_argument("--process", dest="video", help="video to process")
    args = parser.parse_args()
    sys.exit(main(args))
