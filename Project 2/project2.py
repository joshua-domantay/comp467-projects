# Joshua Anthony Domantay
# Kevin Chaja
# COMP 467 - 21333
# 5 March 2023
# Import / export data

import os
import sys
import argparse
import csv

work_folder = "import_files"

def read_file(filename):
    iofile = open(os.path.join(work_folder, filename), 'r')
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

def get_xytech_info(filename):
    # Get lines from xytech file
    lines = read_file(filename)

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
        if "notes:" == lines[i].strip().lower():
            info["notes"] = lines[i + 1].strip()
        elif ":" in lines[i]:       # Producer, operator, job
            line_info = lines[i].strip().split(":")
            if len(line_info[1]) == 0:      # No need to add location
                continue
            key = line_info[0].strip().lower()
            val = line_info[1].strip()
            info[key] = val
    
    return info, server_paths

def get_location_from_server(job):
    index = job.index("production") + len("production") + 1
    return job[index:]

# def process_work_files(work_files):
#     data = []
#     new_data = []
#     for work_file in args.workFiles:
#         new_data = []
#         if work_file.split("_")[0].lower() == "baselight":

def get_baselight_info(job, filename):
    # Get lines from baselight file
    lines = read_file(filename)
    
    # Read each line
    data = []
    for line in lines:
        line_info = line.strip().split(" ")     # TODO: Does not take into account if file or folder has space
        if line_info[0] != "":
            new_row = []
            new_row.append(line_info[0].split(job)[1])      # Get path and remove local storage path
            frames = []
            for frame in line_info[1:]:     # Read frames
                if frame.isdigit():     # Avoid <err> or <null>
                    frames.append(frame)
            new_row.append(frames)
            data.append(new_row)
    return compress_baselight_data(data)

def compress_baselight_data(data):
    for i in range(len(data)):
        # Sort first just in case
        frames = data[i][1]
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
        data[i][1] = new_frames
    return data

def add_sans_path_to_frames(job, path, job_and_frames):
    for i in range(len(job_and_frames)):
        job_and_frames[i][0] = path + job + job_and_frames[i][0]

def validate_args(args):
    # Check "work" folder (import_files)
    if not os.path.exists(work_folder):
        print("Folder " + work_folder + " does not exist")
        return 2

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
        if not os.path.exists(os.path.join(work_folder, workFile)):
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

def main(args):
    valid_args = validate_args(args)
    if(valid_args != 0):
        return valid_args

    xytech_info, xytech_paths = get_xytech_info(args.xytechFile)
    #job_and_frames = get_baselight_info(args.jobFolder, baselight_filename)
    # add_sans_path_to_frames(args.jobFolder, sans_path, job_and_frames)

    csv_filename = "test" + ".csv"
    with open(csv_filename, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(xytech_info.values())
        writer.writerow([])
        writer.writerow([])
        # for work in job_and_frames:
        #     for frames in work[1]:
        #         writer.writerow([work[0], frames])
    csv_file.close()
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", nargs="+", dest="workFiles", help="files to process")        # Instead of *, use + because it requires at least 1 file
    parser.add_argument("--xytech", dest="xytechFile", help="xytech file to process")
    parser.add_argument("--verbose", action="store_true", help="show verbose")
    parser.add_argument("--output", dest="output", help="output to csv or database")
    args = parser.parse_args()
    sys.exit(main(args))
