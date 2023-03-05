# Joshua Anthony Domantay
# Kevin Chaja
# COMP 467 - 21333
# 5 March 2023
# Import / export data

import sys
import argparse
import csv

def read_file(filename):
    iofile = open(filename, 'r')
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

    # Get header and locations info
    header = {
        "producer" : "",
        "operator" : "",
        "job" : "",
        "notes" : ""
    }
    locations = []
    for i in range(len(lines)):
        if "location:" == lines[i].strip().lower():
            i += 1
            while lines[i].strip() != "":       # Add to locations until newline
                locations.append(lines[i].strip())
                i += 1
        if "notes:" == lines[i].strip().lower():
            header["notes"] = lines[i + 1].strip()
        elif ":" in lines[i]:
            line_info = lines[i].strip().split(":")
            key = line_info[0].lower()
            val = line_info[1].strip()
            header[key] = val
    
    return header.values(), locations

def get_baselight_info(job, filename):
    # Get lines from baselight file
    lines = read_file(filename)

    # Read each line
    data = {}
    for line in lines:
        line_info = line.strip().split(" ")     # TODO: Does not take into account if file or folder has space
        
        if line_info[0] != "":
            key = line_info[0].split(job)[1]    # Get rid of local storage path

            # If "location" is not recorded in info yet, instantiate a list for its value
            if key not in data:
                data[key] = []

            # Read frames
            for frame in line_info[1:]:
                if frame.isdigit():     # Avoid <err> or <null>
                    data[key].append(frame)
    
    return compress_baselight_data(data)

def compress_baselight_data(data):
    for location in data:
        # Sort first just in case
        frames = data[location]
        quicksort(frames, 0, (len(frames) - 1))

        new_frames = []
        i = frames[0]
        j = frames[0]
        for frame in frames:
            if (int(frame) - int(j)) > 1:   # Difference with last frame is greater than 1 = not consecutive
                if(i != j):
                    new_frames.append(str(i) + "-" + str(j))    # Range
                else:
                    new_frames.append(i)    # No consecutive frame
                i = frame
            j = frame
        if(i != j):
            new_frames.append(str(i) + "-" + str(j))    # Range
        else:
            new_frames.append(i)    # No consecutive frame
        data[location] = new_frames
    return data

def set_frames_to_location(job, locations, l_and_f):
    for_csv = []
    for loc in locations:
        x = []
        x.append(loc)
        for frames in l_and_f[loc.split(job)[1]]:
            x.append(frames)
        for_csv.append(x)
    return for_csv

def main(args):
    if args.jobFolder is None:
        print("No job selected")
        return 2
    # else if(if directory not exists)
    else:
        xytech_filename = args.jobFolder + "/xytech.txt"
        header, locations = get_xytech_info(xytech_filename)
        baselight_filename = args.jobFolder + "/baselight_export.txt"
        loc_and_frames = get_baselight_info(args.jobFolder, baselight_filename)
        sans_frames = set_frames_to_location(args.jobFolder, locations, loc_and_frames)

        csv_filename = args.jobFolder + ".csv"
        with open(csv_filename, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(header)
            writer.writerow([])
            writer.writerow([])
            for work in sans_frames:
                writer.writerow(work)
        csv_file.close()
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", dest="jobFolder", help="job to process")
    # parser.add_argument("--verbose", action="store_true", help="show verbose")
    # parser.add_argument("--TC", dest="timecode", help="Timecode to process")
    args = parser.parse_args()
    sys.exit(main(args))
