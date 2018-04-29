# ABOUT: Script to calculate mean execution time
#
# Input 1: query output file consisting of two columns: (query name with ID) and (query exec time)
# Output: Mean query execution time for every query pattern

import sys

if __name__ == '__main__':

    result_file = open(sys.argv[1])
    output_file = open(sys.argv[2])

    id_dict = dict()
    mean_time_dict = dict()
    for line in result_file:
        line = line.strip()
        parts = line.split("\t")
        id = int(parts[0].split("--SO-OS-SS-VP")[0])
        pattern_id = id - 1 / 100 + 1
        time = parts[1]
        if pattern_id in id_dict:
            lst = id_dict[pattern_id]
            lst.add(time)
            id_dict[pattern_id] = lst
        else:
            lst = list()
            lst.add(time)
            id_dict[pattern_id] = lst

    for key in id_dict.keys():
        lst = id_dict[key]
        meantime = 0
        size = len(lst)
        for elem in lst:
            meantime += elem
        mean_time_dict[key] = meantime / size

    for key in mean_time_dict.keys():
        output_file.write(key + "\t" + mean_time_dict[key])

    output_file.close()
