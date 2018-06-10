"""This script deletes all duplicate sentences in a parallel corpus of two languages, L1 and L2. A sentence is considered
to be duplicate if the same string appears in L1 in two lines and if in the same two lines in L2, the strings are also the
 same (with regard to each other only in L2). This means if the L1 corpus e.g. contains the
same string in lines 3 and 15, but the L2 corpus contains two different strings in the respective lines (due to different translations)
 the sentences will NOT be deleted."""

import os
import sys
from time import time
from multiprocessing import Pool


class FastElimDupl:
    def __init__(self, path_to_L1_file, path_to_L2_file, num_threads):
        self.l1 = path_to_L1_file
        self.l2 = path_to_L2_file
        self.num_threads = num_threads
        self.l1_string_to_dupl_set = dict()  # key: duplicate string, value: set of all its IDs
        self.l1_dupl_id_to_string = dict()  # key: duplicate ID, value: corresponding duplicate string
        self.l2_string_to_dupl_set = dict()  # key: duplicate string, value: set of all its IDs
        self.l2_dupl_id_to_string = dict()  # key: duplicate ID, value: corresponding duplicate string
        self.ids_to_delete = set()

    @staticmethod
    def get_dupl_ids(path_to_file):
        duplicate_file = path_to_file + '.duplicates'
        # Use command-line tools to find the duplicates
        os.system('nl ' + path_to_file + ' | sort -k 2 | uniq -D -f 1 | sort -k 2 > ' + duplicate_file)
        string_to_dupl_set = dict()  # key: duplicate string, value: set of all its IDs
        dupl_id_to_string = dict()  # key: duplicate ID, value: corresponding duplicate string

        with open(duplicate_file, "r") as f:
            for line in f:
                if line.strip() == "":  # there are some empty lines in the output of the nl command
                    continue
                line_number, string = line.split("\t")
                if string not in string_to_dupl_set.keys():
                    string_to_dupl_set[string] = set()
                string_to_dupl_set[string].add(int(line_number))
                dupl_id_to_string[int(line_number)] = string

        return string_to_dupl_set, dupl_id_to_string

    def find_parallel_duplicates_in_subset(self, dupl_set):
        ids_to_delete = set()
        for dupl_id in dupl_set:
            if dupl_id in self.l2_dupl_id_to_string.keys():
                # get all IDs of lines that are duplicates to dupl_id in L2
                l2_duplicate_set = self.l2_string_to_dupl_set[self.l2_dupl_id_to_string[dupl_id]]
                intersection = dupl_set.intersection(l2_duplicate_set)

                # if dupl_set and l2_duplicate_set share more than 1 ID, we have found a parallel duplicate
                if len(intersection) > 1:
                    ids_to_delete = ids_to_delete.union(intersection)

                # if the intersection of dupl_set with l2_duplicate_set is again a superset of dupl_set, dupl_set and
                # l2_duplicate_set contain the same elements and thus all parallel duplicates for this duplicate set
                # have been found
                if intersection.issuperset(dupl_set):
                    break
        return ids_to_delete

    def find_parallel_duplicates(self):
        l1_list_of_dupl_sets = list(self.l1_string_to_dupl_set.values())
        # sort the list in order to equally distribute big and small sets among the CPUs
        l1_list_of_dupl_sets.sort(key=len, reverse=True)
        map_list = []
        for i in range(self.num_threads):
            i_th_list = list()
            # if CPU has e.g. ID 1, get 1st, (1+num_thread)th, (1+2*num_thread)th element etc.
            for subset_id in range(i, len(l1_list_of_dupl_sets), self.num_threads):
                i_th_list.append(l1_list_of_dupl_sets[subset_id])
            map_list.extend(i_th_list)

        # run the processes
        with Pool(processes=self.num_threads) as pool:
            ids_to_delete = pool.map(self.find_parallel_duplicates_in_subset, map_list)

        # make the union of all sets
            for set in ids_to_delete:
                self.ids_to_delete = self.ids_to_delete.union(set)

    def make_new_file(self, input_path, output_path):
        line_number = 0
        with open(input_path, "r") as inp:
            with open(output_path, "w") as outp:
                for line in inp:
                    line_number += 1  # nl starts counting line numbers with 1
                    if line_number not in self.ids_to_delete:
                        outp.write(line)

    def run(self):
        starting_time = time()
        print("Getting L1 duplicates with sort and uniq...")
        self.l1_string_to_dupl_set, self.l1_dupl_id_to_string = self.get_dupl_ids(self.l1)
        print("Done after {:.1f} seconds.".format(time()-starting_time))
        starting_time = time()
        print("Getting L2 duplicates with sort and uniq...")
        self.l2_string_to_dupl_set, self.l2_dupl_id_to_string = self.get_dupl_ids(self.l2)
        print("Done after {:.1f} seconds.".format(time()-starting_time))
        starting_time = time()
        print("Finding parallel duplicates...")
        self.find_parallel_duplicates()
        print("Parallel duplicates found after {:.1f} seconds.".format(time()-starting_time))
        starting_time = time()
        print("Compiling new L1 file...")
        output_l1 = self.l1[:-2] + "dupl_rem." + self.l1[-2:]
        self.make_new_file(self.l1, output_l1)
        print("Done after {:.1f} seconds.".format(time() - starting_time))
        starting_time = time()
        print("Compiling new L2 file...")
        output_l2 = self.l2[:-2] + "dupl_rem." + self.l2[-2:]
        self.make_new_file(self.l2, output_l2)
        print("Done after {:.1f} seconds.".format(time() - starting_time))
        return output_l1, output_l2

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 fast_elim_dupl.py <L1 file> <L2 file> <number of threads>")
        sys.exit(1)
    eliminator = FastElimDupl(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    eliminator.run()
