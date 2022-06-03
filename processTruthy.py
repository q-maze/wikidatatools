import bz2  # for decompressing
import sys  # for IO
import re  # for checking the type of element
import csv # for output
import os  # for checking the filesize

# file path for compressed file
file_path = '/gpfs/gpfs0/project/ds6011-sp22-wiki-data/Data/latest-truthy_2-21-2022.nt.bz2'

# chunk size in bytes to read in
CHUNKSIZE = 500  * 1024
# estimated number of chunks in the file
est_chunks = os.path.getsize(file_path) // CHUNKSIZE

#regex to check if element is an entity
qregex = re.compile("Q[0-9]+>$")
# regex to check if element is a property
pregex = re.compile("P[0-9]+>$")

# create a decompressor object
dc = bz2.BZ2Decompressor()

# Open right files
edges_file = open('/gpfs/gpfs0/project/ds6011-sp22-wiki-data/wikidata_edges_v2.csv', 'w')
data_file = open('/gpfs/gpfs0/project/ds6011-sp22-wiki-data/wikidata_v2.csv', 'w')
# open a file to output the metadata for each element and write header
meta_file = open('/gpfs/gpfs0/project/ds6011-sp22-wiki-data/wikidata_meta_v2.csv', 'w')
# csv writers
edges_csv = csv.writer(edges_file)
data_csv = csv.writer(data_file)
meta_csv = csv.writer(meta_file)
# File headers
edges_csv.writerow(['Entity_1', 'Entity_2', 'Entity_3'])
data_csv.writerow(['Entity_1', 'Entity_2', 'Entity_3', 'Triple_Type'])
meta_csv.writerow(['Entity', 'Label', 'Value', 'Triple_Type'])

#Start of the Decompression and writting

with open(file_path, 'rb') as f:
    while True:
        # read in a chunk of data
        chunk = f.read(CHUNKSIZE)
        # if there is no more data, break out of loop
        if not chunk:
            break
        # for each line in the chunk
        # read in a line of data, decompress it, decode it, strip whitespace, and split into lines
        for line in dc.decompress(chunk).decode().strip().splitlines():
            # split each line into elements
            cut_off = line.rfind(" .")
            if cut_off != -1:
                line = line[:cut_off]

            els = line.split(maxsplit=2)
            # if there are more than two elements (eliminates boundaries of chunks)
            if len(els) > 2:
                # create an output string for the data
                output = ''
                # create an output string for the relationship type
                triple_type = ''
                for i in range(3):
                    # assign element to variable
                    e = els[i]
                    # if the element is an entity
                    if pregex.search(e):
                        # retrieve the last element of the html address and extract the Q number
                        els[i] = e.split('/')[-1].replace(">","")
                        #output = output + e.split('/')[-1].replace("P","").replace(">","")
                        triple_type = triple_type + 'P'
                    elif qregex.search(e):
                        # retrieve the last element of the html address and extract the P number
                        els[i] = e.split('/')[-1].replace(">","")
                        #output = output + e.split('/')[-1].replace('Q','').replace('>','')
                        triple_type = triple_type + 'E'
                    else:
                        # both labels and values are processed the same way
                        els[i] = e.replace("<","").replace(">","")
                        #output = output + e.replace("<","").replace(">","")
                        if i == 1:
                            # labels are always in the second position in the triple
                            triple_type = triple_type + 'L'
                        else:
                            # while values are always in the third position
                            triple_type = triple_type + 'V'
                    if i != 2:
                         # to create a csv add commas between elements
                        output = output + ','
                    else:
                        # if the last element, add a newline char
                        els = els + [triple_type]
                        # output = output + ',' + triple_type + '\n'
                # if the triple type is EPE or EPV write it to the data file
                print(els)
                if (triple_type == 'EPE'):
                    edges_csv.writerow(els[:3])
                elif (triple_type == 'EPV'):
                    data_csv.writerow(els)
                # otherwise write it to the metadata file (labels)
                else:
                    # only record english labels
                    if '@en' in els[2]:
                        meta_csv.writerow(els)
        # update the progress bar
        #pbar.update(1)
        # update the current chunk
        #cur_chunk += 1
# close the files always
edges_file.close()
data_file.close()
meta_file.close()

print("completed")