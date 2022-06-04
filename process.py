import bz2
from pathlib import Path
from multiprocessing import Process, Manager
import itertools

from tqdm import tqdm
from utilities import estimate_file_line_count, process_line, write_tsv_header


def process_truthy_dump(input_path, output_path):
    dump = bz2.open(input_path)

    write_tsv_header(output_path)

    progress = tqdm(total=estimate_file_line_count(input_path))
    counter = 0
    line = dump.readline()

    while True:
        line = dump.readline().strip().decode('utf8')
        counter += 1
        progress.update(1)
        process_line(line, output_path)
        if counter > 100000:
            break


process_truthy_dump(Path('C://Users/qmays/Downloads/latest-truthy.nt.bz2'), Path('test'))
