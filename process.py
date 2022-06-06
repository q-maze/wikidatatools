import bz2
from pathlib import Path
import time
import statistics

from tqdm import tqdm
from utilities import estimate_file_line_count, process_line, write_tsv_header


def process_truthy_dump(input_path, output_path):
    dump = bz2.open(input_path)

    write_tsv_header(output_path)

    progress = tqdm(total=estimate_file_line_count(input_path))
    counter = 0
    line = dump.readline()

    read_times = []
    process_times = []
    while True:
        read_start = time.process_time()
        line = dump.readline().strip().decode('utf8')
        read_time = (time.process_time()-read_start)
        read_times.append(read_time)
        counter += 1
        progress.update(1)
        process_start = time.process_time()
        process_line(line, output_path)
        process_time = time.process_time() - process_start
        process_times.append(process_time)
        if counter > 10000:
            break
    print(f"Mean read time {statistics.mean(read_times)}")
    print(f"Mean process time {statistics.mean(process_times)}")


process_truthy_dump(Path('C://Users/qmays/Downloads/latest-truthy.nt.bz2'), Path('test'))
