import bz2
import multiprocessing as mp
from pathlib import Path
from itertools import zip_longest
from tqdm import tqdm

import utilities2


def create_test_data():
    in_file_name = Path('test/edges.tsv')
    out_file_name = Path('test/edges.tsv.bz2')
    with open(in_file_name, "rb") as in_f:
        with bz2.open(out_file_name, "wb") as out_f:
            out_f.write(in_f.read())
    return out_file_name


def process_function(raw_line):
    if raw_line:
        line = raw_line.strip().decode('utf8')
        return line


def grouper(n, iterable, padvalue=None):
	"""grouper(3, 'abcdefg', 'x') -->
	('a','b','c'), ('d','e','f'), ('g','x','x')"""

	return zip_longest(*[iter(iterable)]*n, fillvalue=padvalue)


if __name__ == "__main__":
    file_name = Path('C://Users/qmays/Downloads/latest-truthy.nt.bz2')
    executors = int(mp.cpu_count()*0.7)
    p = mp.Pool(processes=executors)
    m = mp.Manager()
    f_lock = m.Lock()
    with tqdm(total=utilities2.estimate_file_line_count(file_name)) as pbar:
        for chunk in grouper(100000, bz2.open(file_name)):
            p.apply_async(utilities2.process_line, args=(chunk,f_lock,))
            pbar.update(100000)
    p.close()
    p.join()
