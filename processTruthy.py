import os
import re
import bz2
import csv
import pathlib
import argparse
import multiprocessing as mp
from itertools import zip_longest

from tqdm import tqdm
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser


def estimate_file_line_count(file_path, est_size=1000 * 1024):
    """ Reads a chunk of a file and estimates the number of lines
    in file based on the average size of each line in sample chunk.

    Args:
        file_path: Path to target file.
        est_size: Chunk size in bytes to base estimate on.

    Returns:
        est_lines:
    """
    with open(file_path, "rb") as f:
        buffer = f.read(est_size)
        avg_line_size = len(buffer) // buffer.count((b'\n'))
    est_lines = os.path.getsize(file_path) // avg_line_size
    return est_lines


def init(l):
    """Creates a global file lock for writing to output
    files from each worker.

    Args:
        l: multiprocessing.Lock() object
    """
    global lock
    lock = l


def parse_line(line):
    """Parses file line for triple information and type

    Args:
        line: Line from

    Returns:
        triple_type:    The type of triple read from the file,
                        can be any of the following:
                        EPE - Entity Property Entity
                        EPV - Entity Property Value
                        ELV - Entity Label Value
                        error - Unhandled cases and exceptions
        els:    List of parsed elements from line.

    """
    class _Sink(object):
        """Sink object for W3CNTriplesParser, otherwise
        writes to std.out
        """
        def __init__(self):
            self.length = 0
            self.triple_list = []

        def triple(self, sub, pred, obj):
            self.length += 1
            self.triple_list = [str(sub), str(pred), str(obj)]
    sink = _Sink()
    parser = W3CNTriplesParser(sink)
    parser.parsestring(line)
    triple_type = ''
    els = sink.triple_list
    try:
        for i in range(3):
            e = els[i]
            if re.search("P[0-9]+$", e):
                els[i] = e.split('/')[-1]
                triple_type = triple_type + 'P'
            elif re.search("Q[0-9]+$", e):
                els[i] = e.split('/')[-1]
                triple_type = triple_type + 'E'
            else:
                triple_type = triple_type + 'V'
    except:
        return "error", [line]

    if triple_type in ("EPE", "EPV"):
        return triple_type, els
    else:
        triple_type = "ELV"
        els = [els[0], els[2]]
        return triple_type, els


def process_chunk(raw_chunk, chunksize, root_path):
    """Processes a chunk of decompressed lines and writes
    them to output files.

    Args:
        raw_chunk: Iterable of decompressed lines to process
        chunksize: Number of lines in raw_chunk iterable
        root_path: Path to current file location for output write

    Returns:
        chunksize: To update tqdm progress bar in main()

    """
    line_types = ("EPE", "EPV", "ELV", "error")
    output_paths = {
        "EPE": pathlib.Path(root_path + "\\output\\edges.tsv"),
        "EPV": pathlib.Path(root_path + "\\output\\data.tsv"),
        "ELV": pathlib.Path(root_path + "\\output\\meta.tsv"),
        "error": pathlib.Path(root_path + "\\output\\errors.tsv"),
    }
    output_buffers = {k: [] for k in line_types}

    for raw_line in raw_chunk:
        line = raw_line.strip().decode('utf8')
        line_type, parsed_line = parse_line(line)
        if line_type and parsed_line:
            output_buffers[line_type].append(parsed_line)
    lock.acquire()
    for ltype in line_types:
        with open(output_paths[ltype], "a+", newline='', encoding='utf-8') as o:
            for line in output_buffers[ltype]:
                csv.writer(o, delimiter='\t').writerow(line)
    lock.release()
    return chunksize


def chunker(n, iterable, padvalue=None):
    """ Splits an iterable object into n iterable chunks

    Args:
        n: Size of each chunk in lines.
        iterable: The iterable to split.
        padvalue: Value to pad end of any short chunk with.

    Returns:
        groups: Iterable of n-sized chunks.
    """
    groups = zip_longest(*[iter(iterable)]*n, fillvalue=padvalue)
    return groups


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process .nt.bz2 wikidata file")
    parser.add_argument("-f", dest="filepath", required=True,
                        help="Path to latest-truthy.nt.bz2 file", metavar="FILE")
    parser.add_argument('-o', dest='output', required=False,
                        help='Output filepath', metavar='OUT')
    parser.add_argument('-c', dest='chunksize', required=False,
                        help='Number of lines to be passed to workers', metavar='CHUNK')
    args = parser.parse_args()

    if not args.output:
        output_path = os.path.dirname(os.path.abspath(__file__))
    else:
        if not os.path.exists(args.output):
            os.mkdir(args.output)
        output_path = args.output

    if not args.chunksize:
        chunksize = 10000
    else:
        chunksize = args.chunksize
    # replace with args.filepath for slurm jobs
    # source_file = pathlib.Path("C:/Users/qmays/Downloads/latest-truthy.nt.bz2")
    executors = int(mp.cpu_count() * 0.7)
    l = mp.Lock()
    p = mp.Pool(executors, init, (l,))
    with tqdm(total=estimate_file_line_count(args.filepath)) as pbar:
        for chunk in chunker(chunksize, bz2.open(args.filepath)):
            results = p.apply_async(process_chunk, (chunk, chunksize, output_path,))
            pbar.update(results.get())
