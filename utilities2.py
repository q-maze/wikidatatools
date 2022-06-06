import os
import re
import csv
from pathlib import Path



def estimate_file_line_count(file_path, est_size=1000 * 1024):
    with open(file_path, "rb") as f:
        buffer = f.read(est_size)
        avg_line_size = len(buffer) // buffer.count((b'\n'))
    return os.path.getsize(file_path) // avg_line_size


def write_tsv_header(output_path):
    edges_file = open(os.path.join(output_path, r'edges.tsv'), 'w', newline='')
    data_file = open(os.path.join(output_path, r'data.tsv'), 'w', newline='')
    meta_file = open(os.path.join(output_path, r'meta.tsv'), 'w', newline='')

    edges_csv = csv.writer(edges_file, delimiter='\t')
    data_csv = csv.writer(data_file, delimiter='\t')
    meta_csv = csv.writer(meta_file, delimiter='\t')

    edges_csv.writerow(['Head', 'Relation', 'Tail'])
    data_csv.writerow(['Head', 'Relation', 'Value'])
    meta_csv.writerow(['Head', 'Label', 'Value', 'Other'])

    edges_file.close()
    data_file.close()
    meta_file.close()


def process_line(raw_line, lock):
    qregex = re.compile("Q[0-9]+$")
    pregex = re.compile("P[0-9]+$")

    output_path = Path('C://Users/qmays/PycharmProjects/wikidatatools/output')

    line = raw_line.strip().decode('utf8')

    els = re.findall('[^<>]*<(.*?)>[^<>]*', line)
    if re.search('"(.*?)"', line):
        els.append(re.search('"(.*?)"', line).group(1))
    elif re.search(' _:(.*?) .', line):
        els.append(re.search(' _:(.*?) .', line).group(1))
    else:
        pass
    triple_type = ''
    try:
        for i in range(3):
            e = els[i]
            # if the element is an entity
            if pregex.search(e):
                # retrieve the last element of the html address and extract the Q number
                els[i] = e.split('/')[-1].replace(">", "")
                triple_type = triple_type + 'P'
            elif qregex.search(e):
                # retrieve the last element of the html address and extract the P number
                els[i] = e.split('/')[-1].replace(">", "")
                triple_type = triple_type + 'E'
            else:
                # both labels and values are processed the same way
                els[i] = e.replace("<", "").replace(">", "")
                if i == 1:
                    # labels are always in the second position in the triple
                    triple_type = triple_type + 'L'
                else:
                    # while values are always in the third position
                    triple_type = triple_type + 'V'
            if triple_type == 'EPE':
                lock.acquire()
                edges_file = open(os.path.join(output_path, f'edges.tsv'), 'a+', newline='', encoding='utf-8')
                edges_csv = csv.writer(edges_file, delimiter='\t')
                edges_csv.writerow(els)
                edges_file.close()
                lock.release()
            elif triple_type == 'EPV':
                lock.acquire()
                data_file = open(os.path.join(output_path, f'data.tsv'), 'a+', newline='', encoding='utf-8')
                data_csv = csv.writer(data_file, delimiter='\t')
                data_csv.writerow(els)
                lock.release()
            # otherwise write it to the metadata file (labels)
            else:
                lock.acquire()
                meta_file = open(os.path.join(output_path, f'meta.tsv'), 'a+', newline='', encoding='utf-8')
                meta_csv = csv.writer(meta_file, delimiter='\t')
                meta_csv.writerow(els)
                meta_file.close()
                lock.release()
    except:
        lock.acquire()
        error_log = open(os.path.join(output_path, f'errors.txt'), 'w', encoding='utf-8')
        error_log.write(line)
        error_log.close()
        lock.release()
