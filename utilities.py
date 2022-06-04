import os
import re
import csv


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


def process_line(line, output_path=None):
    qregex = re.compile("Q[0-9]+$")
    pregex = re.compile("P[0-9]+$")

    edges_file = open(os.path.join(output_path, r'edges.tsv'), 'a', newline='', encoding='utf-8')
    data_file = open(os.path.join(output_path, r'data.tsv'), 'a', newline='', encoding='utf-8')
    meta_file = open(os.path.join(output_path, r'meta.tsv'), 'a', newline='', encoding='utf-8')
    error_log = open(os.path.join(output_path, r'errors.txt'), 'w', encoding='utf-8')

    edges_csv = csv.writer(edges_file, delimiter='\t')
    data_csv = csv.writer(data_file, delimiter='\t')
    meta_csv = csv.writer(meta_file, delimiter='\t')

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
                edges_csv.writerow(els)
            elif triple_type == 'EPV':
                data_csv.writerow(els)
            # otherwise write it to the metadata file (labels)
            else:
                # only record english labels
                if '@en' in els[2]:
                    meta_csv.writerow(els)
    except:
        error_log.write(line)

    edges_file.close()
    data_file.close()
    meta_file.close()
    error_log.close()
