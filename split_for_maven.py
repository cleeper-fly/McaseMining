import csv
import math
import argparse

from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument('--batch_size', type=int, dest='batch_size', required=True)
parser.add_argument('--clean_file', type=str, dest='clean_file', required=True)
parser.add_argument('--for_maven_files', type=str, dest='for_maven_files', required=True)
parser.add_argument('--coordinates_tsv', type=str, dest='COORDINATES_TSV_PATH', required=True)
parser.add_argument('--unprocessed_tsv', type=str, dest='UNPROCESSED_TSV_PATH', required=True)


def _find_occurrences(s: str) -> list:
    max_length = len(s)
    sample = []
    for i, letter in enumerate(s):
        if i < max_length-1:
            if letter == '.' and s[i+1] == ' ':
                sample.append(i)
        else:
            if letter == '.':
                sample.append(i)
    if not sample or not s.endswith('.'):
        sample.append(str(len(s) - 1))
    return sample


counter = 0
if __name__ == '__main__':
    args = parser.parse_args()
    with open(Path(args.clean_file), 'r', newline='', encoding='utf-8') as file_clean_file, \
            open(Path(args.COORDINATES_TSV_PATH), 'a', newline='', encoding='utf-8') as file_COORDINATES_TSV_PATH:
        reader = file_clean_file.readlines()
        reader = [line.rstrip('\r\n').split('\t') for line in reader]
        writer = csv.writer(file_COORDINATES_TSV_PATH, delimiter='\t')
        batches = math.ceil(len(reader) / args.batch_size)
        batches_counter = 1
        while batches_counter < batches + 1:
            with open(f'{args.for_maven_files}{batches_counter}.txt', 'a', encoding='utf-8') as maven_text, open(
                    Path(args.UNPROCESSED_TSV_PATH), 'a', encoding='utf-8') as out_coords:
                inside_batch_counter = 1
                coords_writer = csv.writer(out_coords, delimiter='\t')
                line = reader[counter]
                text_line_1 = line[1].replace('"""', '')
                text_line_2 = line[2].replace('"""', '')
                dots = _find_occurrences(text_line_1 + ' ' + text_line_2)
                pmid = str(line[0])
                reader_line = [pmid, str(text_line_1), str(text_line_2), ';'.join(str(coord) for coord in dots)]
                writer.writerow(reader_line)
                coords_writer.writerow(reader_line)
                maven_text.write(text_line_1 + ' ' + text_line_2)
                last_dot = dots[-1]
                counter += 1
                while inside_batch_counter < args.batch_size:
                    line = reader[counter]
                    text_line_1 = line[1].replace('"""', '')
                    text_line_2 = line[2].replace('"""', '')
                    dots = _find_occurrences(text_line_1 + ' ' + text_line_2)
                    pmid = str(line[0])
                    reader_line = [pmid, text_line_1, text_line_2, ';'.join(str(coord) for coord in dots)]
                    coords_writer.writerow(reader_line)
                    maven_text.write(text_line_1 + ' ' + text_line_2)
                    new_dot_set = [str(int(last_dot) + 1)]
                    for x in dots:
                        new_dot_set.append(str((int(x) + int(last_dot) + int(1))))
                    new_args = ';'.join(y for y in new_dot_set)
                    new_row = reader_line[:3]
                    new_row.append(new_args)
                    writer.writerow(new_row)
                    last_dot = new_dot_set[-1]
                    counter += 1
                    inside_batch_counter += 1
            batches_counter += 1
