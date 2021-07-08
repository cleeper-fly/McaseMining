import csv
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--raw_file', type=str, dest='chunk_file', required=True)
parser.add_argument('--clean_file', type=str, dest='clean_file', required=True)

if __name__ == "__main__":
    args = parser.parse_args()
    with open(Path(args.chunk_file), newline = '') as f:
        reader = f.read()
        reader = reader[25:]
        reader = [line for line in reader.split('\n')]
        currline = []
        all_lines = []
        i = 0
        while i < len(reader):
            if reader[i][0:7].isdigit():
                if len(reader[i]) < 3:
                    i += 1
                    continue

                first = reader[i].split('\t')
                all_lines.append(currline)
                currline = []
                currline.append(first[0])
                currline.append(first[1])
                currline.append(''.join(x for x in first[2:]))
            else:
                onetwo = currline[:-1]
                last = currline[-1] + reader[i]
                onetwo.append(last)
                currline = onetwo
            i += 1

        all_lines = all_lines[1:]

    with open(Path(args.clean_file), 'w+') as nef:
        writer = csv.writer(nef, delimiter='\t')
        for line in all_lines:
            if line[-1] != '':
                writer.writerow(line)
