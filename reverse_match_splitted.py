import argparse
import collections
import concurrent.futures
import math
import os
import re
import pandas as pd

from os import listdir
from os.path import join, isfile
from re import Pattern
from typing import Optional, TextIO
from pathlib import Path
from my_utils import timeit

parser = argparse.ArgumentParser()
parser.add_argument('--batch_size', type=int, dest='batch_size', required=True)
parser.add_argument('--func_name', type=str, dest='func_name', required=True, choices=['hpo', 'ordo'])
parser.add_argument('--unprocessed_tsv', type=str, dest='UNPROCESSED_TSV_PATH', required=True)
parser.add_argument('--coordinates_tsv', type=str, dest='COORDINATES_TSV_PATH', required=True)
parser.add_argument('--hpo_tsv', type=str, dest='TSV_HPO_PATH', required=True)
parser.add_argument('--finished_tsv', type=str, dest='TSV_ORDO_PATH', required=True)
parser.add_argument('--temp_directory', type=str, dest='TMP_DIR', required=True)


class FuncNameDependencyResolver:
    DEFAULT_HEADERS = ['id', 'title', 'abstract', 'sentences', 'hpo']

    class Regex:
        hpo = re.compile(r'^.*/HP_\d*$')
        ordo = re.compile(r'^.*/Orphanet_\d*$')

    class Name:
        HPO = 'hpo'
        ORDO = 'ordo'

    @classmethod
    def input_file_by_func_name(cls, func_name: str) -> str:
        if func_name == cls.Name.HPO:
            return str(Path(args.UNPROCESSED_TSV_PATH))
        else:
            return str(Path(args.TSV_HPO_PATH))

    @classmethod
    def output_file_by_func_name(cls, func_name: str) -> str:
        if func_name == cls.Name.HPO:
            return str(Path(args.TSV_HPO_PATH))
        else:
            return str(Path(args.TSV_ORDO_PATH))

    @classmethod
    def columns_by_func_name(cls, func_name: str) -> list[str]:
        return cls.DEFAULT_HEADERS if func_name == cls.Name.HPO else [*cls.DEFAULT_HEADERS, 'ordo']

    @classmethod
    def regex_by_func_name(cls, func_name: str) -> Pattern:
        return getattr(cls.Regex, func_name)


class Term:
    def __init__(self, name: str, ref: str, coords: str):
        self.name = name
        self.ref = ref
        self.start, self.stop = (int(x) for x in coords.split(':'))

    def get_string(self):
        return f'{self.name},{self.ref},{self.start}:{self.stop}'


class CoordinatesReverseMatchUseCase:
    def __init__(self, command_line_args: argparse.Namespace):
        self._func_name = command_line_args.func_name
        self._batch_size = command_line_args.batch_size
        self._output_dfs = {}
        self._open_files_and_create_dfs()

    @timeit
    def execute(self):
        with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = {
                executor.submit(self._process_batch, i): f'batch_{i}'
                for i in range(math.ceil(self._proc_file.shape[0] / self._batch_size))
            }
            for future, batch_name in futures.items():
                self._output_dfs[batch_name] = future.result()
        self._save_output()

    def _process_batch(self, batch_number):
        print(f'Started processing batch #{batch_number}')
        _output_df = pd.DataFrame(
            columns=FuncNameDependencyResolver.columns_by_func_name(func_name=self._func_name)
        )
        lines_from_file = self._get_lines_from_file(current_batch=batch_number)

        start = batch_number * self._batch_size
        max_row_id = self._proc_file.shape[0]
        if max_row_id > batch_number * self._batch_size + self._batch_size:
            end = batch_number * self._batch_size + self._batch_size
        else:
            end = batch_number * self._batch_size + (max_row_id - batch_number * self._batch_size)

        for i in range(start, end):
            terms_per_paper = []
            coords_after = self._coordinates_df.iloc[i][3].split(';')
            first_coord, last_coord = int(coords_after[0]), int(coords_after[-1])
            for file_line in lines_from_file:
                term = Term(*file_line.split('&*'))
                if term.start >= first_coord and term.stop < last_coord:
                    term.start = term.start - first_coord
                    term.stop = term.stop - first_coord
                    terms_per_paper.append(term.get_string())

            terms_set = ';'.join(x for x in terms_per_paper)
            trow = self._proc_file.iloc[i].to_list()
            trow.append(terms_set)
            trow = pd.Series(trow, index=FuncNameDependencyResolver.columns_by_func_name(func_name=self._func_name))
            _output_df = _output_df.append(trow, ignore_index=True)
        print(f'Finished processing batch #{batch_number}')
        return _output_df


    def _get_lines_from_file(self, current_batch: int):
        with open(file=str(Path(f'{args.TMP_DIR}{self._samples[current_batch]}')), encoding='utf-8') as fi:
            return self._collect_all_lines_from_file(fi, self._func_name)


    def _open_files_and_create_dfs(self):
        with open(file=Path(args.COORDINATES_TSV_PATH), encoding='utf-8') as coordinates_fi:
            self._coordinates_df = pd.read_csv(coordinates_fi, delimiter='\t', header=None)
        with open(
                file=FuncNameDependencyResolver.input_file_by_func_name(self._func_name),
                newline='\n',
                encoding='utf-8',
        ) as fi:
            self._proc_file = pd.read_csv(fi, delimiter='\t', header=None)

        self._samples = self._get_dir_content(args.TMP_DIR)
        self._samples = sorted(self._samples, key=lambda s: [int(t) if t.isdigit()
                                                             else t.lower() for t in re.split('(\d+)', s)])


    def _save_output(self):
        ordered_results = collections.OrderedDict(
            sorted(
                self._output_dfs.items(),
                key=lambda x: int(''.join(filter(str.isdigit, x[0].split('.')[0])))
            )
        )
        results_list = list(ordered_results.values())
        result = results_list[0]
        results_list.pop(0)
        for v in results_list:
            result = result.append(v)

        with open(
                file=Path(FuncNameDependencyResolver.output_file_by_func_name(self._func_name)),
                mode='w',
                encoding='utf-8',
        ) as fi:
            result.to_csv(fi, sep='\t', index=False, header = False)

    @classmethod
    def _collect_all_lines_from_file(cls, _file: TextIO, func_name: str) -> list[str]:
        counter1 = 0
        counter2 = 1
        lines_from_file = []
        lines = _file.readlines()
        while counter1 < len(lines):
            try:
                str_list_1 = lines[counter1].split('\t')
                str_list_2 = lines[counter2].split('\t')
                if parsed_pair := cls._process_temp(func_name, str_list_1, str_list_2):
                    link, term, location = parsed_pair
                    lines_from_file.append(f'{term}&*{link}&*{":".join(y for y in location)}')
            except Exception as e:
                print(f'collect all lines {_file} error in line {counter1}. Ошибка: {e}')
            counter1 += 2
            counter2 += 2
        return lines_from_file

    @staticmethod
    def _get_dir_content(path: str) -> list[str]:
        only_files = [_file for _file in listdir(join(Path(path))) if isfile(join(Path(path), _file))]
        return [_file for _file in only_files if _file.endswith('a1')]

    @staticmethod
    def _process_temp(
            func_name: str,
            str_list_1: list[str],
            str_list_2: list[str]
    ) -> Optional[tuple[str, str, list[str]]]:
        if re.search(FuncNameDependencyResolver.regex_by_func_name(func_name), str_list_2[-2]):
            location_list = str_list_1[1].split(' ')
            loc = [location_list[1], location_list[2]]
            r_term = str_list_1[-1].rstrip('\n')
            r_link = str(re.findall('http.*', str_list_2[-2])[0])
            return r_link, r_term, loc

    @staticmethod
    def _split_df(df: pd.DataFrame, batch_size: int) -> list[pd.DataFrame]:
        return [df[i:i + batch_size].reset_index(drop=True).copy(deep=True) for i in range(0, df.shape[0], batch_size)]


if __name__ == '__main__':
    args = parser.parse_args()
    CoordinatesReverseMatchUseCase(args).execute()
