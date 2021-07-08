import requests
import concurrent.futures
import sys
import os
import re
import pandas as pd
import argparse

sys.path.append("/home/gleb/negation-detection")
import negation_detection

from bs4 import BeautifulSoup
from pathlib import Path
from my_utils import timeit

parser = argparse.ArgumentParser()

parser.add_argument('--finished_tsv', type=str, dest='finished_tsv_path', required=True)
parser.add_argument('--ordo_table', type=str, dest='ordo_table_path', required=True)
parser.add_argument('--hpo_table', type=str, dest='hpo_table_path', required=True)

def _get_abr_from_allie(term: str) -> list:
    query = 'http://allie.dbcls.jp/rest/getPairsByLongform?keywords=' + f'{term.lower()}'
    responce = requests.get(query).text
    soup = BeautifulSoup(responce, 'xml')
    abbreviations = [x.get_text() for x in soup.find_all('abbreviation')]
    return abbreviations


def _convert_terms_with_allie(terms: list, text: list) -> list:
    additional_terms = []
    text = '.'.join(x for x in text)
    for term in terms:
        term_name = term[0]
        abbreviations = _get_abr_from_allie(term_name)
        for abbreviation in abbreviations:
            if text.find(abbreviation):
                x = [[m.start(), m.end()] for m in re.finditer(f" {abbreviation} ", text)]
                for el in x:
                    additional_terms.append([term_name, term[1], str(el[0]) + ':' + str(el[1])])
    if len(additional_terms) > 0:
        terms.append(additional_terms)
    return terms


def _filter_genes(given_response: str) -> list:
    indices = [1, 2, 3, 5]
    raw_response = [x.split('\t') for x in given_response.split('\n')]
    raw_response = [x for x in raw_response[2:] if len(x) > 3]
    response = [[response_element[ind] for ind in indices] for response_element in raw_response]
    return response


def _pubtator_responce(pubmedid: str) -> list:
    link = 'https://www.ncbi.nlm.nih.gov/research/pubtator-api/publications/export/pubtator?'
    query = f'{link}pmids={pubmedid}&concepts=gene'
    try:
        response = requests.get(query).text
    except Exception:
        response = []
    return _filter_genes(response)


def _match_term(term: list, gene: list, current_item: int, next_item: int) -> list:
    temp_term = Term(term)
    temp_gene = Gene(gene)
    if (int(temp_term.start) >= current_item and int(temp_gene.start) >= current_item) and (
            int(temp_term.end) < next_item and int(temp_gene.end) < next_item):
        return [temp_gene.id, [temp_term.id, temp_term.name]]
    else:
        return []


def _pairs_with_negation(terms: list, dots: list, genes: list, text: list) -> list:
    pairs = []
    if terms[0][0] != '':
        for term, gene in zip(terms, genes):
            for dots_iterator in range(len(dots) - 1):
                current_item, next_item = int(dots[dots_iterator]), int(dots[dots_iterator + 1])
                if _match_term(term, gene, current_item, next_item):
                    if negation_detection.predict(text[dots_iterator],
                                                  _match_term(term, gene, current_item, next_item)[1][1]):
                        pass
                    else:
                        continue
                    pairs.append(_match_term(term, gene, current_item, next_item))
    else:
        return pairs
    return pairs


def _update_frame(query: list, df: pd.DataFrame, classification_type: str) -> pd.DataFrame:
    gene_id = query[0]
    term_id = query[1][0]
    term_term = query[1][1]
    if gene_id in df.Gene.to_list():
        total_this_gene = df[(df['Gene'] == gene_id)]['Total']
        if not isinstance(total_this_gene, int):
            total_this_gene = int(total_this_gene.to_list()[0])
        total_this_gene = int(total_this_gene)
        if term_id in df[(df['Gene'] == gene_id)][f'{classification_type}_id'].to_list():
            value = int(df[(df['Gene'] == gene_id) & (df[f'{classification_type}_id'] == term_id)]['N'])
            df.loc[(df['Gene'] == gene_id) & (df[f'{classification_type}_id'] == term_id), 'N'] = value + 1
        else:
            df.loc[len(df.index)] = [gene_id, term_id, term_term, 1, total_this_gene, 1]
        df.loc[(df['Gene'] == gene_id), 'Total'] = total_this_gene + 1
    else:
        df.loc[len(df.index)] = [gene_id, term_id, term_term, 1, 1, 1]
    return df


def _count_frequencies(df: pd.DataFrame):
    for index, row in df.iterrows():
        df.at[index, 'Frequency'] = round(row.N / row.Total, 4)


class Gene:
    def __init__(self, params: list):
        self.start, self.end, self.name, self.id = params


class Term:
    def __init__(self, params: list):
        self.name = params[0]
        if len(re.findall("Orphanet_\d*$", params[1])) != 0:
            self.id = re.findall("Orphanet_\d*$", params[1])[0]
        if len(re.findall("HP_\d*$", params[1])) != 0:
            self.id = re.findall("HP_\d*$", params[1])[0]
        self.term_link = params[1]
        self.start, self.end = params[2].split(':')


class ProcessingLine:
    def __init__(self, line: list):
        self.id = line[0]
        self.text = [x for x in (str(line[1]) + str(line[2])).split('.') if len(x) > 8]
        self.dots = [0] + (line[3].split(';'))
        hpos = [[y for y in x.split(',')] for x in line[4].split(';')] if str(line[4]) != 'nan' else []
        if len(hpos) > 0:
            try:
                self.hpos = _convert_terms_with_allie(hpos, self.text)
            except Exception:
                self.hpos = hpos
        else:
            self.hpos = hpos
        ordos = [[y for y in x.split(',')] for x in line[5].split(';')] if str(line[5]) != 'nan' else []
        if len(ordos) > 0:
            try:
                self.ordos = _convert_terms_with_allie(ordos, self.text)
            except Exception:
                self.ordos = ordos
        else:
            self.ordos = ordos
        self.pubtator = []


def _process_df(iteration_row: pd.Series) -> tuple[list, list]:
    stack_hpo = []
    stack_ordo = []
    row = iteration_row[1].to_list()
    processing_line = ProcessingLine(row)
    if len(processing_line.hpos) > 0 or len(processing_line.ordos) > 0:
        processing_line.pubtator = _pubtator_responce(processing_line.id)
        if processing_line.pubtator:
            try:
                temp = _pairs_with_negation(processing_line.hpos, processing_line.dots, processing_line.pubtator,
                                            processing_line.text)
                if len(temp[0]) > 0:
                    for temp_element in temp:
                        stack_hpo.append(temp_element)
            except Exception:
                pass
            try:
                temp = _pairs_with_negation(processing_line.ordos, processing_line.dots, processing_line.pubtator,
                                            processing_line.text)
                if len(temp[0]) > 0:
                    for temp_element in temp:
                        stack_ordo.append(temp_element)
            except Exception:
                pass
    return stack_hpo, stack_ordo


@timeit
def execute():
    with open(Path(args.finished_tsv_path), 'r', encoding='utf-8') as f:
        reports_with_terms = pd.read_csv(f, delimiter='\t', header=None, dtype=str)
    num_cores = os.cpu_count()
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores-1) as executor:
        result = list(executor.map(_process_df, reports_with_terms.iterrows(), chunksize=num_cores))

    return result


if __name__ == '__main__':
    args = parser.parse_args()
    df_hpo = pd.DataFrame(columns=["Gene", "HPO_id", "HPO_term", "Frequency", "Total", "N"])
    df_ordo = pd.DataFrame(columns=["Gene", "ORDO_id", "ORDO_term", "Frequency", "Total", "N"])
    for i in execute():
        for el in i[0]:
            _update_frame(el, df_hpo, 'HPO')
        for el in i[1]:
            _update_frame(el, df_ordo, 'ORDO')
    _count_frequencies(df_hpo)
    _count_frequencies(df_ordo)
    df_hpo.to_csv(Path(args.hpo_table_path), sep='\t', index=False)
    df_ordo.to_csv(Path(args.ordo_table_path), sep='\t', index=False)
