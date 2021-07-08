import requests
import json
import os
import pandas as pd
import argparse

from pymed import PubMed
from pathlib import Path

pubmed = PubMed(tool="PubMedLoad", email="t@ttt.com")
parser = argparse.ArgumentParser()
parser.add_argument('--output_file', type=str, dest='outfile', required=True)


if __name__ == "__main__":
    args = parser.parse_args()
    search_term = '"case reports" [publication type] OR "case reports" [ti] OR "case report" [ti]'
    results = pubmed.query(search_term, max_results=2500000)
    articlesList = []
    articlesInfo = []
    for article in results:
        articleDict = article.toDict()
        articlesList.append(articleDict)
    for article in articlesList:
        pubmedId = article['pubmed_id'].partition('\n')[0]
        articlesInfo.append({u'pubmed_id':pubmedId,
                           u'title':article['title'],
                           u'abstract':article['abstract']})
    articlesPD = pd.DataFrame.from_dict(articleInfo)
    export_csv = articlesPD.to_csv(Path(args.outfile), sep = '\t', index = None, header=True)
