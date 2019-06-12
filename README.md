# OPIEC: An Open Information Extraction Corpus

<img src="img/opiec-logo.png" align="right" width=200>

## Introduction
OPIEC is an Open Information Extraction (OIE) corpus, consisted of more than 341M triples extracted from the entire English Wikipedia. Each triple from the corpus is consisted of rich meta-data: each token from the subj/obj/rel along with NLP annotations (POS tag, NER tag, ...), provenance sentence along with the dependency parse, original (golden) links from Wikipedia, sentence order, space/time, etc (for more detailed explanation of the meta-data, see [here](#metadata)). 

There are two major corpora released with OPIEC:

1. OPIEC: an OIE corpus containing hundreds of millions of triples.
2. WikipediaNLP: the entire English Wikipedia with NLP annotations.

For more details concerning the construction, analysis and statistics of the corpus, read the AKBC paper [*"OPIEC: An Open Information Extraction Corpus*"](https://arxiv.org/pdf/1904.12324.pdf). To download the data and get additional resources, please visit the [project page](https://www.uni-mannheim.de/dws/research/resources/opiec/).

## Pipeline 

<img src="img/opiec-pipeline.png" align="center" width=400>

## Citation

If you use any of these corpora, or use the findings from the paper, please cite: 

```
@inproceedings{gashteovski2019opiec,
  title={OPIEC: An Open Information Extraction Corpus},
  author={Gashteovski, Kiril and Wanner, Sebastian and Hertling, Sven and Broscheit, Samuel and Gemulla, Rainer},
  booktitle={Proceedings of the Conference on Automatic Knowledge Base Construction (AKBC)},
  year={2019}
}
```
