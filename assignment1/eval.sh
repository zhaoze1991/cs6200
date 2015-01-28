#!/bin/bash
./trec_eval.pl  ./AP_DATA/qrels.adhoc.51-100.AP89.txt Okapi_TF > Okapi_TF_result
./trec_eval.pl  ./AP_DATA/qrels.adhoc.51-100.AP89.txt TF_IDF > TF_IDF_result
./trec_eval.pl  ./AP_DATA/qrels.adhoc.51-100.AP89.txt Okapi_BM25 > Okapi_BM25_result
./trec_eval.pl  ./AP_DATA/qrels.adhoc.51-100.AP89.txt Laplace_smoothing > Laplace_smoothing_result
./trec_eval.pl  ./AP_DATA/qrels.adhoc.51-100.AP89.txt Jelinek_Mercer > Jelinek_Mercer_result
