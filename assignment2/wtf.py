#!/usr/local/bin/python
import math
def Okapi_TF(d, average):
    tf = d.information['tf']
    length = d.information['document_length']
    return tf / (tf + 0.5 + 1.5 * (length / average))
# ----------------------------------------------------------
def TF_IDF(d, average, D):
    df = d.information['df']
    return Okapi_TF(d, average) * math.log10(D / df)
# ----------------------------------------------------------
def Okapi_BM25(tfq, d, average, D):
    k1 = 1.2
    k2 = 100.0
    b = 0.75
    tf = d.information['tf']
    df = d.information['df']
    length = d.information['document_length']
    one = math.log10((D + 0.5) / (df + 0.5))
    two = (tf + k1 * tf) / (tf + k1 * (1 - b + (b * length) / average))
    three = (tfq + k2 * tfq) / (tfq + k2)
    return one * two * three
# ----------------------------------------------------------
def Laplace(d, V):
    tf = d.information['tf']
    length = d.information['document_length']
    p = float(tf + 1) / (length + V)
    p = math.log10(p)
    return p
def Laplace2(tf, length, V):
    # handle when the document was not hit tf = 0
    p = (tf + 1) / float((length + V))
    p = math.log10(p)
    return p
# ----------------------------------------------------------
def Jelinek_Mercer(d, back_tf, TOTAL_DOCUMENT_LENGTH):
    f = 0.37
    tf = d.information['tf']
    length = d.information['document_length']
    p = f * (tf / length) + (1 - f) * (back_tf  / TOTAL_DOCUMENT_LENGTH)
    p = math.log10(p)
    return p
def Jelinek_Mercer2(length, back_tf, TOTAL_DOCUMENT_LENGTH):
    # handle when the document was not hit
    f = 0.37
    tf = 0
    p = f * (tf / length) + (1 - f) * (back_tf  / TOTAL_DOCUMENT_LENGTH)
    try:
        p = math.log10(p)
    except ValueError:
        return p
    return p
# ----------------------------------------------------------

