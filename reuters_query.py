import nltk
import fileinput
from glob import glob
import re
import collections
import pickle
from itertools import chain

generateFiles = input("Do you want to run any queries?: (y/n) ")
if generateFiles.lower() == 'y':

    #config settings

    PRODUCTION_MODE = True
    isCompressed = input("Do you want to run on the compressed inverted index (y) or the uncompressed one (n)?: ")
    compressing = True if isCompressed.lower() == 'y' else False

    if PRODUCTION_MODE:
        inputDirectory = 'inverted-index-result'
    else:
        inputDirectory = "reuters-test-input"

    #other settings

    if compressing:
        inputFile = glob(inputDirectory + '/master-inverted-index-compressed.txt')
    else:
        inputFile = glob(inputDirectory + '/master-inverted-index-uncompressed.txt')

    stopwords = open("stopwords.sgm", 'r').read().split("\n")

    for file in inputFile:
        invertedIndex = pickle.load(open(file, 'rb'))

        while True:
            searchString = input('Please enter a search query: ')
            queryWords = searchString.split(" ")

            if compressing:
                queryWords = [x for x in queryWords if x not in stopwords] # remove stopwords
                queryWords = [x.lower() for x in queryWords] # lowercase each word

            resultSet = set() # a set used for intersection operations
            for word in queryWords:
                if word in invertedIndex:
                    if len(resultSet) == 0:
                        resultSet = set(invertedIndex[word])
                    else:
                        resultSet = resultSet & set(invertedIndex[word]) # intersects the current resultSet with the postings list of the current word

            queryResult = list(resultSet)
            queryResult.sort()
            print(searchString + ": " + str(queryResult))