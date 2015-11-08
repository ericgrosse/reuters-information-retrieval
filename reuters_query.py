import fileinput
from glob import glob
import re
import collections
import pickle
from itertools import chain

generateFiles = raw_input("Do you want to run any queries?: (y/n) ")
if generateFiles.lower() == 'y':

    #config settings
    PRODUCTION_MODE = True
    isCompressed = raw_input("Do you want to run on the compressed inverted index (y) or the uncompressed one (n)?: ")
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

    input = open(inputDirectory + "/data.txt", 'rb')
    numberOfDocs = pickle.load(input)
    avgDocLength = pickle.load(input)
    input.close()

    print("Loading the inverted index from disk")
    for file in inputFile:
        invertedIndex = pickle.load(open(file, 'rb'))
    print("Finished loading the inverted index\n")

    while True:
        searchString = raw_input('Please enter a search query: ')
        queryWords = searchString.split(" ")

        if compressing:
            queryWords = [x for x in queryWords if x not in stopwords] # remove stopwords
            queryWords = [x.lower() for x in queryWords] # lowercase each word

        resultSet = set() # a set used for intersection operations
        for word in queryWords:
            for key, value in invertedIndex:
                if word == key:
                    # Crude way of extracting all the docIDs into a list
                    docIDs = []
                    for el in value:
                        docIDs.append(el['docID'])
                    if len(resultSet) == 0:
                        resultSet = set(docIDs)
                    else:
                        resultSet = resultSet & set(docIDs) # intersects the current resultSet with the postings list of the current word

        queryResult = list(resultSet)
        queryResult.sort()
        print(searchString + ": " + str(queryResult))