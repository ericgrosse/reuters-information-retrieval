import fileinput
from glob import glob
import re
import collections
import pickle
from itertools import chain
import math
import json
import time
import msgpack

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
    start = time.time()
    for file in inputFile:
        with open(file, 'rb') as content:
            byteData = pickle.load(content)
            invertedIndex = msgpack.unpackb(byteData)
    end = time.time()
    print("Finished loading the inverted index")
    print("Operation took " + str(end-start) + " seconds\n")

    while True:
        searchString = raw_input('Please enter a search query: ')
        queryWords = re.split('\W+', searchString)

        if compressing:
            queryWords = [x for x in queryWords if x not in stopwords] # remove stopwords
            queryWords = [x for x in queryWords if x not in ""] # remove empty strings
            queryWords = [x.lower() for x in queryWords] # lowercase each word

        # Compute the term frequency of the query terms
        queryWordFrequencies = {}
        for word in queryWords:
            if word in queryWordFrequencies:
                queryWordFrequencies[word] += 1
            else:
                queryWordFrequencies[word] = 1

        # Filter out identical words
        queryWords = list(set(queryWords))

        matches = []
        docIDMatches = []
        hasMatches = False

        for word in queryWords:
            hasMatches = False
            for key, value in invertedIndex:
                if word == key:
                    hasMatches = True # If a match is found at some point, flip the boolean
                    matches.append((key, value))
                    # Crude way of extracting all the docIDs into a list
                    docIDs = []
                    for el in value:
                        docIDs.append(el['docID'])
                    docIDMatches.append(set(docIDs))
            if not hasMatches: # If the boolean was never flipped, the word was not found in the inverted index. This means the intersection will return nothing, so search returns empty
                break

        if len(docIDMatches) > 0:
            docIDMatches = set.intersection(*docIDMatches)

        if(hasMatches and len(docIDMatches) > 0):

            matchedWords = [key for key, value in matches]

            '''for el in matches:
                print(el)'''

            #output = open('reuters-test-input/test-data.txt','wb')
            #pickle.dump(matches, output)

            #print(matchedWords)
            #print(docIDMatches)

            #Perform the Okapi BM25 calculations
            rankedList = []

            for docNumber in docIDMatches:

                RSVd = 0
                print("Current doc: " + str(docNumber))

                for word in matchedWords:

                    print("Current word: " + word)
                    # per word data
                    wordData = [myTuple[1] for myTuple in matches if myTuple[0] == word][0]
                    docFrequency = len(wordData)

                    # per document data
                    documentData = [x for x in wordData if x['docID'] == docNumber][0]
                    docLength = documentData['docLength']
                    termFrequency = documentData['tf']

                    # constants
                    k1 = 1.5
                    k3 = 1.5
                    b = 0.75

                    product1 = math.log10( float(numberOfDocs) / float(docFrequency) )
                    product2 = ( (k1 + 1) * termFrequency ) / float( k1 * ( (1 - b) + b * ( float(docLength) / avgDocLength) ) + termFrequency )
                    product3 = ( (k3 + 1) * queryWordFrequencies[word] ) / float( k3 + queryWordFrequencies[word] )

                    productResult = product1 * product2 * product3
                    RSVd += productResult

                    print("********************")
                    print("word: " + word)
                    print("********************")
                    print("Doc ID: " + str(docNumber))
                    #print("product1: " + str(product1))
                    #print("product2: " + str(product2))
                    #print("product3: " + str(product3))
                    print("Partial RSVd: " + str(productResult))
                    print("Doc length: " + str(docLength))
                    print("Term frequency " + str(termFrequency))
                    print("Document frequency " + str(docFrequency))
                print("")
                rankedList.append((docNumber,RSVd))

            print("Constants:")
            print("Total number of docs: " + str(numberOfDocs))
            print("Average doc length: " + str(avgDocLength))
            print("")

            rankedList.sort(key=lambda x: x[1], reverse=True)
            print("Results for \'" + searchString + "\': ")
            for key, value in rankedList:
                print("Document " + str(key) + "\tScore: " + str(value))
            print("")
        else:
            print("No matches returned\n")