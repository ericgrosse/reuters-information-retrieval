import fileinput
from glob import glob
import re
import collections
import pickle
import json
import msgpack
import time
from bs4 import BeautifulSoup
import math
import nltk
from nltk import word_tokenize
import codecs

def sentiment(document, dictionaryPath):
    filenameAFINN = dictionaryPath
    afinn = dict(map(lambda (w, s): (w, int(s)), [
            ws.strip().split('\t') for ws in open(filenameAFINN) ]))
    pattern_split = re.compile(r"\W+")
    words = pattern_split.split(document.lower())
    sentiments = map(lambda word: afinn.get(word, 0), words)
    if sentiments:
        # How should you weight the individual word sentiments?
        # You could do N, sqrt(N) or 1 for example. Here I use sqrt(N)
        sentiment = float(sum(sentiments))/math.sqrt(len(sentiments))
    else:
        sentiment = 0
    return sentiment
def parseHTML(html):
    soup = BeautifulSoup(html, "html.parser")
    [s.extract() for s in soup('script')]
    return soup.get_text()
def partitionRule(incrementValue, compareValue, cutoff):
    """
    Helper function for partitionList. Decides whether the partition size should round up or down
    For example, if you want to split a list of size 15 into 10 separate lists, the size of each
    list would either be 1 or 2. The full split would end up being [2,2,2,2,2,1,1,1,1,1].
    The cutoff is found by the remainder of division of the list size and the partition size.
    Every value before the cutoff is rounded up, and every value after the cutoff is rouned down.
    """

    if(compareValue < cutoff):
        return incrementValue + 1
    else:
        return incrementValue
def partitionList(inputList, numberOfPartitions):
    """
    Partitions a single list into a desired number of partitions
    """

    listLength = len(inputList)
    partitionLength = int(listLength / numberOfPartitions)
    partitionRemainder = listLength % numberOfPartitions

    count = 0
    appendCount = 0
    newList = []

    while(count < listLength):
        offset = partitionRule(partitionLength, appendCount, partitionRemainder)
        newList.append(inputList[count:count + offset])
        count = count + offset
        appendCount = appendCount + 1
    return newList
def visitAllSubdirectories(rootFolder):
    result = []
    for name in glob(rootFolder + "/*"):
        partition = name.split("/")
        if "." not in partition[-1]:
            result += visitAllSubdirectories(name)
        else:
            result.append(name)
    return result

generateFiles = raw_input("Do you want to tokenize your collection?: (y/n) ")
if generateFiles.lower() == 'y':

    #config settings

    PRODUCTION_MODE = True

    if PRODUCTION_MODE:
        inputDirectory = 'web-documents'
        outputDirectory = 'inverted-index-blocks'
    else:
        inputDirectory = "test-input"
        outputDirectory = "test-output"

    #other settings
    inputFileList = visitAllSubdirectories(inputDirectory)
    numberOfFiles = len(inputFileList)
    numberOfBlocks = int(input("How many blocks would you wish to use: "))
    blocks = partitionList(inputFileList, numberOfBlocks)

    fileNumber = 1
    blockNumber = 1
    stopwords = open("supporting-files/stopwords.sgm", 'r').read().split("\n")
    aFinn = 'supporting-files/AFINN/AFINN-111.txt'
    sumOfDocLengths = 0

    startTotal = time.time()

    for block in blocks:
        start = time.time()
        invertedIndex = {}

        for file in block:

            words_raw = open(file, 'r').read()
            words = parseHTML(words_raw)
            words = re.split('[^a-zA-Z]+', words) # filters out newlines, numbers and punctuation
            words = [x.lower() for x in words] # convert words to lowercase
            words = [x for x in words if x not in stopwords] # filter out stopwords
            words = [x for x in words if x not in ""] # filters out the empty string

            # Compute the sentiment score for the document
            docString = ' '.join(words)
            sentimentScore = sentiment(docString, aFinn)

            # Store the document length as the number of indexed words in the document. Results differ based on whether or not compression is applied
            documentLength = len(words)
            sumOfDocLengths += documentLength

            # Create the inverted index (using a dictionary<string, list> data structure)
            for word in words:
                if word in invertedIndex:
                    if invertedIndex[word][-1]['docID'] == fileNumber:
                        invertedIndex[word][-1]['tf'] += 1
                    else:
                        invertedIndex[word].append({'docID': fileNumber, 'tf': 1, 'docLength': documentLength, 'sentiment': sentimentScore})
                elif word not in invertedIndex:
                    invertedIndex[word] = [{'docID': fileNumber, 'tf': 1, 'docLength': documentLength, 'sentiment': sentimentScore}]
            fileNumber += 1

        # Bookkeeping sent to a separate file
        averageDocumentLength = float(sumOfDocLengths) / float(numberOfFiles)

        output = open('inverted-index-result/data.txt', 'wb')
        pickle.dump(numberOfFiles, output)
        pickle.dump(averageDocumentLength, output)
        output.close()

        # Sort the inverted index and write to disk (as binary data)
        sortedIndex = map(list, sorted(invertedIndex.items())) # The sorted index is a list of lists instead of a dictionary

        with open(outputDirectory + '/inverted-index-block-' + str(blockNumber).zfill(3) + '.txt', 'wb') as output:
            byteData = msgpack.packb(sortedIndex)
            pickle.dump(byteData, output)
        # Write the raw output as well
        with open(outputDirectory + '/raw-inverted-index-block-' + str(blockNumber).zfill(3) + '.txt', 'wb') as output:
            json.dump(sortedIndex, output)

        end = time.time()
        print("Finished processing block " + str(blockNumber) + " in " + str(end-start) + " seconds")

        blockNumber += 1

    endTotal = time.time()
    print("\nThe full tokenization took " + str(endTotal-startTotal) + " seconds")