import fileinput
from glob import glob
import re
import collections
import pickle
import json
import msgpack
import time

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

generateFiles = raw_input("Do you want to tokenize the Reuters collection?: (y/n) ")
if generateFiles.lower() == 'y':

    #config settings

    PRODUCTION_MODE = False
    DEBUGGING = True
    isCompressed = raw_input("Do you want to apply compression techniques?: (y/n) ")
    compressing = True if isCompressed.lower() == 'y' else False

    if PRODUCTION_MODE:
        inputDirectory = 'reuters-split'
        outputDirectory = 'inverted-index' + ("-compressed" if compressing else "-uncompressed")
    else:
        inputDirectory = "reuters-test-input"
        outputDirectory = "reuters-test-output"

    #other settings
    inputFileList = glob(inputDirectory + '/reuters-*.sgm')
    numberOfFiles = len(inputFileList)
    numberOfBlocks = int(input("How many blocks would you wish to use: "))
    blocks = partitionList(inputFileList, numberOfBlocks)

    fileNumber = 1
    blockNumber = 1
    stopwords = open("stopwords.sgm", 'r').read().split("\n")
    sumOfDocLengths = 0

    globalStart = time.clock()
    globalCount = 0

    for block in blocks:

        blockStartTime = time.clock()

        if DEBUGGING:
            print('*****************************************************')

        print("Started processing block " + str(blockNumber))
        invertedIndex = {}

        for file in block:

            if DEBUGGING:
                start = time.clock()
                words_raw = open(file, 'r').read()
                end = time.clock()
                print("\nFile open operation took " + str((end-start)*1000) + " milliseconds")
            else:
                words_raw = open(file, 'r').read()

            if compressing:

                if DEBUGGING:
                    start = time.clock()
                    words_filtered = re.sub("&#[0-9]+;*", "", words_raw) #filters out ASCII symbols of the form &#{number};
                    end = time.clock()
                    print("words_filtered took " + str((end-start)*1000) + " milliseconds")
                else:
                    words_filtered = re.sub("&#[0-9]+;*", "", words_raw) #filters out ASCII symbols of the form &#{number};

                if DEBUGGING:
                    start = time.clock()
                    words = re.split('[^a-zA-Z]+', words_filtered) # filters out newlines, numbers and punctuation
                    end = time.clock()
                    print("Filtering newlines, numbers and punctuation took  " + str((end-start)*1000) + " milliseconds")
                else:
                    words = re.split('[^a-zA-Z]+', words_filtered) # filters out newlines, numbers and punctuation

                # Removes all content before the <BODY> tag and after the </BODY> tag (basically removes all the XML tags)

                if DEBUGGING:
                    start = time.clock()
                    if "BODY" in words:
                        openingBodyTag = words.index("BODY")
                        words = words[openingBodyTag + 1:]
                        closingBodyTag = words.index("BODY")
                        words = words[:closingBodyTag]
                    end = time.clock()
                    print("BODY filtering took " + str((end-start)*1000) + " milliseconds")
                else:
                    if "BODY" in words:
                        openingBodyTag = words.index("BODY")
                        words = words[openingBodyTag + 1:]
                        closingBodyTag = words.index("BODY")
                        words = words[:closingBodyTag]

                if DEBUGGING:
                    start = time.clock()
                    words = [x.lower() for x in words] # convert words to lowercase
                    end = time.clock()
                    print("lowercasing took " + str((end-start)*1000) + " milliseconds")
                else:
                    words = [x.lower() for x in words] # convert words to lowercase

                if DEBUGGING:
                    start = time.clock()
                    words = [x for x in words if x not in stopwords] # remove stopwords
                    end = time.clock()
                    print("removing stopwords took " + str((end-start)*1000) + " milliseconds")
                else:
                    words = [x for x in words if x not in stopwords] # remove stopwords

                if DEBUGGING:
                    start = time.clock()
                    words = [x for x in words if x not in ''] # filters out empty strings
                    end = time.clock()
                    print("filtering empty strings took " + str((end-start)*1000) + " milliseconds")
                else:
                    words = [x for x in words if x not in ''] # filters out empty strings

            else:
                words = re.split('\W+', words_raw) # only filters out newlines and punctuation
                words = [x for x in words if x not in ''] # filters out empty strings

            # Store the document length as the number of indexed words in the document. Results differ based on whether or not compression is applied
            documentLength = len(words)
            sumOfDocLengths += documentLength

            # Create the inverted index (using a dictionary<string, list> data structure)
            if DEBUGGING:
                start = time.clock()
                for word in words:
                    if word in invertedIndex:
                        if invertedIndex[word][-1]['docID'] == fileNumber:
                            invertedIndex[word][-1]['tf'] += 1
                        else:
                            invertedIndex[word].append({'docID': fileNumber, 'tf': 1, 'docLength': documentLength})
                    elif word not in invertedIndex:
                        invertedIndex[word] = [{'docID': fileNumber, 'tf': 1, 'docLength': documentLength}]
                end = time.clock()
                print("Creating the inverted index took " + str((end-start)*1000) + " milliseconds")
            else:
                for word in words:
                    if word in invertedIndex:
                        if invertedIndex[word][-1]['docID'] == fileNumber:
                            invertedIndex[word][-1]['tf'] += 1
                        else:
                            invertedIndex[word].append({'docID': fileNumber, 'tf': 1, 'docLength': documentLength})
                    elif word not in invertedIndex:
                        invertedIndex[word] = [{'docID': fileNumber, 'tf': 1, 'docLength': documentLength}]

            #if PRODUCTION_MODE:
            #    if fileNumber % 100 == 0:
            #        print("Finished processing file " + str(fileNumber))

            fileNumber += 1

        # Bookkeeping sent to a separate file
        averageDocumentLength = float(sumOfDocLengths) / float(numberOfFiles)
        if PRODUCTION_MODE:
            output = open('inverted-index-result/data.txt', 'wb')
            pickle.dump(numberOfFiles, output)
            pickle.dump(averageDocumentLength, output)
            output.close()

        # Sort the inverted index and write to disk (as binary data)
        if DEBUGGING:
            start = time.clock()
            sortedIndex = map(list, sorted(invertedIndex.items())) # The sorted index is a list of lists instead of a dictionary
            end = time.clock()
            print("Sorting the inverted index took " + str((end-start)*1000) + " milliseconds")
        else:
            sortedIndex = map(list, sorted(invertedIndex.items())) # The sorted index is a list of lists instead of a dictionary

        if DEBUGGING:
            start = time.clock()
            with open(outputDirectory + '/inverted-index-block-' + str(blockNumber).zfill(3) + '.txt', 'wb') as output:
                byteData = msgpack.packb(sortedIndex)
                pickle.dump(byteData, output)
            end = time.clock()
            print("Writing block " + str(blockNumber) + " took " + str((end-start)*1000) + " milliseconds")
            print('*****************************************************')
        else:
            with open(outputDirectory + '/inverted-index-block-' + str(blockNumber).zfill(3) + '.txt', 'wb') as output:
                byteData = msgpack.packb(sortedIndex)
                pickle.dump(byteData, output)

        blockEndTime = time.clock()
        globalCount += blockEndTime-blockStartTime
        print("Processing block " + str(blockNumber) + " took " + str((blockEndTime-blockStartTime)) + " seconds")

        blockNumber += 1

    globalEnd = time.clock()
    print("Tokenizing took " + str(globalEnd-globalStart) + " seconds")
    print("Another measure gives " + str(globalCount) + " seconds")