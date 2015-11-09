import fileinput
from glob import glob
import re
import collections
import pickle
import json

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

    PRODUCTION_MODE = True
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

    for block in blocks:

        invertedIndex = {}

        for file in block:

            words_raw = open(file, 'r').read()

            if compressing:

                words_filtered = re.sub("&#[0-9]+;*", "", words_raw) #filters out ASCII symbols of the form &#{number};
                words = re.split('[^a-zA-Z]+', words_filtered) # filters out newlines, numbers and punctuation

                # Removes all content before the <BODY> tag and after the </BODY> tag (basically removes all the XML tags)

                if "BODY" in words:
                    openingBodyTag = words.index("BODY")
                    words = words[openingBodyTag + 1:]
                    closingBodyTag = words.index("BODY")
                    words = words[:closingBodyTag]

                words = [x.lower() for x in words] # convert words to lowercase
                words = [x for x in words if x not in stopwords] # remove stopwords
                words = [x for x in words if x not in ''] # filters out empty strings

            else:
                words = re.split('\W+', words_raw) # only filters out newlines and punctuation
                words = [x for x in words if x not in ''] # filters out empty strings

            # Store the document length as the number of indexed words in the document. Results differ based on whether or not compression is applied
            documentLength = len(words)
            sumOfDocLengths += documentLength

            # Create the inverted index (using a dictionary<string, list> data structure)
            for word in words:
                if word in invertedIndex:
                    if invertedIndex[word][-1]['docID'] == fileNumber:
                        invertedIndex[word][-1]['tf'] += 1
                    else:
                        invertedIndex[word].append({'docID': fileNumber, 'tf': 1, 'docLength': documentLength})
                elif word not in invertedIndex:
                    invertedIndex[word] = [{'docID': fileNumber, 'tf': 1, 'docLength': documentLength}]

            if PRODUCTION_MODE:
                if fileNumber % 10 == 0:
                    print("Finished processing file " + str(fileNumber))

            fileNumber += 1

        # Bookkeeping sent to a separate file
        averageDocumentLength = float(sumOfDocLengths) / float(numberOfFiles)
        if PRODUCTION_MODE:
            output = open('inverted-index-result/data.txt', 'wb')
            pickle.dump(numberOfFiles, output)
            pickle.dump(averageDocumentLength, output)
            output.close()

        # Sort the inverted index and write to disk (as binary data)
        sortedIndex = map(list, sorted(invertedIndex.items())) # The sorted index is a list of lists instead of a dictionary
        with open(outputDirectory + '/inverted-index-block-' + str(blockNumber).zfill(3) + '.txt', 'wb') as output:
            json.dump(sortedIndex, output)

        if PRODUCTION_MODE:
            print("\nFinished processing block " + str(blockNumber) + "\n")

        blockNumber += 1