import fileinput
from glob import glob
import re
import collections
import pickle
import json
import msgpack
from itertools import chain

'''
Merges multiple sorted lists into a single sorted list
'''
def parallelMerge(args): # args expects a list of lists. Ex/ [ [ ("a", [4,23,55]), ("b", [3,12,59]) ], [ ("a", [33,44,55]), ("c", [12,82,106]) ] ]

    mergeResultIncrementor = 0
    mergeResult = []
    mergeResultLength = 0

    for lst in args:
        mergeResultLength += len(lst)

    while mergeResultIncrementor < mergeResultLength:

        min = ("zzzzzzzzzzzzzzzzzzzzzzzz", [0]) # Assign min a very high value so that it's guaranteed to be overridden right away
        position = -1 # Assign an impossible position for the same reason as above

        for index in range(len(args)):
            if args[index][0][0] < min[0]: # If the first element of the chosen list has a key less than the min's key
                min = args[index][0] # min is set to the entire tuple, ex/ ("a", [2,5,7])
                position = index

        if(len(mergeResult) > 0 and mergeResult[-1][0] == min[0]):
            mergeResult[-1][1].extend(min[1])
        else:
            mergeResult.append(min)

        args[position].pop(0)
        if len(args[position]) == 0:
            args.pop(position)

        mergeResultIncrementor += 1

    return mergeResult

generateFiles = raw_input("Do you want to merge each block into a single inverted index?: (y/n) ")
if generateFiles.lower() == 'y':

    #config settings

    PRODUCTION_MODE = True
    isCompressed = raw_input("Do you want to merge the compressed blocks (y) or the uncompressed ones (n): ")
    compressing = True if isCompressed.lower() == 'y' else False

    if PRODUCTION_MODE:
        inputDirectory = 'inverted-index' + ("-compressed" if compressing else "-uncompressed")
        outputDirectory = 'inverted-index-result'
    else:
        inputDirectory = "reuters-test-input"
        outputDirectory = "reuters-test-output"

    #other settings
    inputFileList = glob(inputDirectory + '/inverted-index-block-*.txt')

    masterList = []

    for file in inputFileList:
        print("Processing file " + str(file))

        with open(file, 'rb') as content:
            byteData = pickle.load(content)
            invertedIndex = msgpack.unpackb(byteData)

        masterList.append(invertedIndex)

    print("\nCreating the master inverted index...")
    masterInvertedIndex = parallelMerge(masterList)
    print("Master inverted index created")
    masterInvertedIndexFilename = outputDirectory + "/master-inverted-index" + ("-compressed" if compressing else "-uncompressed") + ".txt"
    print("\nWriting the master inverted index to " + masterInvertedIndexFilename)
    with open(masterInvertedIndexFilename,'wb') as output:
        byteData = msgpack.packb(masterInvertedIndex)
        pickle.dump(byteData, output)
    print("Finished writing to " + masterInvertedIndexFilename)