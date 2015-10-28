import nltk
import fileinput
from glob import glob
import re
import collections
import pickle
from itertools import chain

generateFiles = input("Do you want to merge each block into a single inverted index?: (y/n) ")
if generateFiles.lower() == 'y':

    #config settings

    PRODUCTION_MODE = True
    isCompressed = input("Do you want to merge the compressed blocks (y) or the uncompressed ones (n): ")
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

    masterOutputFilename = outputDirectory + "/_master-inverted-index-raw" + ("-compressed" if compressing else "-uncompressed") + ".txt"
    masterOutput = open(masterOutputFilename, "w")

    for file in inputFileList:
        print("Processing file " + str(file))
        invertedIndex = pickle.load(open(file, 'rb'))
        masterList = masterList + list(invertedIndex.items())

    print("\nCreating the master inverted index...")

    masterOrderedDictionary = collections.OrderedDict()
    for key, value in masterList:
        for item in value:
            masterOrderedDictionary.setdefault(key,[]).append(item)

    print("Master inverted index created")
    masterOrderedDictionaryFilename = outputDirectory + "/master-inverted-index" + ("-compressed" if compressing else "-uncompressed") + ".txt"

    print("\nWriting the master inverted index to " + masterOrderedDictionaryFilename)
    pickle.dump(masterOrderedDictionary, open(masterOrderedDictionaryFilename,'wb'))
    print("Finished writing to " + masterOrderedDictionaryFilename)

    print("\nWriting the master inverted index to " + masterOutputFilename)
    masterOutput.write(str(masterOrderedDictionary))
    masterOutput.close()
    print("Finished writing to " + masterOutputFilename + "\n")
