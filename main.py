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
import sys
import copy

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
def visitAllSubdirectories(rootFolder):
    result = []
    for name in glob(rootFolder + "/*"):
        partition = name.split("/")
        if "." not in partition[-1]:
            result += visitAllSubdirectories(name)
        else:
            result.append(name)
    return result

generateFiles = raw_input("Do you want to perform sentiment analysis on your collection?: (y/n) ")
if generateFiles.lower() == 'y':

    #config settings
    PRODUCTION_MODE = True

    if PRODUCTION_MODE:
        inputDirectory = 'web-documents'
        outputDirectory = 'generated-files'
    else:
        inputDirectory = "test-input"
        outputDirectory = "test-output"

    #other settings
    departments = {
        'bcee': [],
        'computer-science-software-engineering': [],
        'electrical-computer': [],
        'eng-society': [],
        'info-systems-eng': [],
        'mechanical-industrial': []
    }
    departmentScores = {
        'bcee': 0,
        'computer-science-software-engineering': 0,
        'electrical-computer': 0,
        'eng-society': 0,
        'info-systems-eng': 0,
        'mechanical-industrial': 0
    }
    departmentAverageScores = copy.deepcopy(departmentScores)

    inputFileList = visitAllSubdirectories(inputDirectory)

    for file in inputFileList:
        department = file.split("\\")[1]
        if department in departments.keys():
            departments[department].append(file)

    stopwords = open("supporting-files/stopwords.sgm", 'r').read().split("\n")
    aFinn = 'supporting-files/AFINN/AFINN-111.txt'

    startTotal = time.time()

    # The main body
    for department, files in dict.iteritems(departments):
        start = time.time()
        invertedIndex = []
        totalScore = 0
        numberOfFiles = len(files)

        for file in files:

            words_raw = open(file, 'r').read()
            words = parseHTML(words_raw)
            words = re.split('[^a-zA-Z]+', words) # filters out newlines, numbers and punctuation
            words = [x.lower() for x in words] # convert words to lowercase
            words = [x for x in words if x not in stopwords] # filter out stopwords
            words = [x for x in words if x not in ""] # filters out the empty string

            # Compute the sentiment score for the document
            docString = ' '.join(words)
            sentimentScore = sentiment(docString, aFinn)
            totalScore += sentimentScore
            invertedIndex.append([file, sentimentScore])

        departmentScores[department] = totalScore
        departmentAverageScores[department] = float(totalScore) / float(numberOfFiles)

        with open(outputDirectory + '/inverted-index-' + department + '.txt', 'wb') as output:
            pickle.dump(invertedIndex, output)
        # Write the raw output as well
        with open(outputDirectory + '/raw-inverted-index-' + department + '.txt', 'wb') as output:
            for elem in invertedIndex:
                output.write(str(elem) + "\n")

        end = time.time()
        print("Finished processing files for department " + department + " in " + str(end-start) + " seconds")

    endTotal = time.time()
    print("\nThe full preprocessing took " + str(endTotal-startTotal) + " seconds")

    sortedDepartmentScores = sorted(departmentScores.items(), key=lambda x: x[1], reverse=True)
    sortedAverageDepartmentScores = sorted(departmentAverageScores.items(), key=lambda x: x[1], reverse=True)

    print("\nDepartment Scores:\n")
    for items in sortedDepartmentScores:
            print(items[0] + ": " + str(items[1]))

    print("\nDepartment Average Scores:\n")
    for items in sortedAverageDepartmentScores:
            print(items[0] + ": " + str(items[1]))