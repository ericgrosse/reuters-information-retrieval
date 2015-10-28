'''

Splits the 22 reuters files into 21,578 files, splitting each file based on a new opening and closing <REUTERS> tag

'''

import fileinput
from glob import glob

generateFiles = input("Do you want to split the Reuters collection?: (y/n) ")

if generateFiles.lower() == 'y':
    outputBase = 'reuters-split/reuters-'
    fileNumber = 0

    fnames = glob('reuters/reut2-0*.sgm')

    for file_line in fnames:

        fileNumber += 1

        input = open(file_line, 'r').read().split('\n')
        outputData = ""

        output = open(outputBase + str(fileNumber).zfill(5) + '.sgm', 'w')

        for line in range(len(input)):

            if(input[line] != "<!DOCTYPE lewis SYSTEM \"lewis.dtd\">" ): # skip DOCTYPE declaration

                if( (line == len(input)  - 2 and input[line] == "</REUTERS>") or (line == len(input)  - 1 and input[line] == "</REUTERS>") ): # every file ends with </REUTERS> and then an empty line
                    outputData = input[line] # do not print newline for last line
                    output.write(outputData)
                    output.close()
                    break
                elif(input[line] != "</REUTERS>"):
                    outputData = input[line] + "\n"
                    output.write(outputData)
                elif(input[line] == "</REUTERS>"): # output is </REUTERS>
                    outputData = input[line] # do not print newline for last line
                    output.write(outputData)
                    output.close()
                    fileNumber += 1
                    if fileNumber % 10 == 0:
                        print("Generated " + str(fileNumber) + " files")
                    output = open(outputBase + str(fileNumber).zfill(5) + '.sgm', 'w')

    print("File splitting complete")