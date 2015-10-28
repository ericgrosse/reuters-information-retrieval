print("You can run one of the following files:\n")
print("1: reuters_split.py")
print("2: reuters_tokenizer.py")
print("3: reuters_merge.py")
print("4: reuters_query.py")
print("5: All of the above\n")
userInput = input("Which file do you wish to run? ")

if(userInput == "1"):
   import reuters_split
if(userInput == "2"):
    import reuters_tokenizer
if(userInput == "3"):
    import reuters_merge
if(userInput == "4"):
    import reuters_query
if(userInput == "5"):
    import reuters_split
    import reuters_tokenizer
    import reuters_merge
    import reuters_query