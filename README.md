# BigData - Spark RDD APIs

we need to read list of documents to find some answering a user’s query

* the application receives Inverted Index Construction by read the content of the files into a single RDD , and then to do the required mapping 
* Word, Count(Word), Doument_list:


MY OUTPUT 

<img width="299" alt="image" src="https://user-images.githubusercontent.com/100388300/196514048-0ca7242f-21f8-414c-8765-b7ea84e25a92.png">

first col . Word: one single word from this collection of files
seconde col Count(Word): the number of documents containing Word
last col Doument_list: a list of documents containing (Word)


# Query Processing: 

the application Returns the name of the file containing the searched word

<img width="105" alt="image" src="https://user-images.githubusercontent.com/100388300/196519209-c1e38ca1-e9e7-4365-8eff-ccb6165885d0.png">
<img width="159" alt="image" src="https://user-images.githubusercontent.com/100388300/196519246-e7224c9e-d31f-41b7-86f5-b44cf5e43b96.png">

