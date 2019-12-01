import subprocess

infileto = 58
inputfolderroot = "hdfs://santa-fe:47001/FakeNewsCorpus-Outputs/news_cleaned_partitioned/news_cleaned_2018_02_1300"
outputfolderroot = "hdfs://santa-fe:47001/FakeNewsCorpus-Outputs/KeywordsFromPartitions/news_cleaned_partitioned/news_cleaned_2018_02_1300"
logfolder = "/s/chopin/a/grad/joyghosh/Desktop/ds_project/logs/"

def savetext(text, filename):
    text_file = open(filename, "w+")
    text_file.write(text)
    text_file.close()

for filenum in range(4, infileto):
    inputfolder = inputfolderroot + str(filenum).zfill(3)
    outputfolder = outputfolderroot + str(filenum).zfill(3)
    jobname = "job-" + str(filenum).zfill(3)
    command = "spark-submit --supervise --driver-memory 2G --executor-memory 2G --num-executors 29 ReadCSVFileTry4.py "
    command = command + "{} {} {}".format(inputfolder, outputfolder, jobname)
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate() 
    logoutfile = logfolder + str(filenum).zfill(3) + ".out"
    logerrfile = logfolder + str(filenum).zfill(3) + ".err"
    if(output is None):
        output = ""
    if(error is None):
        error = ""

    savetext(output, logoutfile)
    savetext(error, logerrfile)
    print("ran for job " + jobname)
print("finished")

