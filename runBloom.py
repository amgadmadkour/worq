import sys, getopt, subprocess, time, os, errno


def submitSparkCommand(localPath, hdfsPath, falsePositive, hashFunction):
    if not hdfsPath.endswith("/"):
        hdfsPath += "/"

    if not localPath.endswith("/"):
        localPath += "/"

    command = ("spark-submit "
               + "--driver-memory 16g "
               + "--master spark://172.18.11.128:7077 "
               + "--executor-memory 20g "
               + "--class edu.purdue.worq.WORQBloom "
               + "target/uber-worq-1.0-SNAPSHOT.jar "
               + "-l " + localPath + " "
               + "-r hdfs://172.18.11.128:8020/user/amadkour/" + hdfsPath + " "
               + "-t spark "
               + "-f " + falsePositive + " "
               + "-h " + hashFunction)

    start = int(round(time.time()))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    end = int(round(time.time()))
    total = "{:.0f}".format(end - start)
    return total


if __name__ == "__main__":
    localPath = sys.argv[1]
    hdfsPath = sys.argv[2]
    falsePositive = sys.argv[3]
    hashFunction = sys.argv[4]
    logfile = sys.argv[5]

    LOG = open(logfile, "w")

    total = submitSparkCommand(localPath=localPath, hdfsPath=hdfsPath, falsePositive=falsePositive,
                               hashFunction=hashFunction)
    LOG.write("Parameters : %s %s\n" % (falsePositive, hashFunction))
    LOG.write("Time Taken: %s sec \n" % (total))
    LOG.close()
