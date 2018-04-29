import subprocess
import sys
import time


def submitSparkCommand(localPath, hdfsPath, queryPath, dataset, engine, numCores, s2rdf):
    if not hdfsPath.endswith("/"):
        hdfsPath += "/"

    if not localPath.endswith("/"):
        localPath += "/"

    command = ("spark-submit "
               + "--driver-memory 20g "
               + "--master spark://172.18.11.128:7077 "
               + "--class edu.purdue.worq.WORQQuery "
               + "target/uber-worq-1.0-SNAPSHOT.jar "
               + "-l " + localPath + " "
               + "-r hdfs://172.18.11.128:8020/user/amadkour/" + hdfsPath + " "
               + "-q " + queryPath + " "
               + "-b " + dataset + " "
               + "-e " + engine + " "
               + "-c " + numCores)
    # + "-s " + s2rdf)

    start = int(round(time.time()))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    end = int(round(time.time()))
    total = "{:.0f}".format(end - start)
    return total


if __name__ == "__main__":
    localPath = sys.argv[1]
    hdfsPath = sys.argv[2]
    queryPath = sys.argv[3]
    dataset = sys.argv[4]
    logfile = sys.argv[5]
    engine = sys.argv[6]
    numCores = sys.argv[7]
    s2rdf = sys.argv[8]

    LOG = open(logfile, "w")

    total = submitSparkCommand(localPath=localPath, hdfsPath=hdfsPath, queryPath=queryPath, dataset=dataset,
                               engine=engine, numCores=numCores, s2rdf=s2rdf)
    LOG.write("Query File : %s \n" % (queryPath))
    LOG.write("Time Taken: %s sec \n" % (total))

    LOG.close()
