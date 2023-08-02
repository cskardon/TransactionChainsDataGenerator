import datetime
import math
import random
import uuid
import argparse
import os
from pathlib import Path
import sys
import time
import concurrent.futures
import traceback

from generator import (
    generateAmount,
    generateDatetime,
    generateFileName,
    generateParty,
    generatePartyPair,
)
from writer import writeGraph, writeCSV

parties = []
transactions = []


def main():
    root_path = Path.cwd()

    my_parser = argparse.ArgumentParser(description="Generate Mock Transactions Data")
    my_parser.add_argument(
        "-o", "--output", help="the output path for the files", required=True
    )
    my_parser.add_argument(
        "-tc",
        "--transaction-count",
        help="the number of transactions to generate",
        type=int,
        required=True,
    )
    my_parser.add_argument(
        "-pc",
        "--parties-count",
        help="the number of parties to generate"
        "default is 10%% of transaction count",
        type=int,
        required=False,
    )
    my_parser.add_argument(
        "-mdb",
        "--max-days-before",
        help="the number of days over which transactions will be distributed (default is 3)",
        required=False,
        type=int,
        default=3,
    )
    my_parser.add_argument(
        "-thc",
        "--thread-count",
        help="the number of threads to use for writing the files (default is 20)",
        required=False,
        type=int,
        default=20,
    )
    my_parser.add_argument(
        "-fc",
        "--files-count",
        help="the number of files over which to distribute the transactions",
        required=True,
    )
    my_parser.add_argument(
        "-sp",
        "--suspicious-percentage",
        help="the percentage of suspicious transactions (default is 0.0001 = 0.01%%)",
        required=False,
        type=float,
        default=0.001,  # 0.1%
    )
    my_parser.add_argument(
        "-cc",
        "--community-count",
        help="the number of communities to generate",
        required=False,
        type=int,
        default=1
    )

    args = my_parser.parse_args()

    # availableTx = args.transaction_count
    # maxTx = availableTx / args.community_count
    # minTx = 4
    
    # availableParties = args.parties_count
    # maxParties = int(availableParties / args.community_count)
    # minParties = 2
    # if maxParties <= minParties:
    #     maxParties = minParties + 1
    # print("===============>> >> MaxP ", maxParties)

    # while availableTx > 0:
    #     tx = random.randrange(minTx, maxTx)
    #     if tx > availableTx:
    #         tx = availableTx
    #     availableTx -= tx
    #     print("==============>> Creating ", tx, " transactions, there are now ", availableTx, " transactions left")

    #     if availableTx == 0:
    #         partiesCount = availableParties
    #         availableParties -= partiesCount
    #     else:
    #         partiesCount = random.randrange(minParties, maxParties)
    #         if partiesCount > availableParties:
    #             partiesCount = availableParties
    #         if availableParties - partiesCount < minParties:
    #             partiesCount += 1 
    #         availableParties -= partiesCount
    #     print("==============>> Creating ", partiesCount, " parties, there are now ", availableParties, " parties left")

    tx = int(args.transaction_count / args.community_count)
    partiesCount = int(args.parties_count / args.community_count)
    
    print("Tx per community: ", tx, " Parties per community: ", partiesCount)

    for _ in range(0, args.community_count):
        txVal = tx
        partiesVal = partiesCount
        if random.random() > 0.5:
            txVal += 1
            partiesVal += 1

        invoke_command(
            args.output,
            txVal,
            partiesVal,
            args.max_days_before,
            args.thread_count,
            args.suspicious_percentage,
            args.files_count,
        )


def invoke_command(
    output_path,
    transaction_count,
    parties_count,
    max_days_before,
    thread_count,
    suspicious_percentage,
    number_of_files,
):
    global parties
    print(f"The output path is \"{output_path}\"")
    if not os.path.isdir(output_path):
        print("The specified path does not exist: " + str(output_path))
        sys.exit()

    fileFormat = "nt"
    fileExtension = "csv" #"nt"

    rows = int(transaction_count)
    if parties_count:
        partyCount = int(parties_count)
    else:
        partyCount = int(transaction_count) // 10
    batch_size = int(rows / int(number_of_files))

    now = datetime.datetime.utcnow()
    print("Generating: " + str(rows) + " Transactions.")
    print("Estimated batch size:" + str(batch_size))
    print("Start process. " + str(datetime.datetime.now()))
    start = time.time()

    maxDaysBefore = max_days_before
    print("Generating Parties")
    localParties = generate_parties(partyCount)
    print("Generating Transactions")
    generate_transactions(rows, now, maxDaysBefore, localParties)
    print("Generating suspicious Parties")
    localParties = generate_suspicious_parties(int(partyCount * suspicious_percentage), localParties)
    parties += localParties
    generate_files(batch_size, output_path, fileExtension, fileFormat, thread_count)
    print("Finished. " + str(datetime.datetime.now()))
    end = time.time()
    print("Total execution time: " + str(end - start) + " Seconds")
    print("Total execution time: " + str((end - start) / 60) + " Minutes")


def generate_parties(partyCount):
    # Generate parties
    localParties = []
    for i in range(partyCount):
        internal = random.choice(["Y", "N"])
        party = {
            **generateParty(),
            "internal": internal,
            "isSuspicious": "N",
            "exited": random.choices(
                ["N", "Y" if internal == "N" else "N"], weights=[199, 1]
            )[0]
        }
        localParties.append(party)
    return localParties


def generate_transactions(rows, now, maxDaysBefore, localParties):
    # Generate transactions
    for i in range(rows):
        partyPair = generatePartyPair(localParties)

        transactions.append(
            {
                "id": uuid.uuid4().hex,
                "amount": generateAmount(),
                "date": generateDatetime(now, maxDaysBefore).isoformat(),
                "originator": partyPair[0],
                "beneficiary": partyPair[1],
            }
        )


def generate_suspicious_parties(rows, parties):
    print("Updating {0} suspicious parties".format(rows))
    for i in range(rows):
        update_party = True
        while update_party:
            party = random.choice(parties)
            if party["isSuspicious"] == "N":
                party["isSuspicious"] = "Y"
                update_party = False
    return parties

def generate_files(batch_size, outputPathCore, fileExtension, fileFormat, thread_count):
    print("Writing files to : " + str(outputPathCore))
    with concurrent.futures.ProcessPoolExecutor(max_workers=thread_count) as executor:
        for batchIndex in range(math.ceil(len(parties) / batch_size)):
            batchStart = batchIndex * batch_size
            batchEnd = batchStart + batch_size
            partyBatch = parties[batchStart:batchEnd]
            outputPath = generateFileName(
                str(outputPathCore), "parties", batchIndex, fileExtension
            )
            executor.submit(
                write_files_multi_threaded,
                "Thread #{0}".format(batchIndex),
                outputPath,
                fileFormat,
                partyBatch,
                [],
            )

    with concurrent.futures.ProcessPoolExecutor(max_workers=thread_count) as executor:
        for batchIndex in range(math.ceil(len(transactions) / batch_size)):
            batchStart = batchIndex * batch_size
            batchEnd = batchStart + batch_size
            transactionBatch = transactions[batchStart:batchEnd]
            outputPath = generateFileName(
                str(outputPathCore), "transactions", batchIndex, fileExtension
            )
            executor.submit(
                write_files_multi_threaded,
                "Thread #{0}".format(batchIndex),
                outputPath,
                fileFormat,
                [],
                transactionBatch,
            )


def write_files_multi_threaded(
    threadName, outputPath, fileFormat, partyBatch, transactionBatch
):
    try:
        print("thread {0} is working on file:".format(threadName))
        print(outputPath)
        #writeGraph(str(outputPath), fileFormat, partyBatch, transactionBatch)
        writeCSV(str(outputPath), partyBatch, transactionBatch)
    except:
        traceback.print_exc()


if __name__ == "__main__":
    main()
