import csv

# data = open("customers-100.csv",mode="r")
# csv_file = csv.reader(data)
# csv_file = list(csv_file)
# print(csv_file[1])
def getAllEmails():
    data = open("customers-100.csv",mode="r")
    csv_file = csv.reader(data)
    csv_file = list(csv_file)
    print(len(csv_file[0]))
    emails = []
    for line in csv_file:
        emails.append(line[9])
    print(emails)
    data.close()
    

def getAllNames():
    data = open("customers-100.csv",mode="r")
    csv_file = csv.reader(data)
    csv_file = list(csv_file)
    full_names = []
    for line in csv_file[1:]:
        full_names.append(line[2]+" "+line[3])
    print(full_names)
    data.close()

def writeCSV():
    data = open("customers-100.csv",mode="r")
    csv_file = csv.reader(data)
    csv_file = list(csv_file)

    try:
        newFile = open("sortedcsv.csv", mode="x",newline="")
    except FileExistsError:
        print("Error: The file 'sortedcsv.csv' already exists.")
        newFile = open("sortedcsv.csv", mode="w",newline="")

    csv_writer = csv.writer(newFile,delimiter=",")
    for line in csv_file:
        if line == csv_file[0]:
            csv_writer.writerow([line[9],"name"])
        else:
            csv_writer.writerow([line[9],line[2]+" "+line[3]])
    
    data.close()
    newFile.close()

getAllEmails()
getAllNames()
writeCSV()