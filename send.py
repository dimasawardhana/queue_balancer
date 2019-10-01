import csv, json, pika, sys

def produce(queue_name, content):
    msg = json.dumps(content)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(exchange='', routing_key=queue_name,body=msg)
    print content["id"]," sent succesfully"
    
if __name__ == "__main__":
    
    csvSet = {
        "100" : "./dataset/data_100.csv",
        "8000" : "./dataset/data_8000.csv",
        "4000" : "./dataset/data_4000.csv",
        "articles1": "./dataset/articles1.csv",
        "articles2": "./dataset/articles2.csv",
        "articles3": "./dataset/articles3.csv"
    }
    csvPath = csvSet[sys.argv[1]]
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    queue_name = sys.argv[2]
    with open(csvPath) as csvFile:
        csv.field_size_limit(10000000)
        csvReader = csv.DictReader(csvFile)
        for row in csvReader:
            jsonData = {}
            _id = row["id"]
            content = unicode(row["content"], errors="ignore")
            jsonData["id"] = _id
            jsonData["content"] = content
            produce(queue_name, jsonData)
    pass
