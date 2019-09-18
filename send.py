import csv, json, pika

def produce(queue_name, content):
    msg = json.dumps(content)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(exchange='', routing_key=queue_name,body=msg)
    print content["id"]," sent succesfully"

if __name__ == "__main__":
    csvPath = "./dataset/data_6000.csv"
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    queue_name = "queue_1"
    with open(csvPath) as csvFile:
        csvReader = csv.DictReader(csvFile)
        for row in csvReader:
            jsonData = {}
            _id = row["id"]
            content = unicode(row["content"], errors="ignore")
            jsonData["id"] = _id
            jsonData["content"] = content
            produce(queue_name, jsonData)
    pass
