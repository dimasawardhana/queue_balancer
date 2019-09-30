import csv, json, pika, sys

def produce(queue_name, content):
    msg = json.dumps(content)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(exchange='', routing_key=queue_name,body=msg)
    print content["id"]," sent succesfully"
    
if __name__ == "__main__":
    csvPath = "./dataset/data_10.csv"
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    queue_name = ["queue_coba", "queue_1", "queue_2", "queue_3"]
    i = 0
    with open(csvPath) as csvFile:
        csv.field_size_limit(10000000)
        csvReader = csv.DictReader(csvFile)
        for row in csvReader:
            jsonData = {}
            _id = row["id"]
            content = unicode(row["content"], errors="ignore")
            jsonData["id"] = _id
            jsonData["content"] = content
            produce(queue_name[i], jsonData)
            i += 1
            if i > len(queue_name)-1:
                i=0
    pass
