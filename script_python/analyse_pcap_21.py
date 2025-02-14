# Trouver les protocoles

from pyspark.sql import SparkSession
from scapy.all import PcapReader, IP, UDP, TCP, ICMP
import time
from io import BytesIO

# 1️⃣ Initialiser Spark avec des optimisations
spark = SparkSession.builder \
    .appName("Protocols") \
    .config("spark.sql.shuffle.partitions", "200") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext

start_time = time.time()

hdfs_folder_path = "hdfs://localhost:9000/user/pcap_files/"
pcap_rdd = sc.binaryFiles(hdfs_folder_path).repartition(50)

# Ether / IP / UDP 172.19.0.8:https > 172.19.0.27:53074 / Raw
def extract_protocols(pcap_file):
    protocol_counts = {}  # Utiliser un dictionnaire pour compter les occurrences
    protocol_test = set()
    try:
         with BytesIO(pcap_file) as f:
            for packet in PcapReader(f):  # Lecture en flux
                protocol_test.add(packet[IP].proto)
                if TCP in packet:
                    protocol_counts['TCP'] = protocol_counts.get('TCP', 0) + 1  # Compter les occurrences de TCP
                elif UDP in packet:
                    protocol_counts['UDP'] = protocol_counts.get('UDP', 0) + 1  # Compter les occurrences de UDP
                elif ICMP in packet:
                    protocol_counts['ICMP'] = protocol_counts.get('ICMP', 0) + 1  # Compter les occurrences d'ICMP
                else:
                    proto = packet[IP].proto
                    protocol_counts[proto] = protocol_counts.get(proto, 0) + 1  # Compter d'autres protocoles
    except Exception as e:
        print(f"Erreur lors du traitement du fichier PCAP : {e}")
    return protocol_counts

protocol_counts_rdd = pcap_rdd.flatMap(lambda x: extract_protocols(x[1]).items()) \
                               .reduceByKey(lambda a, b: a + b)


# protocol_counts_rdd = pcap_rdd.flatMap(lambda x: extract_protocols(x[1])) \
#                                .map(lambda proto: (proto, 1)) \
#                                .reduceByKey(lambda a, b: a + b)

# 5️⃣ Afficher les résultats
protocol_counts = protocol_counts_rdd.collect()
for proto, count in protocol_counts:
    print(f"Protocole: {proto} | Occurrences: {count}")


end_time = time.time()
execution_time = end_time - start_time

with open("time.txt", "w") as f:
    f.write(f"{execution_time}")


spark.stop()


