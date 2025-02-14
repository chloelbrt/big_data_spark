# Tentative d'analyse de l'attaque : 
# HTTP GET Flood: A bot continuously sends a high volume of HTTP/3 GET requests over a single QUIC connection, overwhelming 
# the server with requests and consuming resources. This attack creates a single intense bidirectional flow between the bot and 
# the server.


from pyspark.sql import SparkSession
from scapy.all import PcapReader, IP, UDP, Raw
import time
from io import BytesIO

# 1️⃣ Initialiser Spark avec des optimisations
spark = SparkSession.builder \
    .appName("HTTP3_GET_Flood_Attack") \
    .config("spark.sql.shuffle.partitions", "200") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext

start_time = time.time()

# 2️⃣ Charger les fichiers PCAP depuis HDFS avec plus de partitions pour parallélisme
hdfs_folder_path = "hdfs://localhost:9000/user/pcap_files/"
pcap_rdd = sc.binaryFiles(hdfs_folder_path).repartition(50)

# 3️⃣ Fonction pour extraire les paquets QUIC et détecter les requêtes HTTP/3 GET
def extract_http3_get_requests(pcap_content):
    ip_requests = []  # Liste pour stocker les IPs et les requêtes HTTP/3 GET
    
    try:
        with BytesIO(pcap_content) as f:
            for packet in PcapReader(f):  # Lecture en flux
                # Vérifier si c'est un paquet UDP et vérifier le port 443 (typique pour QUIC)
                if UDP in packet and (packet[UDP].dport == 443 or packet[UDP].sport == 443):
                    if Raw in packet:
                        raw_data = packet[Raw].load
                        # Recherche d'une requête HTTP/3 GET dans les données brutes
                        if b"GET" in raw_data and b"HTTP/3" in raw_data:
                            ip_requests.append(packet[IP].src)  # Ajouter l'IP source
    except Exception as e:
        print(f"Erreur lors du traitement du fichier PCAP : {e}")
    
    return ip_requests

# 4️⃣ Transformer l'RDD et compter les occurrences des IPs source
ip_requests_rdd = pcap_rdd.flatMap(lambda x: extract_http3_get_requests(x[1])) \
                          .map(lambda ip: (ip, 1)) \
                          .reduceByKey(lambda a, b: a + b)

# 5️⃣ Afficher l'IP source avec le plus grand nombre de requêtes HTTP/3 GET
most_frequent_ip = ip_requests_rdd.takeOrdered(1, key=lambda x: -x[1])

if most_frequent_ip:
    print(f"L'IP source réalisant l'attaque est : {most_frequent_ip[0][0]} avec {most_frequent_ip[0][1]} requêtes GET HTTP/3.")
else:
    print("Aucune requête HTTP/3 GET trouvée.")

end_time = time.time()
execution_time = end_time - start_time

with open("time.txt", "w") as f:
    f.write(f"Temps d'exécution: {execution_time} secondes.")

# 6️⃣ Arrêter Spark
spark.stop()
