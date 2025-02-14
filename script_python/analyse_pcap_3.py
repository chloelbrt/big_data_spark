# Compte les IP uniques d'un seul fichier

from pyspark.sql import SparkSession
from scapy.all import rdpcap, IP
from io import BytesIO

# 1. Initialiser la session Spark
spark = SparkSession.builder.appName("PCAP_IP_Counter").getOrCreate()
sc = spark.sparkContext

# 2. Lire le fichier PCAP depuis HDFS sous forme de RDD binaire
hdfs_path = "hdfs://localhost:9000/user/pcap_files/normal_traffic_recording_17.pcap"
pcap_rdd = sc.binaryFiles(hdfs_path)  # Récupère le fichier en tant que RDD [(filename, content)]

# 3. Fonction pour extraire les IPs à partir du contenu PCAP
def extract_ips(pcap_content):
    packets = rdpcap(BytesIO(pcap_content))  # Charger les paquets avec Scapy
    ip_set = set()
    
    for packet in packets:
        if IP in packet:
            ip_set.add(packet["IP"].src)
            ip_set.add(packet["IP"].dst)

    return list(ip_set)  # Retourner une liste d'IPs uniques trouvées

# 4. Appliquer la fonction à chaque partition de l'RDD
ip_rdd = pcap_rdd.flatMap(lambda x: extract_ips(x[1]))  # x[1] = contenu du fichier PCAP

# 5. Compter les IPs uniques avec Spark
unique_ips_count = ip_rdd.distinct().count()

# 6. Afficher le résultat
print(f"Nombre d'adresses IP uniques : {unique_ips_count}")

# Fermer Spark
spark.stop()

