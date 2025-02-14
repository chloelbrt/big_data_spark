from pyspark import SparkContext, SparkConf
from scapy.all import rdpcap
import os

# Configuration Spark
conf = SparkConf().setAppName("PCAP Processing") \
    # .set("spark.shuffle.io.maxRetries", "10") \
    # .set("spark.shuffle.io.retryWait", "30s") \
    # .set("spark.shuffle.file.buffer", "1m")  

sc = SparkContext(conf=conf)

# Fonction pour lire les fichiers PCAP et extraire les IPs
def extract_ips_from_pcap(file_path):
    packets = rdpcap(file_path)
    ips = []
    for packet in packets:
        if packet.haslayer("IP"):
            ips.append(packet["IP"].src)  # On extrait l'IP source
    return ips

# Charger les fichiers depuis HDFS
pcap_files = sc.wholeTextFiles("hdfs://localhost:9000/user/pcap_files/")  # Assurez-vous que le chemin est correct

# Fonction pour traiter les fichiers PCAP et extraire les IPs
def process_pcap_files(pcap_data):
    file_path, content = pcap_data
    ips = extract_ips_from_pcap(file_path)  # Extraire les IPs
    return ips

# Appliquer la transformation pour extraire les IPs de chaque fichier
ips_rdd = pcap_files.flatMap(process_pcap_files)  # flatMap permet de "aplatir" les listes d'IPs extraites

# Compter les occurrences de chaque IP
ip_counts = ips_rdd.map(lambda ip: (ip, 1)) \
                   .reduceByKey(lambda a, b: a + b)  # Agréger les IPs en comptant les occurrences
print('AAAAAAAAAAAAAAAAAAA', ip_counts)
# ip_counts.saveAsTextFile("hdfs://localhost:9000/user/hadoop/ip_counts_output/")

# Repartitionner les données pour éviter les grandes partitions
# ip_counts_repartitioned = ip_counts.repartition(50)  

# Sauvegarder le résultat dans HDFS
# ip_counts_repartitioned.saveAsTextFile("hdfs://localhost:9000/user/hadoop/ip_counts_output/")

# Fermeture du contexte Spark
sc.stop()
