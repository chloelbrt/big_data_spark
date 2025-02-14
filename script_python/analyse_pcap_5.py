# Compte les IP uniques dans les fichiers du répertoire donné de manière optimisée

from pyspark.sql import SparkSession
from scapy.all import PcapReader, IP
from io import BytesIO

# Initialiser la session Spark avec des optimisations
spark = SparkSession.builder \
    .appName("Optimized_PCAP_IP_Counter") \
    .config("spark.sql.shuffle.partitions", "400") \
    .getOrCreate()
sc = spark.sparkContext

# Lire tous les fichiers PCAP en RDD binaire et les répartir sur plusieurs partitions
hdfs_folder_path = "hdfs://localhost:9000/user/pcap_files/"
pcap_rdd = sc.binaryFiles(hdfs_folder_path).repartition(800)  # Augmente le parallélisme

# Fonction pour extraire les IPs en mode **streaming**
def extract_ips_stream(pcap_content):
    try:
        ip_set = set()
        with BytesIO(pcap_content) as f:
            for packet in PcapReader(f):  # Lecture en flux au lieu de tout charger en RAM
                if IP in packet:
                    ip_set.add(packet[IP].src)
                    ip_set.add(packet[IP].dst)
        return ip_set
    except Exception as e:
        print(f"Erreur lors du traitement d'un fichier PCAP : {e}")
        return set()

# Appliquer la fonction en parallèle, puis récupérer les IPs sous forme de tuples (IP, 1)
ip_rdd = pcap_rdd.flatMap(lambda x: extract_ips_stream(x[1])).map(lambda ip: (ip, 1))

# Suppression des doublons et comptage des IPs uniques en parallèle
unique_ips_count = ip_rdd.reduceByKey(lambda a, b: a).count()

# Afficher le résultat
print(f"Nombre total d'adresses IP uniques : {unique_ips_count}")

# Fermer Spark
spark.stop()
