# Compte les IP uniques dans les fichiers du répertoire donné

from pyspark.sql import SparkSession
from scapy.all import rdpcap, IP
from io import BytesIO

# 1. Initialiser la session Spark
spark = SparkSession.builder.appName("PCAP_IP_Counter").getOrCreate()
sc = spark.sparkContext

# 2. Lire tous les fichiers PCAP du dossier sur HDFS sous forme de RDD binaire
hdfs_folder_path = "hdfs://localhost:9000/user/pcap_files/"
pcap_rdd = sc.binaryFiles(hdfs_folder_path)  # Charge tous les fichiers PCAP

# 3. Fonction pour extraire les IPs à partir du contenu PCAP
def extract_ips(pcap_content):
    try:
        packets = rdpcap(BytesIO(pcap_content))  # Charger les paquets avec Scapy
        ip_set = set()
        
        for packet in packets:
            if IP in packet:
                ip_set.add(packet["IP"].src)
                ip_set.add(packet["IP"].dst)

        return list(ip_set)  # Retourner une liste d'IPs uniques trouvées
    except Exception as e:
        print(f"Erreur lors du traitement d'un fichier PCAP : {e}")
        return []  # Retourner une liste vide en cas d'erreur

# 4. Appliquer la fonction à chaque fichier PCAP dans le RDD
ip_rdd = pcap_rdd.flatMap(lambda x: extract_ips(x[1]))  # x[1] = contenu du fichier PCAP

# 5. Supprimer les doublons et compter les adresses IP uniques
unique_ips_count = ip_rdd.distinct().count()

# 6. Afficher le résultat
print(f"Nombre total d'adresses IP uniques dans tous les fichiers PCAP : {unique_ips_count}")

# Fermer Spark
spark.stop()
