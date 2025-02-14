from pyspark.sql import SparkSession
from pyspark import SparkContext
from scapy.all import rdpcap

# Initialisation de Spark
spark = SparkSession.builder.appName("PCAP IP Count").getOrCreate()

# Charger les fichiers PCAP depuis HDFS
hdfs_path = "hdfs://localhost:9000/user/hadoop/pcap_files/"
pcap_files = spark.sparkContext.textFile(hdfs_path)

# Fonction pour extraire les adresses IP depuis un fichier PCAP
def extract_ips(pcap_file):
    try:
        packets = rdpcap(pcap_file)
        ip_addresses = set()
        for pkt in packets:
            if pkt.haslayer("IP"):
                ip_addresses.add(pkt["IP"].src)
                ip_addresses.add(pkt["IP"].dst)
        return ip_addresses
    except Exception as e:
        print(f"Erreur lors de l'extraction des IPs depuis {pcap_file}: {e}")
        return set()

# Appliquer la fonction pour extraire les IPs
rdd = spark.sparkContext.parallelize(pcap_files.collect()).flatMap(extract_ips)

# Compter les adresses IP uniques
unique_ips = rdd.distinct().count()

print(f"Nombre d'IP uniques trouvées : {unique_ips}")

# Arrêter Spark
spark.stop()
