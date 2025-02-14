from pyspark.sql import SparkSession
from scapy.all import PcapReader, IP
from io import BytesIO

# 1️⃣ Initialiser Spark avec des optimisations
spark = SparkSession.builder \
    .appName("Most_Frequent_Source_IP") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "16g") \
    .getOrCreate()
sc = spark.sparkContext

# 2️⃣ Charger les fichiers PCAP depuis HDFS avec plus de partitions pour parallélisme
hdfs_folder_path = "hdfs://localhost:9000/user/pcap_files/"
pcap_rdd = sc.binaryFiles(hdfs_folder_path).repartition(100)

# 3️⃣ Fonction pour extraire les IP sources des paquets en mode streaming
def extract_source_ips(pcap_content):
    try:
        with BytesIO(pcap_content) as f:
            for packet in PcapReader(f):  # Lecture en flux
                if IP in packet:
                    yield packet[IP].src  # Retourner l'IP source directement
    except Exception as e:
        print(f"Erreur lors du traitement d'un fichier PCAP : {e}")

# 4️⃣ Transformer l'RDD et compter les occurrences de chaque IP source
ip_counts_rdd = pcap_rdd.flatMap(lambda x: extract_source_ips(x[1])) \
                        .map(lambda ip: (ip, 1)) \
                        .reduceByKey(lambda a, b: a + b)

# 5️⃣ Trouver l'IP source la plus fréquente
most_frequent_ip = ip_counts_rdd.takeOrdered(1, key=lambda x: -x[1])

# 6️⃣ Afficher le résultat
if most_frequent_ip:
    print(f"L'IP source la plus fréquente est : {most_frequent_ip[0][0]} avec {most_frequent_ip[0][1]} occurrences.")
else:
    print("Aucune IP source trouvée.")

# 7️⃣ Fermer Spark
spark.stop()
