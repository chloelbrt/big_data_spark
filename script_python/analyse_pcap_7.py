# SQL -- beaucoup trop long (x2)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from scapy.all import PcapReader, IP
from io import BytesIO
import time

# 1️⃣ Initialiser Spark avec des optimisations
spark = SparkSession.builder \
    .appName("Most_Frequent_Source_IP") \
    .config("spark.sql.shuffle.partitions", "200") \
    .master("local[2]") \
    .getOrCreate()

start_time = time.time()

# 2️⃣ Charger les fichiers PCAP depuis HDFS avec plus de partitions pour parallélisme
hdfs_folder_path = "hdfs://localhost:9000/user/pcap_files/"
pcap_rdd = spark.sparkContext.binaryFiles(hdfs_folder_path).repartition(50)

# 3️⃣ Fonction pour extraire les IP sources des paquets en mode streaming et transformer en DataFrame
def extract_source_ips(pcap_content):
    ips = []
    try:
        with BytesIO(pcap_content) as f:
            for packet in PcapReader(f):  # Lecture en flux
                if IP in packet:
                    ips.append(packet[IP].src)  # Ajouter l'IP source à la liste
    except Exception as e:
        print(f"Erreur lors du traitement d'un fichier PCAP : {e}")
    return ips

# 4️⃣ Extraire les IPs sources et créer un RDD avec les IPs
ip_rdd = pcap_rdd.flatMap(lambda x: extract_source_ips(x[1]))

# 5️⃣ Convertir l'RDD en DataFrame
ips_df = spark.createDataFrame(ip_rdd.map(lambda ip: (ip,)), ["ip"])

# 6️⃣ Créer une vue temporaire pour utiliser SQL
ips_df.createOrReplaceTempView("ips")

# 7️⃣ Utiliser Spark SQL pour compter les occurrences de chaque IP
most_frequent_ip = spark.sql("""
    SELECT ip, COUNT(*) as count
    FROM ips
    GROUP BY ip
    ORDER BY count DESC
    LIMIT 1
""")

# 8️⃣ Afficher le résultat
if most_frequent_ip.count() > 0:
    result = most_frequent_ip.collect()[0]
    print(f"L'IP source la plus fréquente est : {result['ip']} avec {result['count']} occurrences.")
else:
    print("Aucune IP source trouvée.")

end_time = time.time()
execution_time = end_time - start_time

# 9️⃣ Sauvegarder le temps d'exécution dans un fichier
with open("time.txt", "w") as f:
    f.write(f"{execution_time}")

# 🔟 Fermer Spark
spark.stop()
