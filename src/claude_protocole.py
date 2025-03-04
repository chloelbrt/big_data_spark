import subprocess
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, collect_set, explode, col, count
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

def extraire_ips_et_protocoles_depuis_pcap(hdfs_path):
    
    # Exécution de la commande tcpdump via subprocess
    cmd = f"$HADOOP_HOME/bin/hdfs dfs -cat {hdfs_path} | tcpdump -r - -n -v"
    
    try:
        # Exécution de la commande
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        # Expressions régulières pour extraire les adresses IP et protocoles
        ip_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
        protocol_pattern = r'(TCP|UDP|ICMP|QUIC)'
        
        # Extraction des IPs
        ips = re.findall(ip_pattern, result.stdout)
        
        # Extraction des protocoles
        protocols = re.findall(protocol_pattern, result.stdout)
        
        # Combinaison des IPs et protocoles
        result_list = []
        for ip in set(ips):
            for proto in set(protocols):
                result_list.append((ip, proto))
        
        return result_list
    
    except Exception as e:
        print(f"Erreur lors du traitement : {e}")
        return []

def main():
    # Création de la session Spark
    spark = SparkSession.builder \
        .appName("ExtractionIPsProtocolesHDFS") \
        .getOrCreate()
    
    # Chemin des fichiers PCAP sur HDFS
    hdfs_path = "hdfs://localhost:9000/user/pcap_files/*.pcap"
    
    try:
        # Définition du schéma pour le DataFrame
        schema = StructType([
            StructField("ip", StringType(), True),
            StructField("protocole", StringType(), True)
        ])
        
        # UDF pour extraire les IPs et protocoles
        extraire_ips_protocoles_udf = udf(extraire_ips_et_protocoles_depuis_pcap, ArrayType(schema))
        
        # Création d'un DataFrame avec les chemins des fichiers
        fichiers_df = spark.createDataFrame(
            [(chemin,) for chemin in subprocess.check_output(
                f"$HADOOP_HOME/bin/hdfs dfs -ls {hdfs_path} | awk '{{print $8}}'", 
                shell=True, 
                text=True
            ).strip().split('\n')],
            ["chemin"]
        )
        
        # Extraction des IPs et protocoles
        ips_protocoles_df = fichiers_df.select(
            explode(extraire_ips_protocoles_udf("chemin")).alias("details")
        ).select(
            col("details.ip").alias("ip"),
            col("details.protocole").alias("protocole")
        )
        
        # Adresses IP uniques
        ips_uniques = ips_protocoles_df.select("ip").distinct()
        
        # Affichage des résultats d'IP uniques
        print("Adresses IP uniques trouvées:")
        ips_uniques.show(truncate=False)
        print(f"\nNombre total d'adresses IP uniques : {ips_uniques.count()}")
        
        # Comptage des requêtes par protocole
        requetes_par_protocole = ips_protocoles_df.groupBy("protocole").agg(
            count("ip").alias("nombre_requetes")
        ).orderBy("nombre_requetes", ascending=False)
        
        print("\nNombre de requêtes par protocole :")
        requetes_par_protocole.show(truncate=False)
    
    except Exception as e:
        print(f"Erreur : {e}")
    
    finally:
        # Fermeture de la session Spark
        spark.stop()

if __name__ == "__main__":
    main()