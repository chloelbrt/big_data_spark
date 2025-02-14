from scapy.all import rdpcap, IP

def extract_ips_from_pcap(pcap_file):
    packets = rdpcap(pcap_file)  # Charger le fichier PCAP
    ip_addresses = set()  # Utilisation d'un set pour éviter les doublons

    for packet in packets:
        if IP in packet:
            ip_addresses.add(packet[IP].src)  # Adresse IP source
            ip_addresses.add(packet[IP].dst)  # Adresse IP destination

    return ip_addresses

# Exemple d'utilisation
pcap_file = "./tmp/data/normal_traffic_recording_16.pcap"
ip_list = extract_ips_from_pcap(pcap_file)

# print("Adresses IP trouvées :")
# for ip in ip_list:
#     print(ip)

with open("ip_list.txt", "w") as f:
    for ip in ip_list:
        f.write(ip + "\n")
