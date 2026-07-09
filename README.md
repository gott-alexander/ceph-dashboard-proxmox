# Ceph Dashboard für Proxmox VE

Ceph Dashboard für Proxmox VE bringt das **offizielle Ceph Dashboard** in die Proxmox-Welt. Anstatt eine alternative Verwaltungsoberfläche bereitzustellen, sorgt dieses Projekt dafür, dass die von Ceph entwickelte und unterstützte Dashboard-Lösung vollständig mit Proxmox VE zusammenarbeitet.

Das Projekt stellt die notwendige Integration zwischen Proxmox VE und den Ceph Management-Komponenten bereit und ermöglicht Administratoren den direkten Zugriff auf die Funktionen des nativen Ceph Dashboards innerhalb ihrer Proxmox-basierten Infrastruktur.

## Features

- Nutzung des offiziellen Ceph Dashboards
- Nahtlose Integration in Proxmox VE
- Direkter Zugriff auf Cluster-Management, Monitoring und Performance-Daten
- Anzeige von OSDs, MONs, MGRs, Pools, PGs, RBDs und CephFS
- Unterstützung aktueller Ceph- und Proxmox-Versionen
- Keine proprietären Erweiterungen oder Ersatz-Dashboards
- Open-Source und Community-getrieben

## Warum?

Proxmox VE bietet eine hervorragende Integration für Ceph Storage. Bestimmte Funktionen des offiziellen Ceph Dashboards sind jedoch nicht direkt innerhalb von Proxmox verfügbar. Dieses Projekt schließt diese Lücke und ermöglicht die Nutzung des vollständigen Ceph Dashboard Funktionsumfangs in einer Proxmox-Umgebung.

## FAQ

Es gibt eigendlich noch keine Fragen aber \.\.\.

- Funktionieren alle Funktionen? Nein. Jeder ist herzlich eingeladen, die fehlenden Funktionen hinzuzufügen.
- Wurden KI eingesetzt? Ja
- Ersetzt dieses Projekt die Ceph- oder Proxmox-Weboberfläche? Nein. Es ergänzt die bestehenden Komponenten und verbessert die Zusammenarbeit zwischen Ceph Dashboard und Proxmox VE.
- Welche Ceph-Versionen werden unterstützt? Grundsätzlich werden aktuelle Ceph-Releases unterstützt.

## Installation

### 1. Keyring-Verzeichnis erstellen

```bash
mkdir -p /etc/apt/keyrings
```

### 2. Repository-Schlüssel herunterladen

```bash
curl -fsSL https://gott-alexander.github.io/ceph-dashboard-proxmox/apt-repo-gott.gpg \
  -o /etc/apt/keyrings/apt-repo-gott.gpg
```

### 3. Repository hinzufügen

```bash
cat >/etc/apt/sources.list.d/cephprox.sources <<EOF
Types: deb
URIs: https://gott-alexander.github.io/ceph-dashboard-proxmox/
Suites: proxmox
Components: main
Signed-By: /etc/apt/keyrings/apt-repo-gott.gpg
Enabled: true
EOF
```

### 4. Paketlisten aktualisieren

```bash
apt update
```

### 5. Paket installieren

```bash
apt install cephprox
```

## Ceph-Konfiguration

Konfiguration des Proxmox-Zugangs für das Ceph Dashboard:

```
ceph config set mgr mgr/proxmox/pve_api_url 'https://<PROXMOX URL>/api2/json'
ceph config set mgr mgr/proxmox/pve_api_token_id 'root@pam!dashbord'
ceph config set mgr mgr/proxmox/pve_api_token_secret '<PROXMOX-API-SECRET>'
ceph config set mgr mgr/proxmox/pve_api_verify_ssl False
ceph config set mgr mgr/proxmox/pve_api_timeout 30
```

## Voraussetzungen

- Proxmox VE
- Ceph Cluster mit aktivem `mgr`-Dienst
- Proxmox API Token mit ausreichenden Berechtigungen

## Lizenz

Dieses Projekt steht unter einer eigenen Lizenz mit dem Namen "Advanced Open Contribution License"

Copyright (c) 2026 Alexander Gott
