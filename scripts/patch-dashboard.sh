#!/bin/bash
# Copyright (c) 2026 Alexander Gott
# Patcht das Ceph Dashboard Frontend, um den cephadm API-Call auf proxmox umzuleiten.
# Wird durch den APT-Hook bei jeder Installation/Änderung von ceph-mgr-dashboard aufgerufen.

TARGET_DIR="/usr/share/ceph/mgr/dashboard/frontend/dist"

if [ -d "$TARGET_DIR" ]; then
    # Findet alle main.*.js Dateien in allen Sprachordnern (cs, de, en, etc.)
    find "$TARGET_DIR" -type f -name "main.*.js" -exec sed -i "s/mgrModuleService\.getConfig([\"']cephadm[\"'])/mgrModuleService.getConfig(\"proxmox\")/g" {} +
fi

if [ -d "$TARGET_DIR" ]; then
    find "$TARGET_DIR" -type f -name "main.*.js" -exec sed -i 's/"tc_submenuitem","tc_submenuitem_block_nvme"/"tc_submenuitem","d-none","tc_submenuitem_block_nvme"/g' {} +
fi

if [ -d "$TARGET_DIR" ]; then
    find "$TARGET_DIR" -type f -name "main.*.js" -exec sed -i 's/"tc_submenuitem",tc_submenuitem_multiCluster_overview"/"tc_submenuitem","d-none","tc_submenuitem_multiCluster_overview"/g' {} +
fi

if [ -d "$TARGET_DIR" ]; then
    find "$TARGET_DIR" -type f -name "main.*.js" -exec sed -i 's/"tc_submenuitem","tc_submenuitem_file_smb"/"tc_submenuitem","d-none","tc_submenuitem_file_smb"/g' {} +
fi

# Orchestrator anpassen
FILE="/usr/share/ceph/mgr/orchestrator/module.py"
if ! grep -q "'proxmox'" "$FILE"; then
  sed -i.bak -E "s/enum_allowed\s*=\s*\['cephadm',\s*'rook',\s*'test_orchestrator'\],/enum_allowed=['cephadm', 'rook', 'test_orchestrator', 'proxmox'],/" "$FILE"
fi

systemctl daemon-reload
systemctl reset-failed ceph-mgr*
systemctl restart ceph-mgr.target
ceph crash archive-all
