# Copyright (c) 2026 Alexander Gott
import json
import logging
import ssl
import urllib.parse
import urllib.request
from typing import Tuple, Dict, Any, List, Optional
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec
from mgr_module import MgrModule
import orchestrator
from ceph.deployment import inventory
from .cli import ProxmoxCLICommand

class ProxmoxOrchestrator(MgrModule, orchestrator.Orchestrator):
    CLICommand = ProxmoxCLICommand
    
    """
    Proxmox VE Orchestrator module for Ceph.
    Integrates PVE host and disk management into the Ceph Dashboard.
    """

    MODULE_OPTIONS = [
        {
            'name': 'pve_api_url',
            'type': 'str',
            'default': '',
            'desc': 'Basis-URL der Proxmox API, z.B. https://pve01.example.local:8006/api2/json'
        },
        {
            'name': 'pve_api_token_id',
            'type': 'str',
            'default': '',
            'desc': 'Proxmox API Token ID, z.B. cephro@pve!inventory'
        },
        {
            'name': 'pve_api_token_secret',
            'type': 'str',
            'default': '',
            'desc': 'Proxmox API Token Secret'
        },
        {
            'name': 'pve_api_verify_ssl',
            'type': 'bool',
            'default': True,
            'desc': 'SSL-Zertifikat der Proxmox API prüfen'
        },
        {
            'name': 'pve_api_timeout',
            'type': 'int',
            'default': 10,
            'desc': 'HTTP Timeout in Sekunden'
        },
    ]

    @staticmethod
    def can_run() -> Tuple[bool, str]:
        # Wir geben immer True zurück, da die pvesh-Prüfung erst zur Laufzeit stattfindet
        return True, ''

    def available(self) -> Tuple[bool, str, Dict[str, Any]]:
        api_url = self.get_module_option('pve_api_url', '')
        token_id = self.get_module_option('pve_api_token_id', '')
        token_secret = self.get_module_option('pve_api_token_secret', '')

        if not api_url:
            return False, "Proxmox API URL nicht konfiguriert (URL fehlt)", {}
        elif not token_id:
            return False, "Proxmox token_id nicht konfiguriert (token_id fehlt)", {}
        elif not token_secret:
            return False, "Proxmox token_secret nicht konfiguriert (token_secret fehlt)", {}

        return True, "", {}

    def __init__(self, *args, **kwargs):
        super(ProxmoxOrchestrator, self).__init__(*args, **kwargs)

    def serve(self) -> None:
        self.log.info("ProxmoxOrchestrator module started successfully")

    def _get_api_config(self) -> Tuple[str, str, str, bool, int]:
        api_url = self.get_module_option('pve_api_url', '').rstrip('/')
        token_id = self.get_module_option('pve_api_token_id', '')
        token_secret = self.get_module_option('pve_api_token_secret', '')
        verify_ssl = self.get_module_option('pve_api_verify_ssl', True)
        timeout = int(self.get_module_option('pve_api_timeout', 10))

        if not api_url:
            raise orchestrator.OrchestratorError("Missing module option: pve_api_url")
        if not token_id:
            raise orchestrator.OrchestratorError("Missing module option: pve_api_token_id")
        if not token_secret:
            raise orchestrator.OrchestratorError("Missing module option: pve_api_token_secret")

        return api_url, token_id, token_secret, verify_ssl, timeout

    def _api_get(self, path: str) -> Any:
        """
        Führt einen GET gegen die Proxmox REST API aus und gibt payload['data'] zurück.
        path: z.B. '/nodes' oder '/nodes/pve01/disks/list'
        """
        api_url, token_id, token_secret, verify_ssl, timeout = self._get_api_config()

        # path normalisieren
        if not path.startswith('/'):
            path = '/' + path

        url = f"{api_url}{path}"

        headers = {
            'Authorization': f'PVEAPIToken={token_id}={token_secret}',
            'Accept': 'application/json',
        }

        req = urllib.request.Request(url, headers=headers, method='GET')

        if verify_ssl:
            context = ssl.create_default_context()
        else:
            context = ssl._create_unverified_context()

        try:
            with urllib.request.urlopen(req, context=context, timeout=timeout) as resp:
                raw = resp.read().decode('utf-8')
                payload = json.loads(raw)

            # /api2/json liefert i.d.R. {"data": ...}
            if isinstance(payload, dict) and 'data' in payload:
                return payload['data']

            return payload

        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode('utf-8', errors='replace')
            except Exception:
                body = ''
            raise orchestrator.OrchestratorError(f"Proxmox API HTTPError for {path}: {e.code} {e.reason} {body}")
            return None

        except urllib.error.URLError as e:
            raise orchestrator.OrchestratorError(f"Proxmox API URLError for {path}: {e}")
            return None

        except json.JSONDecodeError as e:
            raise orchestrator.OrchestratorError(f"Failed to parse Proxmox API JSON for {path}: {e}")
            return None

        except Exception as e:
            orchestrator.OrchestratorError(f"Unexpected Proxmox API error for {path}: {e}")
            return None

    def _make_device_id(self, disk: dict) -> Optional[str]:
        vendor = disk.get('vendor', '').strip()
        model = disk.get('model', '').strip()
        serial = disk.get('serial', '').strip()

        dev_id = f"{vendor}_{model}_{serial}".replace(' ', '_')
        
        return dev_id

    def _disk_rotational(self, disk: dict) -> str:
        disk_type = str(disk.get('type', '')).lower()
        rpm = disk.get('rpm', -1)

        if disk_type in ('ssd', 'nvme'):
            return '0'
        if disk_type == 'hdd':
            return '1'

        # Fallback über rpm
        if isinstance(rpm, int):
            if rpm > 0:
                return '1'
            if rpm == 0:
                return '0'

        # konservativer Fallback
        return '1'
        
    def _human_bytes(self, num_bytes: int) -> str:
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        size = float(num_bytes)
        for unit in units:
            if size < 1024.0 or unit == units[-1]:
                if unit == 'B':
                    return f'{int(size)}{unit}'
                return f'{size:.1f}{unit}'
            size /= 1024.0
            
    def _read_first_line(self, path: str) -> str:
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.readline().strip()
        except Exception:
            return ''
            
    def _get_ceph_version(self, host: str = None) -> str:
        """
        Liest die aktuelle Ceph-Version sicher aus der internen service_map aus.
        Wenn 'host' übergeben wird, wird nur die Version dieses spezifischen Hosts zurückgegeben.
        """
        current_version_str = 'unknown'
        try:
            svc_map = self.get('service_map')
            if svc_map and 'services' in svc_map:
                services = svc_map['services']
                
                # MGR oder MON gibt es IMMER, RGW nur manchmal.
                for svc_type in ['mgr', 'mon', 'osd', 'rgw']:
                    if svc_type not in services:
                        continue
                        
                    daemons = services[svc_type].get('daemons', {})
                    
                    if isinstance(daemons, dict) and daemons:
                        # Iteriere durch ALLE Werte (überspringt das 'summary' automatisch)
                        for daemon_info in daemons.values():
                            if isinstance(daemon_info, dict):
                                metadata = daemon_info.get('metadata', {})
                                ver_short = metadata.get('ceph_version_short')
                                hostname = metadata.get('hostname')
                                
                                # Host-Filter anwenden, falls ein Host übergeben wurde
                                if host and hostname and host != hostname:
                                    continue
                                
                                if ver_short:
                                    current_version_str = ver_short
                                    break # Innerer Loop beenden
                                    
                    if current_version_str != 'unknown':
                        break # Äußerer Loop beenden
                        
        except Exception as e:
            # Fehler sauber im Ceph Manager Log anzeigen
            raise orchestrator.OrchestratorError(f"Could not determine ceph version from service_map: {e}")

        return current_version_str
        
    def get_proxmox_version(self, host: str) -> str:
        """
        Fragt die Proxmox VE Version eines spezifischen Hosts ab.
        Gibt die Version als String zurück (z.B. '8.1.3') oder einen leeren String bei Fehler.
        """
        try:
            node_enc = urllib.parse.quote(host, safe='')
            status = self._api_get(f'/nodes/{node_enc}/status')
            if isinstance(status, dict):
                pve_ver_raw = status.get('pveversion', '')
                # Format ist z.B. "pve-manager/8.1.3/9032ab30f4c5f66d"
                if '/' in pve_ver_raw:
                    return pve_ver_raw.split('/')[1]
                return pve_ver_raw
        except Exception as e:
            raise orchestrator.OrchestratorError(f"Failed to get Proxmox version for host {host}: {e}")
        
        return None
        
    # --- HAUPTFUNKTIONEN ---
    @orchestrator.handle_orch_error
    def get_hosts(self) -> List[orchestrator.HostSpec]:
        """Liest die Proxmox-Nodes aus"""
        nodes_data = self._api_get('/nodes')
        if not nodes_data:
            return []

        hosts = []
        for node in nodes_data:
            hosts.append(orchestrator.HostSpec(
                hostname=node.get('node', ''),
                addr=node.get('node', ''),
                status=node.get('status', ''),
                labels=[]
            ))
        return hosts

    @orchestrator.handle_orch_error
    def get_inventory(self, host_filter: Optional[orchestrator.InventoryFilter] = None, refresh: bool = False) -> List[orchestrator.InventoryHost]:
        """Liest die physischen Laufwerke der Proxmox-Nodes aus"""
        nodes_data = self._api_get('/nodes')
        if not nodes_data:
            return []

        inv_list = []
        for node in nodes_data:
            node_name = node.get('node', '')
            
            # Filter anwenden, falls das Dashboard nur einen bestimmten Host abfragt
            if host_filter and host_filter.hosts and node_name not in host_filter.hosts:
                continue

            # WICHTIG: /disks/list liefert die eigentlichen Platten, /disks nur die API-Endpunkte!
            node_enc = urllib.parse.quote(node_name, safe='')
            disks_data = self._api_get(f'/nodes/{node_enc}/disks/list')
            if disks_data is None:
                continue

            devs = []
            for disk in disks_data:
                # Proxmox meldet 'used' z.B. als "LVM", "BIOS boot" oder "" wenn frei
                used_by = disk.get('used', '')
                is_available = not bool(used_by)
                rejected_reasons = [] if is_available else [f"Used by: {used_by}"]

                # Ceph erwartet die technischen Details verschachtelt im Dictionary 'sys_api'
                # Die Größe (size) wird von Proxmox in Bytes als Integer geliefert
                sys_api_data = {
                    'size': int(disk.get('size', 0)),
                    'rotational': self._disk_rotational(disk),
                    'health': disk.get('health', 'UNKNOWN'),
                    'model': disk.get('model', '').strip(),
                    'vendor': disk.get('vendor', '').strip(),
                    'node': node_name,
                    'pv_name': disk.get('by_id_link', ''),
                    'serial': disk.get('serial', '').strip(),
                }

                # Device-Objekt korrekt instanziieren
                dev = inventory.Device(
                    path=disk.get('devpath', ''),
                    sys_api=sys_api_data,
                    device_id=self._make_device_id(disk),
                    available=is_available,
                    lvs=[],
                    rejected_reasons=rejected_reasons
                )
                
                devs.append(dev)
            
            # InventoryHost positionsbasiert erstellen: (hostname, devices)
            inv_list.append(orchestrator.InventoryHost(
                node_name, 
                inventory.Devices(devs)
            ))

        return inv_list

    def get_feature_set(self):
        features = {
            'get_hosts': True,
            'get_inventory': True,
            'get_facts': True,
            'describe_service': True,
            'list_daemons': True,
            'upgrade_status': True,
            'upgrade_ls': True,
            'get_security_config': True,

            # bewusst nicht unterstützt
            'blink_device_light': False,
            'zap_device': False,
            'create_osds': False,
            'remove_osds': False,
            'add_host': False,
            'remove_host': False,
            'update_host_addr': False,
            'add_host_label': False,
            'remove_host_label': False,
            'host_ok_to_stop': False,
            'enter_host_maintenance': False,
            'exit_host_maintenance': False,
            'rescan_host': False,
            'remove_service': False,
            'remove_daemons': False,
            'daemon_action': False,
            'apply': False,
            'set_unmanaged': False,
            'upgrade_start': False,
            'upgrade_pause': False,
            'upgrade_resume': False,
            'upgrade_stop': False,
            'apply_mon': False,
            'apply_mgr': False,
            'apply_mds': False,
            'apply_rgw': False,
            'apply_nfs': False,
            'apply_iscsi': False,
            'apply_rbd_mirror': False,
            'apply_cephfs_mirror': False,
            'apply_prometheus': False,
            'apply_grafana': False,
            'apply_alertmanager': False,
            'apply_node_exporter': False,
            'apply_loki': False,
            'apply_promtail': False,
            'apply_crash': False,
            'apply_container': False,
            'apply_snmp_gateway': False,
            'os_exit_noop': False,
            'host_exists': False,
            'shutdown': False,
            'get_fqdn': False,
            '_get_mgr_ips': False,
            '_get_security_config': False,
            'get_mgmt_gw_external_endpoint': False,
            '_get_cephadm_binary_path': False,
            '_trigger_osd_removal': False,
            'pause': False,
            'validate_ssh_config_fname': False,
            '_process_ls_output': False,
            '_generate_key': False,
            '_get_pub_key': False,
            'set_ssh_user': False,
            '_get_extra_ceph_conf': False,
            '_update_config_check': False,
            'extra_ceph_conf': False,
            '_osd_activate': False,
            '_get_cephadm_version_for_host_prep': False,
            '_prepare_new_host_for_sudo_hardening': False,
            'hardware_powercycle': False,
            '_host_ok_to_stop': False,
            'update_maintenance_healthcheck': False,
            'perform_service_action': False,
            'daemon_is_self': False,
            'get_active_mgr': False,
            'get_active_mgr_digests': False,
            '_schedule_daemon_action': False,
            '_check_pool_exists': False,
            '_get_alertmanager_credentials': False,
            'generate_certificates': False,
            'set_prometheus_access_info': False,
            'remove_prometheus_remote_write': False,
            'cert_store_bindings_ls': False,
            'cert_store_cert_check': False,
            'rm_tuned_profile': False,
            'tuned_profile_ls': False,
            'tuned_profile_rm_setting': False,
            'set_health_error': False,
            'remove_health_warning': False,
            '_get_dashboard_url': False,
            'stop_remove_osds': False,
            'set_osd_spec': False,
            'drain_host': False,
            'tuned_profile_add_settings': False,
            'run_cmd': False,
            'node_proxy_fullreport': False,
            'hardware_shutdown': False,
            '_daemon_action_set_image': False,
            'check_host': False,
            '_prepare_host': False,
            '_set_extra_ceph_conf': False,
            '_set_pub_key': False,
            '_as_datetime': False,
            'registry_login': False,
            'wait_async': False,
            'cert_store_rm_cert': False,
            'cert_store_rm_key': False,
            '_prepare_host_for_sudo_hardening': False,
            '_prepare_hosts_for_sudo_hardening': False,
            '_prepare_host_and_enable_sudo_hardening': False,
            'cert_store_get_key': False,
            '_systemd_unit_ls': False,
            '_client_keyring_set': False,
            'get_container_image': False,
            '_setup_user_on_host': False,
            'wrapper': False,
            '_init_cert_mgr': False,
            '_get_mgmt_gw_endpoint': False,
            'get_mgmt_gw_internal_endpoint': False,
            '_kick_serve_loop': False,
            'resume': False,
            'validate_ssh_config_content': False,
            'update_watched_hosts': False,
            'offline_hosts_remove': False,
            'update_failed_daemon_health_check': False,
            '_setup_user_on_all_hosts': False,
            'setup_user_on_host': False,
            '_validate_and_set_ssh_val': False,
            '_set_ssh_config': False,
            '_clear_ssh_config': False,
            '_get_ssh_config': False,
            '_set_priv_key': False,
            '_set_signed_cert': False,
            '_clear_key': False,
            '_config_check_status': False,
            '_config_check_enable': False,
            '_config_check_disable': False,
            'run': False,
            '_check_valid_addr': False,
            '_add_host': False,
            'node_proxy_summary': False,
            'node_proxy_firmware': False,
            'node_proxy_firmwares': False,
            'node_proxy_criticals': False,
            'node_proxy_common': False,
            'get_minimal_ceph_conf': False,
            '_combine_confs': False,
            '_invalidate_daemons_and_kick_serve': False,
            '_invalidate_all_host_metadata_and_kick_serve': False,
            'service_action': False,
            'blink': False,
            'get_osd_uuid_map': False,
            'get_osd_by_id': False,
            '_remove_daemons': False,
            'create_func_map': False,
            '_get_prometheus_credentials': False,
            'set_custom_prometheus_alerts': False,
            'set_prometheus_target': False,
            'remove_prometheus_target': False,
            'set_prometheus_remote_write': False,
            'set_alertmanager_access_info': False,
            'get_prometheus_access_info': False,
            'get_alertmanager_access_info': False,
            'cert_store_reload': False,
            'cert_store_key_ls': False,
            'get_nvmeof_tls_bundle': False,
            '_raise_non_editable_cert_error': False,
            'tuned_profile_add_setting': False,
            'tuned_profile_rm_settings': False,
            'set_health_warning': False,
            'upgrade_check': False,
            'update_service': False,
            'remove_osds_status': False,
            'stop_drain_host': False,
            'trigger_connect_dashboard_rgw': False,
            'systemd_unit_ls': False,
            '_client_keyring_rm': False,
            'cert_store_cert_ls': False,
            'cert_store_get_cert': False,
            'replace_device': False,
            'inner': False,
            '_preview_osdspecs': False,
            'get_first_matching_network_ip': False,
            'get_unique_name': False,
            '_is_adding_nvmeof_group_to_existing_service': False,
            '_plan': False,
            'plan': False,
            '_check_cert_source': False,
            '_trigger_preview_refresh': False,
            'apply_drivegroups': False,
            'device_selection_from_explicit_paths': False,
            'create_osd_default_spec': False,
            'is_path_lists_only': False,
            'validate_device': False,
            '_rotate_daemon_key': False,
            '_mon_public_network_changed': False,
            '_apply': False,
            '_get_candidate_hosts': False,
            '_validate_tunedprofile_settings': False,
            '_validate_tuned_profile_spec': False,
            '_config_checks_list': False,
            'extra_ceph_conf_is_newer': False,
            '_client_keyring_ls': False,
            '_daemon_action': False,
            'path_list_only': False,
            'validate_no_empty_device_paths': False,
            '_add_daemon': False,
            '_create_daemons': False,
            'add_daemon': False,
            '_validate_one_shot_placement_spec': False,
            'apply_tuned_profiles': False,
            '_apply_service_spec': False,
            'apply_ingress': False,
            'apply_iscsi': False,
            'apply_alloy': False,
            'apply_ceph_exporter': False,
            'apply_smb': False,
            'apply_mgmt_gateway': False,
            'apply_oauth2_proxy': False,
        }

        return {
            name: {
                'available': available,
                'message': '' if available else 'Proxmox: not supported'
            }
            for name, available in features.items()
        }
        
    @orchestrator.handle_orch_error
    def get_facts(self, hostname: str):
        """
        Liefert Host-Facts für das Ceph-Dashboard.

        Wichtig:
        - Rückgabe ist eine LISTE
        - das Dict MUSS 'hostname' enthalten
        - NIC/RAW Capacity werden separat aggregiert
        """
        
        # ==========================================
        # NEU: Fallunterscheidung für alle Nodes vs. einzelner Node
        # ==========================================
        if hostname is None:
            self.log.debug('Fetching facts for all hosts')
            all_facts = []
            try:
                nodes = self._api_get('/nodes') or []
                for node in nodes:
                    node_name = node.get('node')
                    if not node_name:
                        continue
                    
                    # Wir rufen uns selbst rekursiv für jeden Node auf
                    try:
                        node_facts = self.get_facts(node_name)
                        if node_facts:
                            all_facts.extend(node_facts)
                    except Exception as e:
                        # Wenn ein Node nicht erreichbar ist, loggen wir den Fehler,
                        # aber das Dashboard der restlichen Nodes lädt trotzdem weiter!
                        self.log.warning(f"Could not get facts for node {node_name}: {e}")
                        all_facts.append({'hostname': node_name})
                        
            except Exception as e:
                raise orchestrator.OrchestratorError(f'Failed to get node list from Proxmox: {e}')
                
            return all_facts

        node_status = self._api_get(f'/nodes/{hostname}/status')
        if not node_status:
            self.log.warning(f'No /status data for node {hostname}')
            return [{
                'hostname': hostname
            }]

        cpuinfo = node_status.get('cpuinfo', {}) or {}
        memory = node_status.get('memory', {}) or {}
        kernel = node_status.get('current-kernel', {}) or {}

        # CPU
        # Bei deinem Beispiel liefert Proxmox:
        # cpuinfo.cores   = 8
        # cpuinfo.sockets = 1
        # cpuinfo.cpus    = 8
        #
        # Für Ceph-Dashboard ist das nützlichste Mapping:
        # cpu_cores   = physische/logische Gesamtkerne des Hosts
        # cpu_count   = Socket-Anzahl
        # cpu_threads = Gesamtthreads (wenn unbekannt = cpus)
        socket_count = int(cpuinfo.get('sockets', 1) or 1)

        # In deinem Beispiel ist cpus=8 bereits die Gesamtanzahl
        total_cores = int(cpuinfo.get('cpus', 0) or 0)
        if total_cores <= 0:
            cores_per_socket = int(cpuinfo.get('cores', 0) or 0)
            total_cores = cores_per_socket * socket_count if cores_per_socket else 0

        total_threads = int(cpuinfo.get('cpus', 0) or 0)
        if total_threads <= 0:
            total_threads = total_cores

        # RAM
        memory_total_bytes = int(memory.get('total', 0) or 0)
        memory_total_kb = memory_total_bytes // 1024

        # RAW CAPACITY Disk-Typen aus disks/list
        raw_capacity_bytes = 0
        hdd_capacity_bytes = 0
        flash_capacity_bytes = 0

        # CAPACITY Disk-Typen aus disks/list
        hdd_count = 0
        flash_count = 0
        nvme_count = 0

        try:
            disks = self._api_get(f'/nodes/{hostname}/disks/list') or []
            for d in disks:
                size = int(d.get('size', 0) or 0)
                raw_capacity_bytes += size

                dtype = str(d.get('type', '')).lower()
                if dtype == 'hdd':
                    hdd_count += 1
                    hdd_capacity_bytes += size
                elif dtype == 'ssd':
                    flash_count += 1
                    flash_capacity_bytes += size
                elif dtype == 'nvme':
                    nvme_count += 1
                    flash_count += 1
                    flash_capacity_bytes += size
                else:
                    hdd_count += 1
                    hdd_capacity_bytes += size
        except Exception:
            raise orchestrator.OrchestratorError(f'Unable to aggregate disks for {hostname}')

        # NICs aus /nodes/<node>/network
        nic_count = 0
        nic_speed_mbps = 0
        try:
            # Proxmox hat einen separaten Node-API-Zweig /nodes/{node}/network
            netifs = self._api_get(f'/nodes/{hostname}/network') or []

            for iface in netifs:
                iface_type = str(iface.get('type', '')).lower()
                active = iface.get('active', 0)

                # konservativ: echte uplink-/hostrelevante Interfaces zählen
                if iface_type in ('eth'):
                    nic_count += 1

                # falls speed vorhanden ist -> größtes aktives Link-Speed merken
                # (nicht jede Proxmox-Version liefert das Feld)
                speed = iface.get('speed')
                try:
                    speed_val = int(speed or 0)
                    if active and speed_val > nic_speed_mbps:
                        nic_speed_mbps = speed_val
                except Exception:
                    pass
        except Exception:
            self.log.exception(f'Unable to aggregate network for {hostname}')

        sys_vendor = self._read_first_line('/sys/devices/virtual/dmi/id/sys_vendor')
        product_name = self._read_first_line('/sys/devices/virtual/dmi/id/product_name')

        facts = {
            # Pflichtfeld für Dashboard-Merge:
            'hostname': hostname,

            # CPU
            'cpu_cores': total_cores,
            'cpu_count': socket_count,
            'cpu_threads': total_threads,
            'cpu_model': cpuinfo.get('model', '') or '',

            # RAM
            'memory_total_kb': memory_total_kb,

            # Architektur / Kernel
            'arch': kernel.get('machine', '') or cpuinfo.get('arch', '') or '',
            'kernel': node_status.get('kversion', '') or '',
            'vendor': sys_vendor,
            'model': product_name+"?",

            # cephadm-artige Kapazitätsfelder
            'hdd_capacity_bytes': hdd_capacity_bytes,
            'hdd_capacity': self._human_bytes(hdd_capacity_bytes),
            'flash_capacity_bytes': flash_capacity_bytes,
            'flash_capacity': self._human_bytes(flash_capacity_bytes),

            # zusätzliche Gesamtwerte
            'raw_capacity_bytes': raw_capacity_bytes,
            'raw_capacity': self._human_bytes(raw_capacity_bytes),

            # Disk-Zähler
            'hdd_count': hdd_count,
            'flash_count': flash_count,
            'nvme_count': nvme_count,

            # NIC
            'nic_count': nic_count,
            'nic_speed_mbps': nic_speed_mbps,
        }

        self.log.debug(f'Host facts for {hostname}: {facts!r}')
        return [facts]
        
    @orchestrator.handle_orch_error
    def list_daemons(self, service_name=None, daemon_type=None, daemon_id=None, host=None, refresh=False):
        """
        Liest alle Daemons aus den verschiedenen Ceph-Maps aus.
        """
        daemon_list = []

        # --- FILTER VORBEREITUNG ---
        filter_types = []
        if daemon_type:
            if isinstance(daemon_type, list):
                filter_types = [str(t).strip().lower() for t in daemon_type]
            elif isinstance(daemon_type, str):
                filter_types = [t.strip().lower() for t in daemon_type.split(',')]

        def is_filtered(dtype):
            return filter_types and dtype.lower() not in filter_types

        # --- 1. VERSION MAP PRO HOST BAUEN ---
        # Extrahiert die Ceph-Version pro Host aus der service_map (deutlich 
        # effizienter als für jeden Daemon _get_ceph_version() aufzurufen)
        host_version_map = {}
        try:
            svc_map = self.get('service_map')
            if svc_map and 'services' in svc_map:
                for svc_type, svc_data in svc_map.get('services', {}).items():
                    daemons = svc_data.get('daemons', {})
                    if isinstance(daemons, dict):
                        for daemon_info in daemons.values():
                            if isinstance(daemon_info, dict):
                                metadata = daemon_info.get('metadata', {})
                                if isinstance(metadata, dict):
                                    hname = metadata.get('hostname', '')
                                    ver_short = metadata.get('ceph_version_short', '')
                                    if hname and ver_short:
                                        host_version_map[hname] = ver_short
        except Exception as e:
            raise orchestrator.OrchestratorError(f"Failed to build host_version_map: {e}")

        def get_host_version(hostname):
            return host_version_map.get(hostname, 'unknown')

        # --- 2. PROXMOX VERSION CACHE (nutzt die neue Funktion) ---
        # Verhindert, dass für jeden Daemon einzeln die API gecallt wird
        pve_version_cache = {}
        def get_pve_image_id(hostname):
            if not hostname or hostname == 'unknown':
                return ''
            if hostname not in pve_version_cache:
                pve_version_cache[hostname] = self.get_proxmox_version(hostname)
            return pve_version_cache[hostname]

        # --- 2. IP zu Hostname Map bauen ---
        ip_to_host = {}
        try:
            mon_map = self.get('mon_map') or {}
            for mon in mon_map.get('mons', []):
                name = mon.get('name', '')
                pub_addr = mon.get('public_addr', '')
                if name and pub_addr:
                    ip = pub_addr.split(':')[0].split('/')[0]
                    ip_to_host[ip] = name
        except Exception: pass

        try:
            mgr_map = self.get('mgr_map') or {}
            active_name = mgr_map.get('active_name', '')
            active_addr = mgr_map.get('active_addr', '')
            if active_name and active_addr:
                ip = active_addr.split(':')[0].split('/')[0]
                ip_to_host[ip] = active_name
            # Auch Standby-MGR-IPs erfassen (wichtig für MDS)
            for s in mgr_map.get('standbys', []):
                if not isinstance(s, dict): continue
                sname = s.get('name', '')
                saddr = s.get('addr', '')
                if sname and saddr:
                    ip = saddr.split(':')[0].split('/')[0]
                    ip_to_host[ip] = sname
        except Exception: pass

        # --- 3. MANAGER ---
        if not is_filtered('mgr'):
            try:
                mgr_map = self.get('mgr_map') or {}
                active_name = mgr_map.get('active_name', '')
                standbys = mgr_map.get('standbys', [])
                
                if active_name:
                    parts = active_name.split('.')
                    hostname = parts[1] if len(parts) > 1 else active_name
                    daemon_list.append(orchestrator.DaemonDescription(
                        daemon_type='mgr', daemon_id=active_name, hostname=hostname,
                        status=orchestrator.DaemonDescriptionStatus.running, is_active=True,
                        version=get_host_version(hostname), systemd_unit=f"ceph-mgr@{active_name}",
                        status_desc='up:active',
                        container_image_id=get_pve_image_id(hostname)
                    ))

                if not isinstance(standbys, list):
                    raise orchestrator.OrchestratorError(f"MGR DEBUG: standbys ist keine Liste! Typ: {type(standbys)}")
                else:
                    for s in standbys:
                        # FIX: standbys sind Dicts mit 'name'-Key!
                        if isinstance(s, dict):
                            name = s.get('name', '')
                        elif isinstance(s, str):
                            name = s
                        else:
                            continue

                        if not name:
                            continue

                        parts = name.split('.')
                        hostname = parts[1] if len(parts) > 1 else name
                        daemon_list.append(orchestrator.DaemonDescription(
                            daemon_type='mgr', daemon_id=name, hostname=hostname,
                            status=orchestrator.DaemonDescriptionStatus.running, is_active=False,
                            version=get_host_version(hostname), systemd_unit=f"ceph-mgr@{name}",
                            status_desc='up:standby',
                            container_image_id=get_pve_image_id(hostname)
                        ))

            except Exception as e:
                raise orchestrator.OrchestratorError(f"MGR DEBUG: Fehler beim Parsen der Manager: {e}")

        # --- 4. MONITORE ---
        if not is_filtered('mon'):
            try:
                mon_map = self.get('mon_map') or {}
                mons = mon_map.get('mons', [])
                
                # Quorum aus mon_status auslesen (in Ceph v19 als JSON-String versteckt)
                quorum_ranks = []
                try:
                    mon_status_raw = self.get('mon_status') or {}
                    if 'json' in mon_status_raw and isinstance(mon_status_raw['json'], str):
                        mon_status = json.loads(mon_status_raw['json'])
                    else:
                        mon_status = mon_status_raw
                    quorum_ranks = mon_status.get('quorum', [])
                except Exception:
                    # Fallback: Wenn wir das Quorum nicht auslesen können, 
                    # nehmen wir an, dass alle Monitore laufen
                    pass

                for mon in mons:
                    name = mon.get('name', '')
                    rank = mon.get('rank')
                    
                    # Ein MON läuft, wenn sein Rank im Quorum ist
                    is_running = rank in quorum_ranks if quorum_ranks else True
                    
                    daemon_list.append(orchestrator.DaemonDescription(
                        daemon_type='mon', daemon_id=name, hostname=name,
                        service_name='mon',
                        status=orchestrator.DaemonDescriptionStatus.running if is_running else orchestrator.DaemonDescriptionStatus.stopped,
                        status_desc='running' if is_running else 'stopped (outside quorum)',
                        is_active=is_running,
                        version=get_host_version(name), 
                        systemd_unit=f"ceph-mon@{name}",
                        container_image_id=get_pve_image_id(name)
                    ))
            except Exception as e:
                raise orchestrator.OrchestratorError(f"MON DEBUG: Fehler beim Parsen der Monitore: {e}")

        # --- 5. OSDs ---
        if not is_filtered('osd'):
            try:
                osd_map = self.get('osd_map') or {}
                osd_states = {}
                for osd_info in osd_map.get('osds', []):
                    if isinstance(osd_info, dict):
                        osd_id = str(osd_info.get('osd', ''))
                        state_str = str(osd_info.get('state', '')).lower()
                        osd_states[osd_id] = 'up' in state_str

                osd_metadata = self.get('osd_metadata') or {}
                for osd_id_str, meta in osd_metadata.items():
                    hostname = meta.get('hostname', '')
                    is_up = osd_states.get(osd_id_str, False)
                    
                    # OSD liefert die Version direkt im Metadaten-Objekt!
                    osd_version = meta.get('ceph_version_short', '') or get_host_version(hostname)
                    
                    daemon_list.append(orchestrator.DaemonDescription(
                        daemon_type='osd', daemon_id=osd_id_str, hostname=hostname,
                        status=orchestrator.DaemonDescriptionStatus.running if is_up else orchestrator.DaemonDescriptionStatus.stopped,
                        status_desc="running" if is_up else "stopped",
                        version=osd_version, systemd_unit=f"ceph-osd@{osd_id_str}",
                        container_image_id=get_pve_image_id(hostname)
                    ))
            except Exception as e:
                raise orchestrator.OrchestratorError(f"OSD DEBUG: Fehler beim Parsen der Manager: {e}")

        # --- 6. METADATA SERVER ---
        if not is_filtered('mds'):
            try:
                fs_map = self.get('fs_map') or {}

                def get_ip(mds_data):
                    raw_addr = mds_data.get('addr', '')
                    if raw_addr and isinstance(raw_addr, str):
                        return raw_addr.split(':')[0].split('/')[0]
                    addrs = mds_data.get('addrs', {})
                    if isinstance(addrs, dict):
                        addrvec = addrs.get('addrvec', [])
                        if isinstance(addrvec, list) and addrvec:
                            v2_addr = addrvec[0].get('addr', '')
                            if v2_addr:
                                return v2_addr.split(':')[0].split('/')[0]
                    return None

                mds_parsed = []

                # A) Standby MDS
                for mds in fs_map.get('standbys', []):
                    if not isinstance(mds, dict): continue
                    mds_name = mds.get('name', '')
                    state = mds.get('state', '')

                    ip = get_ip(mds)
                    hostname = ip_to_host.get(ip, ip) if ip else "unknown"

                    mds_parsed.append({'name': mds_name, 'hostname': hostname, 'state': state})

                # B) Aktive MDS
                for fs in fs_map.get('filesystems', []):
                    mdsmap = fs.get('mdsmap', {}) or {}
                    for gid_key, mds_data in mdsmap.get('info', {}).items():
                        if not isinstance(mds_data, dict): continue
                        mds_name = mds_data.get('name', '')
                        state = mds_data.get('state', '')

                        ip = get_ip(mds_data)
                        hostname = ip_to_host.get(ip, ip) if ip else "unknown"

                        mds_parsed.append({'name': mds_name, 'hostname': hostname, 'state': state})

                # C) In DaemonDescription umwandeln
                for m in mds_parsed:
                    is_up = 'up' in m.get('state', '').lower()
                    daemon_list.append(orchestrator.DaemonDescription(
                        daemon_type='mds',
                        daemon_id=m['name'],
                        hostname=m['hostname'],
                        status=orchestrator.DaemonDescriptionStatus.running if is_up else orchestrator.DaemonDescriptionStatus.stopped,
                        status_desc=m['state'],
                        version=get_host_version(m['hostname']),
                        systemd_unit=f"ceph-mds@{m['name']}",
                        container_image_id=get_pve_image_id(m['hostname'])
                    ))
            except Exception as e:
                raise orchestrator.OrchestratorError(f"MDS DEBUG: Fehler beim Parsen der Manager: {e}")

        # --- 7. RGW / ANDERE ---
        try:
            svc_map = self.get('service_map') or {}
            for svc_type, svc_data in svc_map.get('services', {}).items():
                if is_filtered(svc_type): continue
                for gid, d_info in svc_data.get('daemons', {}).items():
                    if isinstance(d_info, dict):
                        metadata = d_info.get('metadata', {})
                        if not isinstance(metadata, dict): continue
                        full_id = metadata.get('id', str(gid))
                        hostname = metadata.get('hostname', '')
                        dtype = svc_type
                        did = full_id
                        if '.' in full_id:
                            dtype, did = full_id.split('.', 1)
                            
                        # Version direkt aus dem RGW/Service-Metadaten-Objekt!
                        daemon_version = metadata.get('ceph_version_short', '') or get_host_version(hostname)
                        
                        daemon_list.append(orchestrator.DaemonDescription(
                            daemon_type=dtype, daemon_id=did, hostname=hostname,
                            status=orchestrator.DaemonDescriptionStatus.running,
                            version=daemon_version,
                            systemd_unit=f"ceph-{dtype}@{did}",
                            status_desc='running',
                            container_image_id=get_pve_image_id(hostname)
                        ))
        except Exception as e:
                raise orchestrator.OrchestratorError(f"RGW/ANDERE DEBUG: Fehler beim Parsen der Manager: {e}")

        # Globale Host/ID Filter anwenden
        if host:
            daemon_list = [d for d in daemon_list if d.hostname == host]
        if daemon_id:
            daemon_list = [d for d in daemon_list if d.daemon_id == daemon_id]

        return daemon_list

    @orchestrator.handle_orch_error
    def upgrade_ls(self, image: Optional[str], tags: bool, show_all_versions: Optional[bool]) -> Dict[Any, Any]:
        """
        Gibt das exakte Format zurück, das das Ceph Dashboard erwartet.
        """
        return {
            "image": "Alexander Gott",
            "registry": "Proxmox",
            "versions": []
        }

    @orchestrator.handle_orch_error
    def upgrade_status(self) -> orchestrator.UpgradeStatusSpec:
        return orchestrator.UpgradeStatusSpec()

    @orchestrator.handle_orch_error
    def describe_service(self, service_type: Optional[str] = None, service_name: Optional[str] = None, refresh: bool = False):
        """
        Beschreibt Services durch direktes Lesen der Ceph-Maps.
        """
        services = {}

        # --- MON ---
        if not service_type or service_type == 'mon':
            try:
                mon_map = self.get('mon_map') or {}
                mons = mon_map.get('mons', [])
                total = len(mons)
                running = 0

                # In Ceph v19 (Squid) ist die Quorum-Info in mon_status als JSON-String versteckt
                try:
                    mon_status_raw = self.get('mon_status') or {}
                    if 'json' in mon_status_raw and isinstance(mon_status_raw['json'], str):
                        mon_status = json.loads(mon_status_raw['json'])
                    else:
                        mon_status = mon_status_raw
                    
                    quorum_ranks = mon_status.get('quorum', [])
                    running = len(quorum_ranks)
                except Exception as e:
                    raise orchestrator.OrchestratorError(e)

                # Fallback: Wenn alle Monitore im Cluster sind, gehen wir davon aus, dass sie laufen
                if running == 0 and total > 0:
                    running = total
                
                if total > 0:
                    spec = ServiceSpec(
                        service_type='mon',
                        placement=PlacementSpec()
                    )
                    services['mon'] = orchestrator.ServiceDescription(
                        spec=spec,
                        running=running,
                        size=total
                    )
            except Exception as e:
                raise orchestrator.OrchestratorError(f"describe_service MON FAILED: {e}")

        # --- MGR ---
        if not service_type or service_type == 'mgr':
            try:
                mgr_map = self.get('mgr_map') or {}
                active_name = mgr_map.get('active_name', '')
                standbys = mgr_map.get('standbys', [])
                total = (1 if active_name else 0) + len(standbys)
                running = total
                
                if total > 0:
                    spec = ServiceSpec(
                        service_type='mgr',
                        placement=PlacementSpec()
                    )
                    services['mgr'] = orchestrator.ServiceDescription(
                        spec=spec,
                        running=running,
                        size=total
                    )
            except Exception as e:
                raise orchestrator.OrchestratorError(f"describe_service MGR FAILED: {e}")

        # --- OSD ---
        if not service_type or service_type == 'osd':
            try:
                osd_map = self.get('osd_map') or {}
                osds = osd_map.get('osds', [])
                total = len(osds)
                running = sum(1 for o in osds if isinstance(o, dict) and 'up' in str(o.get('state', '')).lower())
                
                if total > 0:
                    spec = ServiceSpec(
                        service_type='osd',
                        placement=PlacementSpec()
                    )
                    services['osd'] = orchestrator.ServiceDescription(
                        spec=spec,
                        running=running,
                        size=total
                    )
            except Exception as e:
                raise orchestrator.OrchestratorError(f"describe_service OSD FAILED: {e}")

        # --- MDS ---
        if not service_type or service_type == 'mds':
            try:
                fs_map = self.get('fs_map') or {}
                for fs in fs_map.get('filesystems', []):
                    mdsmap = fs.get('mdsmap', {}) or {}
                    fs_name = mdsmap.get('fs_name', 'cephfs')

                    info = mdsmap.get('info', {}) or {}

                    # Falls ein 'summary'-Key existiert, herausfiltern
                    actual_info = {
                        k: v for k, v in info.items()
                        if k != 'summary' and isinstance(v, dict)
                    }

                    total = len(actual_info)
                    up_count = sum(1 for m in actual_info.values()
                                   if isinstance(m, dict) and 'up' in str(m.get('state', '')).lower())

                    if total > 0:
                        spec = ServiceSpec(
                            service_type='mds',
                            service_id=fs_name,
                            placement=PlacementSpec()
                        )
                        svc_name = spec.service_name()
                        services[svc_name] = orchestrator.ServiceDescription(
                            spec=spec,
                            running=up_count,
                            size=total
                        )
            except Exception as e:
                raise orchestrator.OrchestratorError(f"describe_service MDS FAILED: {e}")

        # --- RGW und andere aus service_map ---
        try:
            svc_map = self.get('service_map') or {}
            skip_types = {'mon', 'mgr', 'osd', 'mds'}
            for svc_type_key, svc_data in svc_map.get('services', {}).items():
                if service_type and svc_type_key != service_type:
                    continue
                if svc_type_key in skip_types:
                    continue

                daemons = svc_data.get('daemons', {}) or {}

                actual_daemons = {
                    k: v for k, v in daemons.items()
                    if k != 'summary' and isinstance(v, dict)
                }

                total = len(actual_daemons)

                running = 0
                for d in actual_daemons.values():
                    if isinstance(d, dict):
                        # Daemon-Status auswerten (Typisch: status.state)
                        status = d.get('status', {})
                        if isinstance(status, dict):
                            state = status.get('state', 'unknown')
                        else:
                            state = str(status)

                        # Alles was nicht explizit "stopped"/"error" ist, gilt als running
                        if state.lower() not in ('stopped', 'error', ''):
                            running += 1

                # Fallback: Wenn kein Status erkennbar, alle als running zählen
                if running == 0 and total > 0:
                    running = total

                if total > 0:
                    clean_type = 'rgw' if svc_type_key in ('radosgw', 'rgw') else svc_type_key
                    spec = ServiceSpec(
                        service_type=clean_type,
                        service_id='default',
                        placement=PlacementSpec()
                    )
                    svc_name = spec.service_name()
                    services[svc_name] = orchestrator.ServiceDescription(
                        spec=spec,
                        running=running,
                        size=total
                    )
        except Exception as e:
            raise orchestrator.OrchestratorError(f"describe_service RGW/OTHER FAILED: {e}")

        # Filter nach service_name
        result = list(services.values())
        if service_name:
            result = [s for s in result if s.spec and s.spec.service_name() == service_name]

        # WICHTIG: Nur die Liste zurückgeben! Der @handle_orch_error Decorator macht das OrchResult automatisch!
        return result

    @orchestrator.handle_orch_error
    def get_security_config(self) -> Dict[str, bool]:
    
        svc_map = self.get('service_map') or {}
        services = svc_map.get('services', {})
    
        mgmt_gw_enabled = (
            'mgmt-gateway' in services or
            'mgmt_gateway' in services
        )
    
        security_enabled = (
            mgmt_gw_enabled or
            'oauth2-proxy' in services or
            'oauth2_proxy' in services
        )
    
        return {
            'security_enabled': security_enabled,
            'mgmt_gw_enabled': mgmt_gw_enabled
        }
    # --- STUBS (Verhindern Abstürze wegen fehlender abstrakter Methoden) ---

    def add_host(self, host_spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def remove_host(self, host, force=False, offline=False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def update_host_addr(self, host, addr): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def add_host_label(self, host, label): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def remove_host_label(self, host, label, force=False): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def host_ok_to_stop(self, host): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def enter_host_maintenance(self, host, force=False): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def exit_host_maintenance(self, host, force=False, offline=False): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def rescan_host(self, host): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def create_osds(self, drive_group): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI to create OSDs")
    def remove_osds(self, osd_ids, force=False, zap=False, no_destroy=False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def remove_service(self, service_name, force=False): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def remove_daemons(self, names): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def daemon_action(self, action, daemon_name, image=None, force=False): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply(self, specs, no_overwrite=False, continue_on_error=True): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def zap_device(self, host, path): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def blink_device_light(self, ident_fault, on, locs): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def set_unmanaged(self, service_name, value): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def upgrade_start(self, image, version, daemon_types=None, host_placement=None, services=None, limit=None): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def upgrade_pause(self): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def upgrade_resume(self): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def upgrade_stop(self): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_mon(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_mgr(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_mds(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_rgw(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_nfs(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_iscsi(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_rbd_mirror(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_cephfs_mirror(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_prometheus(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_grafana(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_alertmanager(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_node_exporter(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_loki(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_promtail(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_crash(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_container(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")
    def apply_snmp_gateway(self, spec): raise orchestrator.OrchestratorError("Proxmox: Not supported")

    def os_exit_noop(status: int): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def host_exists(hostname_position: int = 1): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def shutdown(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_fqdn(self, hostname: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_mgr_ips(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_security_config(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_mgmt_gw_external_endpoint(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_cephadm_binary_path(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _trigger_osd_removal(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def pause(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def validate_ssh_config_fname(self, ssh_config_fname: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _process_ls_output(self, host: str, ls: List[Dict[str, Any]]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _generate_key(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_pub_key(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def set_ssh_user(self, user: str, skip_pre_steps: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_extra_ceph_conf(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _update_config_check(self, check_name: str, status: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def extra_ceph_conf(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _osd_activate(self, host: List[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_cephadm_version_for_host_prep(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _prepare_new_host_for_sudo_hardening(self, hostname: str, addr: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def hardware_powercycle(self, hostname: str, yes_i_really_mean_it: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _host_ok_to_stop(self, hostname: str, force: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def update_maintenance_healthcheck(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def perform_service_action(self, action: str, service_name: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def daemon_is_self(self, daemon_type: str, daemon_id: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_active_mgr(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_active_mgr_digests(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _schedule_daemon_action(self, daemon_name: str, action: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _check_pool_exists(self, pool: str, service_name: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_alertmanager_credentials(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def generate_certificates(self, module_name: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def set_prometheus_access_info(self, user: str, password: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def remove_prometheus_remote_write(self, url: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def cert_store_bindings_ls(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def cert_store_cert_check(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def rm_tuned_profile(self, profile_name: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def tuned_profile_ls(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def tuned_profile_rm_setting(self, profile_name: str, setting: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def set_health_error(self, name: str, summary: str, count: int, detail: List[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def remove_health_warning(self, name: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_dashboard_url(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def stop_remove_osds(self, osd_ids: List[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def set_osd_spec(self, service_name: str, osd_ids: List[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def drain_host(self, hostname: str, force: bool = False, keep_conf_keyring: bool = False, zap_osd_devices: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def tuned_profile_add_settings(self, profile_name: str, settings: dict): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def run_cmd(cmd_args: dict): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def node_proxy_fullreport(self, hostname: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def hardware_shutdown(self, hostname: str, force: Optional[bool] = False, yes_i_really_mean_it: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _daemon_action_set_image(self, action: str, image: Optional[str], daemon_type: str, daemon_id: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def check_host(self, host: str, addr: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _prepare_host(self, host: str, addr: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _set_extra_ceph_conf(self, inbuf: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _set_pub_key(self, inbuf: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _as_datetime(value: Optional[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def registry_login(self, url: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None, inbuf: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def wait_async(self, coro, timeout: Optional[int] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def update_host_timeout_error(self, host: str, remove: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def async_timeout_handler(self, host: Optional[str] = '',cmd: Optional[str] = '',timeout: Optional[int] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def cert_store_rm_cert(self,cert_name: str,service_name: Optional[str] = None,hostname: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def cert_store_rm_key(self,key_name: str,service_name: Optional[str] = None,hostname: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _prepare_host_for_sudo_hardening(self,host: str,cephadm_args: List[str],addr: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _prepare_hosts_for_sudo_hardening(self,host: str,cephadm_args: List[str],addr: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _prepare_host_and_enable_sudo_hardening(self,user: str,host_label: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def cert_store_get_key(self,key_name: str,service_name: Optional[str] = None,hostname: Optional[str] = None,no_exception_when_missing: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _systemd_unit_ls(self,hostname: Optional[str] = None,daemon_type: Optional[str] = None,daemon_id: Optional[str] = None) : raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _client_keyring_set(self,entity: str,placement: str,owner: Optional[str] = None,mode: Optional[str] = None,no_ceph_conf: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_container_image(self,daemon_name: str,use_current_daemon_image: bool = False,force_ceph_image: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _setup_user_on_host(self, host: str, user: str, ssh_pub_key: str, addr: Optional[str] = None) : raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def wrapper(*args: Any, **kwargs: Any): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _init_cert_mgr(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_mgmt_gw_endpoint(self, is_internal: bool): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_mgmt_gw_internal_endpoint(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _kick_serve_loop(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")  
    def resume(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def validate_ssh_config_content(self, ssh_config: Optional[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def update_watched_hosts(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def offline_hosts_remove(self, host: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def update_failed_daemon_health_check(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _setup_user_on_all_hosts(self, user: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")      
    def setup_user_on_host(host: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI") 
    def _validate_and_set_ssh_val(self, what: str, new: Optional[str], old: Optional[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _set_ssh_config(self, inbuf: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")  
    def _clear_ssh_config(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")   
    def _get_ssh_config(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _set_priv_key(self, inbuf: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _set_signed_cert(self, inbuf: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")  
    def _clear_key(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _config_check_status(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _config_check_enable(self, check_name: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _config_check_disable(self, check_name: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def run(h: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _check_valid_addr(self, host: str, addr: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _add_host(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def node_proxy_summary(self, hostname: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def node_proxy_firmware(self, hostname: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def node_proxy_firmwares(self, hostname: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def node_proxy_criticals(self, hostname: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def node_proxy_common(self, category: str, hostname: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_minimal_ceph_conf(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _combine_confs(self, conf1: str, conf2: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _invalidate_daemons_and_kick_serve(self, filter_host: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _invalidate_all_host_metadata_and_kick_serve(self, hostname: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")   
    def service_action(self, action: str, service_name: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def blink(host: str, dev: str, path: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_osd_uuid_map(self, only_up=False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")   
    def get_osd_by_id(self, osd_id: int): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _remove_daemons(self, name: str, host: str, force_delete_data: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def create_func_map(*args: Any): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_prometheus_credentials(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def set_custom_prometheus_alerts(self, alerts_file: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def set_prometheus_target(self, url: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")   
    def remove_prometheus_target(self, url: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def set_prometheus_remote_write(self, url: str, remote_write_allowed_metrics: List[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def set_alertmanager_access_info(self, user: str, password: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_prometheus_access_info(self) : raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_alertmanager_access_info(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def cert_store_reload(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def cert_store_key_ls(self, include_cephadm_generated_keys: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_nvmeof_tls_bundle(self, service_name: str, daemon_name: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _raise_non_editable_cert_error(self, cert_name: str, consumer: str, service_name: str, hostname: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def tuned_profile_add_setting(self, profile_name: str, setting: str, value: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def tuned_profile_rm_settings(self, profile_name: str, settings: List[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def set_health_warning(self, name: str, summary: str, count: int, detail: List[str]): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def upgrade_check(self, image: str, version: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def update_service(self, service_type: str, service_image: str, image: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def remove_osds_status(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def stop_drain_host(self, hostname: str): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def trigger_connect_dashboard_rgw(self): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def systemd_unit_ls(self,hostname: Optional[str] = None,daemon_type: Optional[str] = None,daemon_id: Optional[str] = None)  : raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _client_keyring_rm(self,entity: str,): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def cert_store_cert_ls(self,filter_by: str = '',show_details: bool = False,include_cephadm_signed: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def cert_store_get_cert(self,cert_name: str,service_name: Optional[str] = None,hostname: Optional[str] = None,no_exception_when_missing: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def replace_device(self,hostname: str,device: str,clear: bool = False,yes_i_really_mean_it: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def inner(func: Callable): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")

    def _preview_osdspecs(self,  osdspecs): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_first_matching_network_ip(self,host: str,sspec,sspec_networks: Optional[List[str]] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def get_unique_name(self,daemon_type: str,host: str,existing,prefix: Optional[str] = None,forcename: Optional[str] = None,rank: Optional[int] = None,rank_generation: Optional[int] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _is_adding_nvmeof_group_to_existing_service(self, nvmeof_spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _plan(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def plan(self, specs): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _check_cert_source(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _trigger_preview_refresh(self, specs, service_name: Optional[str] = None,): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def apply_drivegroups(self, specs): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def device_selection_from_explicit_paths(sel): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def create_osd_default_spec(self, drive_group): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def is_path_lists_only(drive_group): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def validate_device(self, host_name: str, drive_group): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _rotate_daemon_key(self, daemon_spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _mon_public_network_changed(self, daemon_spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _apply(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _get_candidate_hosts(self, placement): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _validate_tunedprofile_settings(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _validate_tuned_profile_spec(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _config_checks_list(self, format): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def extra_ceph_conf_is_newer(self, dt): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _client_keyring_ls(self, format): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _daemon_action(self,daemon_spec,action: str,image: Optional[str] = None,skip_restart_for_reconfig: bool = False,send_signal_to_daemon: Optional[str] = None): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def path_list_only(sel): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")  
    def validate_no_empty_device_paths(drive_group): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _add_daemon(self,daemon_type: str,spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _create_daemons(self,daemon_type: str,spec,daemons,hosts,count: int): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def add_daemon(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _validate_one_shot_placement_spec(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def apply_tuned_profiles(self, specs, no_overwrite: bool = False): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def _apply_service_spec(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def apply_ingress(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI") 
    def apply_iscsi(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def apply_alloy(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def apply_ceph_exporter(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def apply_smb(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def apply_mgmt_gateway(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
    def apply_oauth2_proxy(self, spec): raise orchestrator.OrchestratorError("Proxmox: Use PVE GUI")
